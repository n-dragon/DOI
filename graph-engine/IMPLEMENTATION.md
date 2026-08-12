# Guide d'implémentation — rôle de chaque crate

Ce document complète `ARCHITECTURE.md` (qui montre le flux d'une requête
de bout en bout) en détaillant, crate par crate : son rôle exact, ce
qu'elle expose, de quoi elle dépend, ce qui est déjà écrit et ce qu'il
reste à implémenter. Sert de feuille de route pour la Phase 0/1/2 (spec
§12) — Phase 0/1/2 sont désormais toutes terminées ; ce qui suit
documente aussi les décisions prises pendant l'implémentation (pas
seulement l'état "fait/reste à faire").

---

## graph-schema

**Rôle.** Source de vérité du modèle de données (spec §3). C'est la seule
crate que toutes les autres dépendent — elle-même ne dépend de rien dans
le workspace.

**Ce qu'elle expose.**
- Types de domaine : `NodeId`, `EdgeId`, `Label`, `EdgeType`, `ScalarType`,
  `PropertyDef`, `NodeDef`, `EdgeDef`, `Schema`.
- `SchemaParser` : trait à implémenter — transforme le texte de l'IDL
  déclarative (§3.4) en `Schema`.
- `SchemaEvolution` : trait à implémenter — classe un changement de schéma
  en `Compatible` ou `Incompatible` (§3.5).

**Dépend de** : rien. **Dépendent d'elle** : toutes les autres crates.

**Fait.** Tous les types de domaine, `Schema::node_def`/`edge_def`,
`SchemaError` ; grammaire `pest` de l'IDL et `PestSchemaParser` (S1-S4,
avec tests sur l'exemple §3.4 et les cas d'erreur — label/type dupliqué,
endpoint d'arête inconnu, propriété sans type) ; `SpecSchemaEvolution`
(S5-S7, les deux règles §3.5 : compatible = propriété optionnelle
ajoutée ou nouveau label/type d'arête ; incompatible = propriété
supprimée ou retypée — un renommage n'étant pas distingué d'un
remove+add faute de marqueur dédié dans l'IDL).

**Reste à faire (Phase 0).**
- Génération du schéma des tables Iceberg à partir d'un `Schema` (utilisée
  par le pipeline d'ingestion externe, hors binaire du moteur — mais le
  mapping doit être défini ici pour rester la source de vérité, §3.4).

---

## graph-storage

**Rôle.** Accès lecture seule à Apache Iceberg (spec §4). Ne sait rien de
l'indexation ni des requêtes — juste "donne-moi les lignes d'une table à
un snapshot donné".

**Ce qu'elle expose.**
- `SnapshotId`, `NodeRow`, `EdgeRow`, `PropertyValue`, `StorageError`,
  `NodeRowStream`/`EdgeRowStream`.
- `IcebergReader` : trait à implémenter — `latest_snapshot`, `scan_nodes`,
  `scan_edges` (toutes `async fn`, les deux `scan_*` retournant un stream
  plutôt qu'un itérateur sync : la lecture object-store sous-jacente est
  intrinsèquement async). Implémentation : `IcebergCatalogReader<C>`,
  générique sur tout `iceberg::Catalog`.
- `node_table_name`/`edge_table_name` : convention de nommage §4.1
  (`nodes_<label>`/`edges_<edge_type>`, en minuscules).
- `open_sql_catalog` : construit le catalogue dev persistant (SQLite +
  `FileIO` local), namespace créé s'il n'existe pas déjà.

**Dépend de** : `graph-schema`. **Dépend d'elle** : `graph-index`.

**Fait (ST1-ST6).** Crate Iceberg retenue : `apache/iceberg-rust` (crate
`iceberg`, v0.10) — son API de scan `to_arrow()` retourne des
`RecordBatch` Arrow, directement exploités par la désérialisation ST3.
Catalogue dev : `iceberg-catalog-sql` (`SqlCatalog`) + SQLite + `FileIO`
filesystem local, via `open_sql_catalog` (module `catalog.rs`) — zéro
infra à faire tourner (juste un fichier), mais persistant : le registre
de tables survit à la fin du process, contrairement au `MemoryCatalog`
essayé initialement (revu après avoir constaté, en déployant réellement
`graph-partition-node` en process séparé de l'ingestion, qu'un registre
en mémoire ne peut pas être partagé entre deux process — `NamespaceNotFound`
malgré des fichiers Parquet présents sur disque ; testé par un déploiement
à quatre process réels — `ingest-cloud-cost` / `graph-partition-node` /
`graph-coordinator` / `query-client` — et un test de régression dédié).
`MemoryCatalog` reste utilisé là où le partage inter-process n'a pas de
sens : `examples/demo` (un seul process, ingère et interroge dans le même
run) et les tests d'intégration (ST6, CO4).
Catalogue prod : volontairement `TBD` (même posture que le
partitionnement physique déjà `TBD` en §4.1) — candidat : catalogue REST
devant un service managé (Glue/Polaris/Unity/Nessie), à trancher au
déploiement ; `IcebergCatalogReader<C>` est générique sur `Catalog`, donc
ce choix ne touche pas le code de lecture. `read_property_value` (ST3) :
une branche par variante de `ScalarType`, testée directement sur des
tableaux Arrow construits en mémoire (pas d'I/O). `scan_nodes`/`scan_edges`
(ST4/ST5) : résolvent la table, streament les `RecordBatch`, les
applatissent en `NodeRow`/`EdgeRow` — les colonnes d'identifiants
(`node_id`/`edge_id`/`src_node_id`/`dst_node_id`) sont stockées en
`Int64` signé et re-castées bit-à-bit vers les `u64` du moteur. Test
d'intégration ST6 : écrit une vraie table Iceberg (catalogue mémoire +
FileIO local), y committe un fichier Parquet réel via l'API `writer` de
la crate, puis vérifie que `scan_nodes` relit exactement les lignes
écrites (y compris une valeur `NULL`).

**Reste à faire.** Rien côté Phase 0/1 pour cette crate — génération du
schéma des tables Iceberg à partir d'un `graph_schema::Schema` reste une
tâche à part, déjà notée dans `graph-schema`.
- Trancher l'alignement du partition spec Iceberg avec le partitionnement
  logique du cluster (§4.1, encore `TBD` dans le spec).

---

## graph-index

**Rôle.** Le cœur du moteur : structures en mémoire (spec §5) et cycle de
vie d'une génération d'index. C'est la crate la plus dense du workspace.

**Ce qu'elle expose.**
- `PartitionId`, `RemoteRef` — identité de partition et référence vers un
  nœud hors partition.
- `TopologicalIndex` — CSR (§5.1) : `out_neighbors`/`in_neighbors`/
  `contains`/`node_ids`.
- `PropertyIndex` — B-Tree (§5.2) : `lookup_eq` (le range lookup pour
  `WHERE x > y` reste à ajouter, cf. ci-dessous).
- `IndexGeneration` — bundle immuable topologie + propriétés + métadonnées
  (`GenerationMeta`, exposées telles quelles par `GetIndexStatus`, §8.2).
- `GenerationHandle` — `acquire()`/`swap()` via `ArcSwap`, le mécanisme de
  bascule atomique sans interruption (§5.3).
- `IndexBuilder` : trait à implémenter — construit une `IndexGeneration`
  depuis Iceberg pour une partition donnée.

**Dépend de** : `graph-schema`, `graph-storage`. **Dépendent d'elle** :
`graph-query`, `graph-cluster` (pour `PartitionId`), les deux binaires.

**Fait.** Toutes les structures de données et leur API de lecture, le
mécanisme de swap atomique complet et fonctionnel (`ArcSwap` gère déjà le
"les requêtes en cours gardent l'ancienne génération jusqu'à leur fin").

**Fait (Phase 1) — complément.** `IndexBuilder::build` scanne via
`IcebergReader`, construit les tableaux CSR et les B-Tree de propriété
(IX1-IX8) ; `PropertyIndex::lookup_range` couvre `WHERE x > y` (IX5).

**Fait (Phase 2, IX3 révisée).** Le builder est maintenant réellement
partition-aware : `IcebergIndexBuilder::new` prend un `n_partitions` et
filtre les nœuds scannés à ceux dont `graph_schema::partitioning::
partition_of(node_id, n_partitions) == partition` avant de construire
CSR/index de propriété/`node_records` — plus aucun code en aval n'a
besoin de refiltrer. Une arête dont l'autre extrémité n'est pas locale
devient un `RemoteRef{partition, node}` réel (calculé via la même
fonction) plutôt que `None` systématique — sauf si cette extrémité
hache vers la partition en cours de construction sans y être trouvée
(incohérence de données réelle, pas une référence distante : toujours
droppée silencieusement, comportement Phase 1 inchangé pour ce cas).
`n_partitions: 1` (tous les appels Phase 1 existants) reproduit
exactement l'ancien comportement mono-partition. Testé explicitement
(`remote_edges_are_flagged_as_remote_ref_across_partitions`), en plus du
test Phase 1 renommé (`no_adjacency_entry_is_ever_remote_when_mono_partition`).

**Reste à faire.** Décider la valeur par défaut de l'intervalle de
rebuild (§5.3, `TBD`).

**Fait (extension hors roadmap, IX9-IX10).** `EdgeRecord { edge_type,
properties }` et `IndexGeneration.edge_records` — jusqu'ici seules les
propriétés des *nœuds* survivaient à la construction d'index
(`node_records`), les arêtes n'existaient que comme entrées CSR sans
propriétés attachées. *Décision* : un enregistrement d'arête est possédé
par la partition de sa **source**, le même critère que `build_csr`
utilise déjà pour l'adjacence sortante locale — pas une seconde règle de
répartition à mémoriser séparément. Conséquence : une requête
`GetEdgeProperties`/anti-jointure pour une `EdgeId` donnée n'atteint
jamais plus d'une partition.

---

## graph-dsl

**Rôle.** Le langage de requête (spec §7.1/§7.2) : AST, parsing, et
validation statique contre le schéma actif — avant toute exécution
distribuée (fail-fast).

**Ce qu'elle expose.**
- AST : `Pattern`, `PatternStep`, `NodePattern`, `EdgePattern`,
  `Direction`, `HopRange`, `PropertyFilter`, `ComparisonOp`, `Literal`,
  `WhereCondition`, `Query`.
- `Parser` : trait à implémenter — texte DSL → `Query`. Implémentation :
  `PestParser`.
- `Validator` : trait à implémenter — `Query` + `Schema` → erreurs de
  validation, le cas échéant. Implémentation : `SchemaValidator`.

**Dépend de** : `graph-schema`. **Dépendent d'elle** : `graph-query`,
`graph-coordinator`.

**Fait.** L'AST complet pour les deux opérations prioritaires (k-hop
filtré, pattern matching) ; volontairement **aucune** construction
d'agrégation dans l'AST (décision actée, spec §1.4/§7.1). Grammaire
`pest` couvrant les deux formes prioritaires (D1) et `PestParser` (D2-D4,
avec tests sur les deux exemples du spec §7.1 et les cas d'erreur de
syntaxe). `SchemaValidator` (D7-D9) : existence des labels/types de
relation, compatibilité type de propriété ↔ opérateur de comparaison
(égalité/inégalité valides pour tout scalaire hors `List`/`Vector` ;
opérateurs d'ordre valides seulement pour `Int64`/`Float64`/`Timestamp`).

L'AST distingue `WhereCondition::Property` (`alias.propriété OP
littéral`) de `WhereCondition::AliasComparison` (`alias OP alias`, ex.
`colleague <> p` dans l'exemple pattern-matching §7.1) — le second
compare deux alias liés, pas une propriété, donc ne peut pas réutiliser
`PropertyFilter`.

**Reste à faire (Phase 0).**
- Grammaire formelle complète (`ORDER BY`, `LIMIT`, pagination — encore
  `TBD` dans le spec §7.1).

**Fait (extension hors roadmap, D10-D13).** Alias d'arête (`[g:GRANTS]`,
restreint à un hop fixe — un alias sur `*1..3` désignerait une arête
ambiguë de la chaîne, rejeté par le validateur) ; `RETURN` mixte
(`ReturnItem { alias, property: Option<String> }` remplace le
`Vec<String>` d'origine — un alias nu est `property: None`, pas un cas
distinct) ; `WhereCondition::PropertyComparison` (`a.action = g.action`,
propriété-propriété inter-alias, à ne pas confondre avec
`Property`/`AliasComparison`) ; `WhereCondition::NotExists` — sous-
requête corrélée restreinte à un unique hop **sortant** entre deux alias
déjà liés par le `MATCH` externe, non imbriquable. Motivée par un use
case concret (moindre privilège via télémétrie,
`schema/least_privilege.graphidl`) plutôt qu'une grammaire générique de
sous-requêtes. Détail complet des décisions (pourquoi un hop sortant
seulement, pourquoi pas de `datetime()`) dans `TASKS.md` et
`ARCHITECTURE.md` §6.

---

## graph-query

**Rôle.** Le moteur de planification et d'exécution (spec §7.3/§7.4) —
transforme une `Query` validée en plan, puis l'exécute soit localement
(mono-partition), soit en scatter-gather distribué.

**Ce qu'elle expose.**
- `QueryPlan`, `PlanStep` (`ResolveStart`, `ExpandHop`).
- `Binding` (alias → nœud lié), `Frontier` (bindings locaux + références
  distantes en attente).
- `Planner` : trait à implémenter — `Query` → `QueryPlan`.
- `LocalExecutor` : trait à implémenter — exécute un `PlanStep` contre un
  `GenerationHandle` local (property lookup ou expansion CSR).
- `PartitionRpc` : trait à implémenter — la frontière RPC coordinateur ↔
  partition, indépendante de `graph-proto`/`tonic` (voir plus bas).
- `ScatterGatherExecutor<R: PartitionRpc>` — la boucle scatter-gather
  complète (§7.4) : diffusion de `ResolveStart`, `ExpandHop` décomposé en
  rounds réseau par hop, re-routage des `RemoteRef`, dédoublonnage,
  évaluation `WHERE` après coup. Remplace le trait stub
  `DistributedExecutor` (jamais implémenté avant Phase 2, signature
  `-> Result<(), _>` sans même de moyen de récupérer les résultats).

**Dépend de** : `graph-schema`, `graph-dsl`, `graph-index`,
`graph-cluster` (types de placement — `PartitionHasher`, `PartitionMap`,
`ReplicaEndpoint`), `graph-storage` (`NodeRecord`/`PropertyValue`, pour
`GetNodeProperties`). **Dépendent d'elle** : les deux binaires.

Volontairement **pas** de dépendance sur `graph-proto`/`tonic` : le
`PartitionRpc` que `ScatterGatherExecutor` consomme est un trait défini
dans `graph-query` lui-même ; `bin/graph-coordinator` l'implémente contre
le vrai client gRPC (`grpc_partition_rpc::GrpcPartitionRpc`, Phase 2/CO5),
un test (Q8) l'implémente en mémoire contre deux `SimpleLocalExecutor`.
Le planificateur/exécuteur reste donc testable sans serveur gRPC qui
tourne.

**Fait (Phase 1, Q1-Q4).** Tous les types intermédiaires et contrats. Le
découplage `LocalExecutor`/`DistributedExecutor` est déjà posé :
`LocalExecutor` est tout ce dont a besoin le MVP mono-partition (§12,
Phase 1) — pas besoin d'attendre `graph-cluster` ni le réseau pour
valider le modèle de données et le DSL de bout en bout. `NaivePlanner`
(Q1) et `SimpleLocalExecutor` (Q2/Q3), testés bout en bout (Q4) sur la
requête k-hop exemple du spec §7.1 (borne de hop range et filtre `WHERE`
tous deux vérifiés). Portée v1 assumée : nœud de départ étiqueté avec un
seul filtre d'égalité ; comparaisons d'alias (`colleague <> p`) non
appliquées ; filtres `WHERE` évalués en fin d'`ExpandHop` plutôt que
poussés hop par hop (l'optimisation que §7.3 laisse `TBD`).

**Fait (Phase 2, Q5-Q8).** `ScatterGatherExecutor` complet — `ResolveStart`
diffusé à toutes les partitions (l'index de propriété est local à chaque
partition, §5.2 — pas d'index global v1, à la différence d'une lecture
littérale de la tâche Q5 d'origine qui envisageait un simple routage par
hash), `ExpandHop` décomposé en rounds réseau par hop physique (lecture
littérale du §7.4 plutôt qu'envoyer toute la plage `*min..max` à une
seule partition), `WHERE` évalué une fois par étape via
`GetNodeProperties` (commute avec l'union, donc équivalent au push-down
par index de propriété sans avoir à router un lookup filtré arbitraire
vers chaque partition), déduplication par clé `(alias, node_id)` triée.
Testé bout en bout avec un franchissement réel de frontière de partition
(Q8). Détail complet des décisions dans `TASKS.md`.

**Fait (extension hors roadmap, Q9-Q12).** `Binding` passe d'un alias
`type Binding = HashMap<String, NodeId>` à une struct `{ nodes, edges }`
— une ligne de résultat lie désormais des alias de nœuds *et* d'arêtes.
`PlanStep::{ResolveAll, AntiJoin}` : `ResolveAll` pour un `MATCH` sans
filtre de départ (`MATCH (w:Workload)`, jusqu'ici toute requête en
supposait un) ; `AntiJoin` pour `NOT EXISTS`, compilé en une étape
séparée après la chaîne principale plutôt qu'imbriquée dans le plan — un
anti-join filtre des bindings déjà résolus. `filter_eval.rs` (nouveau,
partagé entre les deux exécuteurs) ajoute une coercion RFC3339 →
microsecondes pour comparer un littéral `String` à une propriété
`Timestamp` — sans elle, `a.last_seen >= "2024-05-…"` n'aurait jamais
matché silencieusement. Décision la plus significative :
`ScatterGatherExecutor::execute_anti_join` doit résoudre des conditions
qui référencent un alias *externe* dont la valeur varie par binding
(`a.action = g.action`) — impossible à router en un seul appel comme un
`WHERE` classique. Résolu par hydratation par binding puis regroupement
des bindings partageant la même liste de conditions déjà résolues avant
d'émettre `CheckAntiJoin` (un appel par `(partition, groupe)`, pas par
binding). Détail complet dans `TASKS.md` et `ARCHITECTURE.md` §6.

---

## graph-proto

**Rôle.** Le contrat réseau (spec §8) : définitions gRPC/Protobuf pour le
service client (`GraphService`) et le service interne coordinateur ↔
partition (`PartitionService`).

**Ce qu'elle expose.** Code généré par `tonic-build` à partir de
`proto/graph.proto` (services + messages : `ExecuteQuery`, `GetSchema`,
`GetIndexStatus`, `HealthCheck` côté client ; `ResolveStart`, `ExpandHop`,
`HealthCheck` côté interne).

**Dépend de** : `tonic`/`prost` uniquement (aucune crate interne).
**Dépendent d'elle** : les deux binaires.

**Fait.** Le `.proto` complet et le build compile (nécessite `protoc`
installé — dépendance externe documentée, pas un problème de conception).
Étendu une fois `graph-query`'s `PlanStep` stabilisé (Q1-Q4) pour porter
ce qu'il transporte réellement : `ExpandHopRequest` a gagné
`from_alias`/`to_alias`/`to_label`, `hop_min`/`hop_max` et une liste de
`PropertyFilter` (absents du contrat d'origine, écrit avant que
`PlanStep::ExpandHop` existe) ; `ResolveStartRequest` a gagné `alias` ;
`PartitionService` a gagné `GetIndexStatus` ; `IndexStatusResponse` a
changé son `pinned_snapshot_id` (scalaire) en `pinned_snapshot_by_table`
(map), `GenerationMeta` épinglant un snapshot par table.

**Reste à faire.** `List`/`Vector` du schéma §3.2 ne sont pas
représentables dans le `Value` `oneof` actuel — pas encore nécessaire
tant que le DSL ne les utilise pas dans une clause `WHERE`.

**Fait (extension hors roadmap, PROTO1).** Étendu sans casser le contrat
existant : `ExpandHopRequest.edge_alias` (optionnel), `Binding.
edge_ids_by_alias` (pendant arête de la map nœud existante),
`ResolveAll`/`CheckAntiJoin`/`GetEdgeProperties` (nouvelles RPC —
`GetEdgeProperties` symétrique de `GetNodeProperties`, Phase 1),
`QueryResult.edge_properties`. `RETURN alias.propriété` réutilise le
`projection: map<string, Value>` existant avec une clé composite
`"alias.propriété"` plutôt qu'un message de projection dédié par forme.

---

## graph-cluster

**Rôle.** Tout ce qui concerne le placement physique (spec §6) :
partitionnement par hash, découverte des répliques, rebalancement, sans
jamais toucher au nombre de partitions logiques (§6.2, décision actée).

**Ce qu'elle expose.**
- `PartitionHasher` — le calcul `hash(node_id) % n_partitions`, centralisé
  ici pour qu'il ne diverge jamais entre coordinateur et nœuds de
  partition.
- `PartitionMap`/`ReplicaEndpoint` — l'affectation courante partition →
  répliques, et `healthy_replicas` pour le load-balancing (§6.4 — pas de
  notion de leader, toutes les répliques sont équivalentes en lecture).
- `Discovery` : trait à implémenter — découverte des répliques (v1 :
  Kubernetes natif, §6.3).
- `RebalancePlanner` : trait à implémenter — calcule un `RebalancePlan`
  (liste de `PartitionMove`) quand l'ensemble de machines change.

**Dépend de** : `graph-schema`, `graph-index`. **Dépendent d'elle** :
`graph-coordinator` (routage), `graph-partition-node` (connaître sa propre
affectation).

**Fait (Phase 2, CL1-CL4).**
- `PartitionHasher` : hash réel (xxh3, `xxhash-rust`), formule canonique
  déplacée dans `graph_schema::partitioning` pour que `graph-index`
  puisse l'utiliser aussi sans cycle de dépendance (voir
  `ARCHITECTURE.md` §2.1).
- `Discovery` : `KubernetesDiscovery` (`kube::Api<Pod>`, pas
  `Endpoints`/`EndpointSlice` — la propriété de partition est portée par
  une annotation Pod `graph.io/partitions`, lue ici, calculée par
  `RebalancePlanner` ailleurs) et `StaticDiscovery` (adresses fixes, pour
  le dev/tests et tout déploiement non-Kubernetes).
- `RebalancePlanner` : `RendezvousRebalancePlanner` — hachage de
  rendez-vous (HRW), pas un simple modulo (qui reréassignerait la
  majorité des partitions à chaque changement de machine, contredisant
  "seules les partitions déplacées sont reconstruites", §6.2). Testé :
  placement initial, perturbation minimale à l'ajout d'une machine,
  idempotence.
- `PartitionMap::healthy_replicas` testé explicitement (CL4).

---

## graph-observability

**Rôle.** Instrumentation partagée (spec §9) pour que les deux binaires ne
divergent pas sur les noms de métriques ou la configuration du tracing.

**Ce qu'elle expose.** Module `metrics` (constantes de noms Prometheus,
chacune adossée à un collecteur `prometheus` réellement enregistré),
`render`/`metrics_router`/`serve_metrics` (endpoint `/metrics`),
`init_tracing`, `inject_trace_context`/`extract_and_continue`
(interceptors `tonic`, remplacent le struct `TraceContext` placeholder,
supprimé).

**Dépend de** : `tracing`, `tracing-subscriber`, `opentelemetry`/
`opentelemetry_sdk`/`opentelemetry-otlp`, `tracing-opentelemetry`,
`prometheus`, `axum`, `tonic` (pour le type des interceptors),
`tokio`. **Dépendent d'elle** : les deux binaires.

**Fait (Phase 2, OB1-OB4).**
- `init_tracing` : `tracing-subscriber` (JSON + `EnvFilter`) toujours
  actif ; exporteur OTLP en plus si un collecteur est configuré/
  joignable — **transport HTTP**, pas gRPC, pour éviter d'épingler une
  seconde version de `tonic` dans l'arbre de dépendances rien que pour
  l'exportateur (elles coexistent sans conflit de toute façon — `cargo
  build` le confirme — mais HTTP évite la question). Un collecteur
  injoignable ne bloque jamais le démarrage : repli sur JSON seul avec
  une ligne d'avertissement (l'observabilité est une aide opérationnelle,
  pas une dépendance de disponibilité).
- `inject_trace_context`/`extract_and_continue` : propagation réelle du
  contexte via les métadonnées gRPC (propagateur W3C `traceparent` par
  défaut d'OpenTelemetry), câblés respectivement dans
  `GrpcPartitionRpc::client` (`bin/graph-coordinator`) et
  `PartitionServiceServer::with_interceptor` (`bin/graph-partition-node`).
- `/metrics` (`serve_metrics`, port dédié — convention Prometheus
  standard) sur les deux binaires. Points d'instrumentation réels câblés
  aux endroits représentatifs : latence/erreurs/hops de requête
  (`GraphServiceImpl::execute_query`), taille d'index/durée de
  rebuild/âge de snapshot (`rebuild.rs`), latence de hop local
  (`PartitionServiceImpl::expand_hop`).

**Reste à faire.** `CROSS_PARTITION_HOP_RATIO` (§9.1) reste déclaré
(exposé dès le démarrage sur `/metrics`, cohérent avec les autres) mais
pas encore observé en pratique dans `ScatterGatherExecutor`.

---

## bin/graph-coordinator — ✅ terminé (Phase 1 + Phase 2, mode distribué)

**Rôle.** Process réseau implémentant `GraphService` — le rôle
coordinateur (§6.1), déployé en `Deployment` Kubernetes (§10).

**Dépend de** : `graph-dsl`, `graph-index`, `graph-query`, `graph-proto`,
`graph-observability`, `graph-cluster`, `graph-storage`.

**Ce qu'elle expose.** Un `[lib]` en plus du binaire (`config`,
`grpc_partition_rpc`, `service`) — nécessaire pour que le test
d'intégration CO4 puisse instancier un vrai `GraphServiceImpl` sans
dupliquer le code.

**Fait (CO1-CO4, Phase 1).** `GraphServiceImpl` : `get_schema`,
`get_index_status`, `health_check`.

**Fait (CO5, Phase 2).** `remote_executor.rs` (l'ancien chemin
mono-partition dédié, un seul `PartitionServiceClient` stocké) est
**supprimé**, remplacé par `grpc_partition_rpc::GrpcPartitionRpc` —
implémente `graph_query::PartitionRpc` contre le vrai client gRPC
généré, avec un cache de connexions par adresse de réplique et
l'interceptor de trace (`inject_trace_context`, OB3) appliqué à chaque
client. `execute_query` : parse → valide → plan → interroge
`graph_cluster::Discovery` pour la `PartitionMap` courante →
`ScatterGatherExecutor::execute` → hydrate `RETURN` via
`ScatterGatherExecutor::get_node_properties` (groupé par partition
propriétaire) → streame. Pas de branche de code séparée pour le cas
mono-partition — le même exécuteur gère `n_partitions: 1` correctement
(une seule partition à diffuser/router), validé par CO4 qui tourne
maintenant à travers ce chemin. `Config` : `GRAPH_DISCOVERY_MODE`
(`static` — `GRAPH_PARTITION_NODE_ADDRS`, ou `kubernetes` —
`GRAPH_K8S_NAMESPACE`/`GRAPH_K8S_LABEL_SELECTOR`/`GRAPH_K8S_PARTITION_PORT`),
`GRAPH_N_PARTITIONS`, `GRAPH_COORDINATOR_METRICS_LISTEN_ADDR` (OB4).

**Gap documenté.** `get_index_status` relaie la partition de plus petit
id comme échantillon représentatif plutôt que d'agréger l'état des N
partitions — `IndexStatusResponse` (§8.2) n'a pas été conçu pour une vue
multi-partitions ; pas une TBD bloquante, juste non résolue ici.

**Fait (extension hors roadmap, CO6).** La projection finale distingue,
par binding, si un alias `RETURN` désigne un nœud ou une arête —
`alias_is_edge`, résolu en observant simplement dans quelle map (`nodes`
ou `edges`) du premier binding disponible l'alias apparaît — avant de
choisir entre projection d'enregistrement complet (alias nu) et
projection scalaire unique (`alias.propriété`). `GrpcPartitionRpc` gagne
`resolve_all`/`get_edge_properties`/`check_anti_join`, symétriques des
RPC déjà câblées pour Q5-Q8.

---

## bin/graph-partition-node — ✅ terminé (Phase 1)

**Rôle.** Process réseau implémentant `PartitionService` — le rôle nœud de
partition (§6.1), déployé en `StatefulSet` Kubernetes (§10). Héberge une
réplique d'une partition et fait tourner le cycle de rebuild périodique.

**Dépend de** : `graph-storage`, `graph-index`, `graph-dsl` (pour les
types du plan), `graph-query`, `graph-proto`, `graph-observability`.

**Ce qu'elle expose.** Un `[lib]` en plus du binaire (`config`,
`rebuild`, `service`), pour la même raison que `graph-coordinator`.

**Fait (PN1-PN5, Phase 1).** `rebuild::bootstrap` (premier build
synchrone) et `rebuild::periodic_rebuild_loop` (rebuild périodique,
`swap` atomique en cas de succès, ancienne génération conservée et
erreur logguée sinon) ; `PartitionServiceImpl` complet :
`resolve_start`/`expand_hop` (délégation à `graph_query::
SimpleLocalExecutor`, traduction wire ↔ types du plan), `get_index_status`,
`health_check`. Config via variables d'environnement
(`GRAPH_SCHEMA_PATH`, `GRAPH_WAREHOUSE_PATH`, `GRAPH_NAMESPACE`,
`GRAPH_PARTITION_ID`, `GRAPH_N_PARTITIONS`, `GRAPH_REBUILD_INTERVAL_SECS`,
`GRAPH_PARTITION_LISTEN_ADDR`).

**Fait (Phase 2, câblage CO5/OB3/OB4).**
- `IcebergIndexBuilder` reçoit maintenant `config.n_partitions` (au lieu
  d'un `1` implicite) — les rebuilds filtrent réellement aux nœuds
  possédés et calculent les `RemoteRef` réels (IX3 révisée).
- `expand_hop` fusionne `Frontier.local` et `Frontier.remote` dans le
  même flux de réponse gRPC (voir `TASKS.md`, note de contrat réseau
  rattachée à CO5) — pas de changement de `graph.proto`.
- Le service est enveloppé par `PartitionServiceServer::with_interceptor`
  (`extract_and_continue`, OB3) : chaque RPC entrant hérite du contexte
  de trace du coordinateur avant même d'atteindre `PartitionServiceImpl`.
- `/metrics` (`GRAPH_PARTITION_METRICS_LISTEN_ADDR`, OB4) exposé en
  tâche de fond ; `rebuild.rs` observe durée de build, taille d'index,
  âge de snapshot ; `PartitionServiceImpl::expand_hop` observe la
  latence de hop local.

**Test d'intégration (CO4, côté `graph-coordinator`).** Lance un vrai
`PartitionServiceImpl` et un vrai `GraphServiceImpl`, tous deux servant
sur de vrais sockets TCP loopback, connectés par de vrais clients gRPC
générés — exécute la requête k-hop exemple du spec §7.1 de bout en bout.
**C'est le jalon MVP mono-partition (spec §12 Phase 1).**

**Fait (extension hors roadmap, PN6).** `PartitionServiceImpl` gagne
`resolve_all` (scan complet d'un label, pour un `MATCH` sans filtre),
`check_anti_join` (évalue `NOT EXISTS` localement — un hop sortant, les
propriétés déjà résolues fournies par l'appelant) et
`get_edge_properties` (symétrique de `get_node_properties`, Phase 1) ;
`expand_hop` câblé pour `edge_alias`.

**Test d'intégration dédié (LP1, côté `graph-coordinator`).** Même
forme que CO4, sur `schema/least_privilege.graphidl`, mais **deux**
partitions réelles (pas une) pour que `check_anti_join` franchisse
effectivement une frontière de partition, et la requête centrale du use
case moindre-privilège verbatim — voir `TASKS.md` et `ARCHITECTURE.md`
§6.

**Déploiement multi-process, vérifié réellement.** Le catalogue dev est
`iceberg-catalog-sql` + SQLite (`open_sql_catalog`, révision de ST1) — un
fichier partagé, pas un registre en mémoire propre au process. Vérifié
en lançant quatre process séparés pour de vrai : `ingest-cloud-cost`
(ingestion), `graph-partition-node`, `graph-coordinator`, `query-client`
(un CLI minimal pour interroger un coordinateur en tournant) — la requête
traverse les quatre et renvoie le bon résultat. `examples/demo` continue
d'utiliser `MemoryCatalog` : il ingère et interroge dans le même process,
où ça reste le choix le plus simple.

---

## examples/ + bin/graph-viewer-server — démonstration et outillage

Hors du moteur lui-même, pour le prendre en main :

- **`examples/demo`** (`graph-engine-demo`) — tout en un seul process :
  ingère un petit graphe Cloud Cost Management, construit l'index,
  exécute la requête exemple, affiche le résultat. Utilise `MemoryCatalog`
  (pas besoin de partage inter-process ici). `cargo run -p graph-engine-demo`.
- **`examples/ingest-cloud-cost`** — le même jeu de données que le démo,
  mais écrit dans le catalogue SQLite persistant, en tant que process
  autonome (le rôle "pipeline d'ingestion externe" du spec §4.3, joué
  pour de vrai plutôt que replié dans le process du démo).
  `cargo run -p ingest-cloud-cost`.
- **`examples/query-client`** — CLI minimal pour interroger un
  `graph-coordinator` en cours d'exécution (`GRAPH_COORDINATOR_ADDR`,
  requête DSL en argument), utile pour vérifier un déploiement sans
  relire des assertions de test. `cargo run -p query-client -- '<DSL>'`.
- **`bin/graph-viewer-server`** — pont HTTP↔gRPC : sert `viewer/index.html`
  en statique et expose `POST /api/query`, qui relaie le DSL tapé à un
  `graph-coordinator` réel (`GraphService::ExecuteQuery`) et retourne les
  lignes en JSON, propriétés incluses (via `GetNodeProperties`, cf.
  section `GetNodeProperties` ci-dessus). `cargo run -p graph-viewer-server`.
- **`viewer/index.html`** — page servie par `graph-viewer-server`,
  visualisant ce même jeu de données. Bascule entre "graphe complet" et
  "résultat de la requête" (chaque nœud exclu annoté de la raison — filtré
  par le `WHERE`, ou jamais atteint par le pattern). Le panneau de requête
  est éditable : chaque clic sur "Run query" est un vrai aller-retour HTTP
  vers `graph-viewer-server`, donc un vrai aller-retour gRPC vers
  `graph-coordinator` — aucune logique de requête n'est ré-implémentée
  côté navigateur.

Séquence pour un vrai déploiement multi-process (vérifiée, cinq process
séparés) : `ingest-cloud-cost` → `graph-partition-node` →
`graph-coordinator` → `graph-viewer-server` (+ `query-client` pour
vérifier en CLI), `GRAPH_CATALOG_DB_PATH`/`GRAPH_WAREHOUSE_PATH`/
`GRAPH_NAMESPACE` identiques entre l'ingestion et `graph-partition-node`,
`GRAPH_SCHEMA_PATH` identique sur `graph-partition-node` et
`graph-coordinator`.

---

## Ordre d'implémentation suggéré (Phase 0 → Phase 2, spec §12)

1. **`graph-schema`** — parser IDL + `SchemaEvolution`. Rien d'autre ne
   peut avancer sans un `Schema` concret.
2. **`graph-storage`** — intégration Iceberg réelle. Nécessaire pour avoir
   de la donnée à indexer.
3. **`graph-index`** — `IndexBuilder::build` réel, range queries sur
   `PropertyIndex`.
4. **`graph-dsl`** — grammaire + parser + validateur.
5. **`graph-query`** — `Planner` + `LocalExecutor`. À ce stade, un
   **MVP mono-partition complet** est possible (§12 Phase 1) : un seul
   `graph-partition-node` avec `n_partitions = 1`, un `graph-coordinator`
   qui route tout localement — valide le modèle de données et le DSL de
   bout en bout avant d'attaquer la distribution.
6. **`graph-cluster`** — hash stable, `Discovery` Kubernetes,
   `RebalancePlanner`.
7. **`graph-query::ScatterGatherExecutor`** — le scatter-gather réel
   (Phase 2, §12), maintenant que `graph-cluster` sait router.
8. **`graph-observability`** — le câblage tracing/métriques réel peut être
   fait en parallèle de n'importe quelle étape ci-dessus, mais devient
   nécessaire pour de vrai à partir de la Phase 2 (observer le fan-out
   inter-partitions, §9.2).
