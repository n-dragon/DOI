# Guide d'implémentation — rôle de chaque crate

Ce document complète `ARCHITECTURE.md` (qui montre le flux d'une requête
de bout en bout) en détaillant, crate par crate : son rôle exact, ce
qu'elle expose, de quoi elle dépend, ce qui est déjà écrit (squelette) et
ce qu'il reste à implémenter. Sert de feuille de route pour la Phase 0/1
(spec §12).

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

**Reste à faire (Phase 1).**
- `IndexBuilder::build` : scanner les tables via `IcebergReader`, filtrer
  aux nœuds/arêtes appartenant à la partition (via `graph-cluster::PartitionHasher`),
  construire les tableaux CSR (calcul des offsets, tri par nœud source)
  et les B-Tree de propriété (seulement pour les propriétés `indexed:
  true` du schéma).
- Support des requêtes par plage (`WHERE friend.birth_year > 1990`) sur
  `PropertyIndex` — actuellement seule l'égalité (`lookup_eq`) est câblée.
- Décider la valeur par défaut de l'intervalle de rebuild (§5.3, `TBD`).

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
- Grammaire formelle complète (alias avancés, `ORDER BY`, `LIMIT`,
  pagination — encore `TBD` dans le spec §7.1).

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
- `DistributedExecutor` : trait à implémenter — la boucle scatter-gather
  complète (§7.4, Figure 2 du diagramme d'architecture) : fan-out du plan
  vers les partitions concernées, dédoublonnage, ré-émission au hop
  suivant.

**Dépend de** : `graph-schema`, `graph-dsl`, `graph-index`. **Dépendent
d'elle** : les deux binaires.

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

**Reste à faire.**
- Phase 2 : `DistributedExecutor` — utilise `graph-cluster::PartitionMap`
  pour router chaque étape vers les bonnes répliques via `graph-proto`.

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

**Fait.** Tous les types de placement, `PartitionHasher` (avec un hash
placeholder à remplacer), `PartitionMap::healthy_replicas`.

**Reste à faire (Phase 2).**
- Choisir une fonction de hash stable (ex: xxhash) et la figer
  définitivement — elle ne doit plus jamais changer une fois le graphe créé.
- `Discovery` : implémentation Kubernetes (crate `kube` — API
  Pods/Endpoints ou headless Service, §6.3/§10).
- `RebalancePlanner` : stratégie concrète (minimiser le déplacement de
  données, équilibrer la charge) — non détaillée dans le spec au-delà du
  principe "réaffecter, jamais rehasher" (§6.2).

---

## graph-observability

**Rôle.** Instrumentation partagée (spec §9) pour que les deux binaires ne
divergent pas sur les noms de métriques ou la configuration du tracing.

**Ce qu'elle expose.** Module `metrics` (constantes de noms Prometheus,
alignées sur la liste de §9.1), `init_tracing`, `TraceContext`.

**Dépend de** : `tracing` uniquement. **Dépendent d'elle** : les deux
binaires.

**Fait.** Les noms de métriques figés, les points d'entrée en place
(appelés dès `main()` dans les deux binaires).

**Reste à faire.**
- Câblage réel `tracing-subscriber` + exporteur OpenTelemetry (§9.2) et
  logs JSON structurés (§9.3 — détail retenu comme `TBD` dans le spec).
- Propagation du `TraceContext` sur chaque appel gRPC coordinateur →
  partition (interceptor `tonic`), pour qu'une requête reste une seule
  trace connectée à travers le fan-out (§9.2).
- Exposition de l'endpoint `/metrics` (Prometheus) sur chaque binaire.

---

## bin/graph-coordinator — ✅ terminé (Phase 1, mode mono-partition)

**Rôle.** Process réseau implémentant `GraphService` — le rôle
coordinateur (§6.1), déployé en `Deployment` Kubernetes (§10).

**Dépend de** : `graph-dsl`, `graph-index`, `graph-query`, `graph-proto`,
`graph-observability`. (`graph-cluster` seulement à partir de CO5,
Phase 2 — mono-partition v1 n'a rien à découvrir.)

**Ce qu'elle expose.** Un `[lib]` en plus du binaire (`config`,
`remote_executor`, `service`) — nécessaire pour que le test d'intégration
CO4 puisse instancier un vrai `GraphServiceImpl` sans dupliquer le code.

**Fait (CO1-CO4).** `GraphServiceImpl` complet : `get_schema`,
`execute_query` (parse → valide → plan → `remote_executor::execute`,
qui traduit chaque `PlanStep` en appel gRPC réel contre le nœud de
partition configuré → projette `RETURN` → streame), `get_index_status`
(relaie celui du nœud de partition), `health_check`. Config via
variables d'environnement (`GRAPH_SCHEMA_PATH`,
`GRAPH_PARTITION_NODE_ADDR`, `GRAPH_COORDINATOR_LISTEN_ADDR`).

**Reste à faire (Phase 2).** `DistributedExecutor`/CO5 : remplacer
l'appel direct à un seul nœud de partition par un vrai scatter-gather via
`graph_cluster::Discovery` une fois plus d'une partition existe.

---

## bin/graph-partition-node — ✅ terminé (Phase 1)

**Rôle.** Process réseau implémentant `PartitionService` — le rôle nœud de
partition (§6.1), déployé en `StatefulSet` Kubernetes (§10). Héberge une
réplique d'une partition et fait tourner le cycle de rebuild périodique.

**Dépend de** : `graph-storage`, `graph-index`, `graph-dsl` (pour les
types du plan), `graph-query`, `graph-proto`, `graph-observability`.

**Ce qu'elle expose.** Un `[lib]` en plus du binaire (`config`,
`rebuild`, `service`), pour la même raison que `graph-coordinator`.

**Fait (PN1-PN5).** `rebuild::bootstrap` (premier build synchrone) et
`rebuild::periodic_rebuild_loop` (rebuild périodique, `swap` atomique en
cas de succès, ancienne génération conservée et erreur logguée sinon) ;
`PartitionServiceImpl` complet : `resolve_start`/`expand_hop` (délégation
à `graph_query::SimpleLocalExecutor`, traduction wire ↔ types du plan),
`get_index_status`, `health_check`. Config via variables d'environnement
(`GRAPH_SCHEMA_PATH`, `GRAPH_WAREHOUSE_PATH`, `GRAPH_NAMESPACE`,
`GRAPH_PARTITION_ID`, `GRAPH_N_PARTITIONS`, `GRAPH_REBUILD_INTERVAL_SECS`,
`GRAPH_PARTITION_LISTEN_ADDR`).

**Test d'intégration (CO4, côté `graph-coordinator`).** Lance un vrai
`PartitionServiceImpl` et un vrai `GraphServiceImpl`, tous deux servant
sur de vrais sockets TCP loopback, connectés par de vrais clients gRPC
générés — exécute la requête k-hop exemple du spec §7.1 de bout en bout.
**C'est le jalon MVP mono-partition (spec §12 Phase 1).**

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

## examples/ — démonstration et outillage

Trois petits binaires, plus une page web statique, hors du moteur
lui-même, pour le prendre en main :

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
- **`viewer/index.html`** — page HTML autonome (aucune dépendance, aucun
  serveur) visualisant ce même jeu de données et la même requête : bascule
  entre "graphe complet" et "résultat de la requête" (chaque nœud exclu
  annoté de la raison — coût trop bas, ou trop de hops). Snapshot statique
  du résultat déjà vérifié via `query-client`, pas un client qui interroge
  un moteur en direct.

Séquence pour un vrai déploiement multi-process (vérifiée) :
`ingest-cloud-cost` → `graph-partition-node` → `graph-coordinator` →
`query-client`, chacun un process séparé, `GRAPH_CATALOG_DB_PATH`/
`GRAPH_WAREHOUSE_PATH`/`GRAPH_NAMESPACE` identiques entre l'ingestion et
`graph-partition-node`.

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
7. **`graph-query::DistributedExecutor`** — le scatter-gather réel
   (Phase 2, §12), maintenant que `graph-cluster` sait router.
8. **`graph-observability`** — le câblage tracing/métriques réel peut être
   fait en parallèle de n'importe quelle étape ci-dessus, mais devient
   nécessaire pour de vrai à partir de la Phase 2 (observer le fan-out
   inter-partitions, §9.2).
