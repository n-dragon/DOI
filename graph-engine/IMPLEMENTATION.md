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
`SchemaError`.

**Reste à faire (Phase 0).**
- Grammaire concrète de l'IDL et son parser (le spec §3.4 donne un exemple
  illustratif proche Protobuf/Avro, la grammaire formelle n'est pas
  figée) — candidat : `pest` (grammaire déclarative en `.pest`, lisible)
  ou un parser combinator (`chumsky`/`nom`) si on veut de meilleurs
  messages d'erreur.
- Logique de `SchemaEvolution::diff` (comparer deux `Schema`, détecter
  suppression/renommage/changement de type de propriété).
- Génération du schéma des tables Iceberg à partir d'un `Schema` (utilisée
  par le pipeline d'ingestion externe, hors binaire du moteur — mais le
  mapping doit être défini ici pour rester la source de vérité, §3.4).

---

## graph-storage

**Rôle.** Accès lecture seule à Apache Iceberg (spec §4). Ne sait rien de
l'indexation ni des requêtes — juste "donne-moi les lignes d'une table à
un snapshot donné".

**Ce qu'elle expose.**
- `SnapshotId`, `NodeRow`, `EdgeRow`, `PropertyValue`, `StorageError`.
- `IcebergReader` : trait à implémenter — `latest_snapshot`, `scan_nodes`,
  `scan_edges`.

**Dépend de** : `graph-schema`. **Dépend d'elle** : `graph-index`.

**Fait.** Les types de lignes/valeurs, le contrat du trait.

**Reste à faire (Phase 0/1).**
- Choisir et intégrer une crate Iceberg Rust (le projet
  `apache/iceberg-rust` est le candidat naturel — catalogue + FileIO).
- Implémentation concrète de `latest_snapshot` (résolution via le
  catalogue Iceberg) et des deux `scan_*` (lecture Parquet sous-jacente,
  désérialisation ligne → `NodeRow`/`EdgeRow` selon le `Schema`).
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
  `Query`.
- `Parser` : trait à implémenter — texte DSL → `Query`.
- `Validator` : trait à implémenter — `Query` + `Schema` → erreurs de
  validation, le cas échéant.

**Dépend de** : `graph-schema`. **Dépendent d'elle** : `graph-query`,
`graph-coordinator`.

**Fait.** L'AST complet pour les deux opérations prioritaires (k-hop
filtré, pattern matching) ; volontairement **aucune** construction
d'agrégation dans l'AST (décision actée, spec §1.4/§7.1).

**Reste à faire (Phase 0).**
- Grammaire formelle complète (alias, `ORDER BY`, `LIMIT`, pagination —
  encore `TBD` dans le spec §7.1) puis son parser. Candidat : `pest` (même
  outillage que pour l'IDL du schéma, cohérence d'outils) ou `chumsky`
  pour de meilleurs messages d'erreur orientés utilisateur.
- Implémentation de `Validator` : vérifier existence des labels/types de
  relation, compatibilité type de propriété ↔ opérateur de comparaison
  utilisé.

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

**Fait.** Tous les types intermédiaires et contrats. Le découplage
`LocalExecutor`/`DistributedExecutor` est déjà posé : `LocalExecutor` est
tout ce dont a besoin le MVP mono-partition (§12, Phase 1) — pas besoin
d'attendre `graph-cluster` ni le réseau pour valider le modèle de données
et le DSL de bout en bout.

**Reste à faire.**
- Phase 1 : `Planner::plan` (lowering naïf, sans optimisation — §7.3 note
  déjà que l'ordre de filtres/choix d'index reste `TBD`) et
  `LocalExecutor` (implémente réellement les deux variantes de
  `PlanStep`).
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

**Reste à faire.** Ajuster les messages une fois la grammaire DSL et les
types de `PlanStep`/`Frontier` stabilisés (le `Value` `oneof` actuel
couvre les types scalaires de base, `List`/`Vector` du schéma §3.2 n'y
sont pas encore représentés).

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

## bin/graph-coordinator

**Rôle.** Process réseau implémentant `GraphService` — le rôle
coordinateur (§6.1), déployé en `Deployment` Kubernetes (§10).

**Dépend de** : `graph-dsl`, `graph-query`, `graph-proto`, `graph-cluster`,
`graph-observability`.

**Fait.** Le serveur gRPC démarre, expose `HealthCheck` fonctionnel ; les
trois autres routes renvoient `unimplemented` en attendant leurs
dépendances.

**Reste à faire.** Construire et injecter dans `GraphServiceImpl` des
implémentations concrètes de `graph_dsl::Parser`/`Validator`,
`graph_query::Planner`/`DistributedExecutor`, `graph_cluster::Discovery` —
puis implémenter `execute_query` (parse → valide → plan → exécute →
streame), `get_schema` et `get_index_status`.

---

## bin/graph-partition-node

**Rôle.** Process réseau implémentant `PartitionService` — le rôle nœud de
partition (§6.1), déployé en `StatefulSet` Kubernetes (§10). Héberge une
réplique d'une partition et fait tourner le cycle de rebuild périodique.

**Dépend de** : `graph-storage`, `graph-index`, `graph-query`,
`graph-proto`, `graph-observability`.

**Fait.** Le serveur gRPC démarre, expose `HealthCheck` fonctionnel, le
squelette de la boucle de rebuild périodique (`rebuild::periodic_rebuild_loop`)
tourne en tâche de fond dès le démarrage.

**Reste à faire.** `rebuild::bootstrap` (construire un `IcebergReader` et
un `IndexBuilder` concrets, faire le premier build synchrone avant de
servir), le corps réel de `periodic_rebuild_loop` (appeler
`IndexBuilder::build` puis `GenerationHandle::swap`), et
`PartitionServiceImpl::resolve_start`/`expand_hop` (délégation à
`graph_query::LocalExecutor`).

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
