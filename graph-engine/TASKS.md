# Tâches unitaires — implémentation

Découpage de `IMPLEMENTATION.md` en tâches unitaires : chacune est
indépendamment implémentable et testable (une fonction/un trait à la
fois), avec ses dépendances explicites. Regroupées par crate, dans
l'ordre de la roadmap (Phase 0 → Phase 2, spec §12). Identifiants courts
(`S1`, `ST2`, ...) pour se référencer entre tâches sans répéter l'intitulé.

Convention : chaque tâche livre du code **et** son test unitaire — une
tâche n'est "faite" que si elle compile et que son test passe.

---

## Phase 0 — Fondations

### graph-schema — ✅ terminé (Phase 0)

- ✅ **S1** — Écrire la grammaire `pest` de l'IDL (syntaxe `schema`, `node`,
  `edge`, types scalaires §3.2) dans un fichier `.pest`.
- ✅ **S2** — Implémenter `SchemaParser::parse` : IDL → `Schema` en utilisant
  S1. *(dépend de S1)* — `PestSchemaParser`.
- ✅ **S3** — Tests : parser un schéma valide multi-nœuds/arêtes (l'exemple
  §3.4) et vérifier le `Schema` produit. *(dépend de S2)*
- ✅ **S4** — Tests : cas d'erreur (label dupliqué, type de relation avec
  label source/cible inconnu, propriété sans type) → `SchemaError::Parse`.
  *(dépend de S2)*
- ✅ **S5** — Implémenter `SchemaEvolution::diff` : cas compatible (ajout de
  propriété optionnelle, ajout de label/type de relation).
- ✅ **S6** — Implémenter `SchemaEvolution::diff` : cas incompatible
  (suppression de propriété, changement de type). *(dépend de S5 ; le
  renommage n'est pas distingué d'un remove+add, l'IDL n'a pas de
  marqueur de renommage — voir `evolution.rs`)*
- ✅ **S7** — Tests `SchemaEvolution::diff` pour chaque cas de S5/S6.
  *(dépend de S6)*

### graph-dsl

### graph-dsl — ✅ terminé (Phase 0)

- ✅ **D1** — Écrire la grammaire `pest` du DSL : `MATCH` avec motif
  nœud/arête, hop range (`*1..3`), `WHERE`, `RETURN` (§7.1).
- ✅ **D2** — Implémenter `Parser::parse` pour un `NodePattern` seul (ex:
  `(p:Person {name: "Alice"})`). *(dépend de D1)* — `PestParser`.
- ✅ **D3** — Implémenter `Parser::parse` pour une chaîne `Pattern` complète
  (nœuds + arêtes typées + hop range). *(dépend de D2)*
- ✅ **D4** — Implémenter `Parser::parse` pour la clause `WHERE`
  (`PropertyFilter` + `ComparisonOp`). *(dépend de D3)* — inclut
  `WhereCondition::AliasComparison` pour `colleague <> p` (§7.1), une
  comparaison entre deux alias liés plutôt qu'une propriété.
- ✅ **D5** — Tests parser : les deux exemples du spec §7.1 (k-hop filtré,
  pattern matching) donnent l'AST attendu. *(dépend de D4)*
- ✅ **D6** — Tests parser : cas d'erreur de syntaxe → `DslError::Syntax`.
  *(dépend de D4)*
- ✅ **D7** — Implémenter `Validator::validate` : existence des labels/types
  de relation contre le `Schema`. *(dépend de S2)* — `SchemaValidator`.
- ✅ **D8** — Implémenter `Validator::validate` : compatibilité type de
  propriété ↔ `ComparisonOp` utilisé dans `WHERE`. *(dépend de D7)*
- ✅ **D9** — Tests validateur : requête valide passe, requête avec label
  inconnu / opérateur incompatible échoue avec le bon `ValidationError`.
  *(dépend de D8)*

### graph-storage — ✅ terminé (Phase 0)

- ✅ **ST1** — Choisir et intégrer une crate Iceberg Rust (`apache/iceberg-rust`
  ou équivalent) ; définir la config de catalogue (local/filesystem pour
  dev, candidat prod à trancher séparément). — crate `iceberg` v0.10 ;
  dev = `iceberg-catalog-sql` + SQLite + `FileIO` filesystem local
  (`graph_storage::open_sql_catalog`) ; prod = `TBD` (candidat : catalogue
  REST). Décisions détaillées dans le doc-comment de `iceberg_reader.rs`
  et de `catalog.rs`.

  *(Révision post-PN/CO)* Le choix initial (`MemoryCatalog`) a un
  registre de tables en mémoire, par processus — testé en le déployant
  réellement (ingestion + `graph-partition-node` en process séparés), il
  ne fonctionne pas dès que plus d'un processus est impliqué
  (`NamespaceNotFound` malgré les fichiers Parquet présents sur disque).
  Remplacé par `iceberg-catalog-sql` + SQLite (`SqlCatalogBuilder`,
  auto-migration au premier `connect`) : même principe "zéro infra" pour
  le dev, mais le registre est un fichier partagé entre processus. Testé
  par un vrai déploiement multi-process (`ingest-cloud-cost` +
  `graph-partition-node` + `graph-coordinator` + `query-client`, quatre
  processus séparés, requête correcte de bout en bout) en plus d'un test
  de régression dédié dans `graph-storage`.
- ✅ **ST2** — Implémenter `latest_snapshot` pour une table donnée.
  *(dépend de ST1)*
- ✅ **ST3** — Implémenter la désérialisation ligne Parquet → `PropertyValue`
  pour chaque variante de `ScalarType` (§3.2). *(dépend de ST1)* — via
  Arrow (`to_arrow()` de la crate `iceberg` retourne des `RecordBatch`,
  pas du Parquet brut).
- ✅ **ST4** — Implémenter `scan_nodes` (filtré par `label`) en s'appuyant sur
  ST2/ST3. *(dépend de ST2, ST3)*
- ✅ **ST5** — Implémenter `scan_edges` (filtré par `edge_type`).
  *(dépend de ST2, ST3)*
- ✅ **ST6** — Test d'intégration : écrire une table Iceberg de test (fixture
  minimale), vérifier `scan_nodes`/`scan_edges` retournent les lignes
  attendues. *(dépend de ST4, ST5)* — deux tests (nœuds et arêtes),
  écriture réelle via l'API `writer` de la crate, catalogue mémoire +
  FileIO local.

---

## Phase 1 — MVP mono-partition

### graph-index — ✅ terminé (Phase 1)

- ✅ **IX1** — Implémenter la construction du tableau CSR sortant (tri des
  arêtes par `src_node_id`, calcul des offsets) dans `IndexBuilder::build`.
  *(dépend de ST4, ST5)* — `IcebergIndexBuilder`.
- ✅ **IX2** — Implémenter la construction du tableau CSR entrant (même
  logique, trié par `dst_node_id`). *(dépend de IX1)*
- ✅ **IX3** — Détection de référence distante (`RemoteRef`) : un voisin dont
  le `node_id` hache vers une autre partition. *(dépend de IX2 ; nécessite
  `PartitionHasher`, cf. CL1 — no-op en Phase 1 mono-partition où tout est
  local)* — `dst_remote` toujours `None`, testé explicitement.
- ✅ **IX4** — Implémenter la construction de `PropertyIndex` (une seule
  propriété indexée pour commencer) à partir des lignes scannées.
  *(dépend de ST4)* — toutes les propriétés `@indexed` du schéma, pas
  qu'une seule.
- ✅ **IX5** — Ajouter le lookup par plage (`range`) sur `PropertyIndex`, en
  plus de `lookup_eq` déjà présent. *(dépend de IX4)* — via
  `std::ops::Bound<PropertyKey>` (pas `graph_dsl::ComparisonOp` :
  `graph-index` ne dépend pas de `graph-dsl`, cf. graphe de dépendances
  en bas de ce fichier).
- ✅ **IX6** — Tests `TopologicalIndex` : voisins sortants/entrants corrects
  sur un petit graphe synthétique (5-10 nœuds). *(dépend de IX2)*
- ✅ **IX7** — Tests `PropertyIndex` : égalité et plage retournent les bons
  `NodeId`. *(dépend de IX5)*
- ✅ **IX8** — Test `GenerationHandle` : une requête qui a acquis une
  génération continue de la voir après un `swap()` concurrent (pas
  d'incohérence, pas de panic). *(dépend de IX1..IX5)*

### graph-query (exécution locale)

### graph-query (exécution locale) — ✅ terminé (Phase 1)

- ✅ **Q1** — Implémenter `Planner::plan` : lowering naïf d'un `Pattern` en
  séquence `ResolveStart` + `ExpandHop*` dans l'ordre du motif.
  *(dépend de D5)* — `NaivePlanner`. Portée v1 : nœud de départ étiqueté
  avec exactement un filtre d'égalité ; les comparaisons d'alias
  (`colleague <> p`) sont abandonnées plutôt qu'appliquées (lacune
  documentée, pas silencieuse).
- ✅ **Q2** — Implémenter `LocalExecutor::resolve_start` (délègue à
  `PropertyIndex::lookup_eq`/range). *(dépend de IX5, Q1)* —
  `SimpleLocalExecutor`.
- ✅ **Q3** — Implémenter `LocalExecutor::expand_hop` (délègue à
  `TopologicalIndex::out_neighbors`/`in_neighbors`, applique les filtres
  `WHERE` au plus tôt). *(dépend de IX2, Q1)* — BFS `hops.min..=hops.max`
  correct pour `*1..3` ; les filtres `WHERE` sont appliqués en une passe
  finale sur `to_alias` via l'index de propriété plutôt que poussés à
  chaque hop (optimisation explicitement `TBD` en §7.3 — la base
  naïve-mais-correcte est ce que cette optimisation doit améliorer).
- ✅ **Q4** — Test bout en bout local : plan + exécute la requête k-hop
  d'exemple (§7.1) contre une `IndexGeneration` construite en mémoire pour
  le test, vérifie les `Binding` obtenus. *(dépend de Q2, Q3)* — vérifie à
  la fois la borne du hop range et le filtre `WHERE birth_year > 1990`.

### bin/graph-partition-node — ✅ terminé (Phase 1)

- ✅ **PN1** — Implémenter `rebuild::bootstrap` : construit `IcebergReader` +
  `IndexBuilder` concrets, premier build synchrone. *(dépend de ST1, IX1)*
- ✅ **PN2** — Implémenter le corps de `periodic_rebuild_loop` (appelle
  `IndexBuilder::build`, puis `GenerationHandle::swap` si succès, logge et
  garde l'ancienne génération si échec). *(dépend de PN1)*
- ✅ **PN3** — Implémenter `PartitionServiceImpl::resolve_start` (délègue à
  `LocalExecutor::resolve_start`). *(dépend de Q2)*
- ✅ **PN4** — Implémenter `PartitionServiceImpl::expand_hop` (délègue à
  `LocalExecutor::expand_hop`). *(dépend de Q3)*
- ✅ **PN5** — Config du binaire : lecture de `n_partitions`/`partition_id`/
  intervalle de rebuild depuis l'environnement. *(`n_partitions`
  informationnel seulement en Phase 1, non encore utilisé pour filtrer —
  IX3 est un no-op tant que le partitionnement Phase 2 n'existe pas)*

  Le catalogue dev est maintenant persistant (`iceberg-catalog-sql` +
  SQLite, cf. révision ST1 ci-dessus) — `graph-partition-node` lit un
  fichier SQLite partagé, pas un registre en mémoire propre au process.
  Vérifié par un déploiement à quatre process séparés (`ingest-cloud-cost`,
  `graph-partition-node`, `graph-coordinator`, `query-client`).

### bin/graph-coordinator (mode mono-partition) — ✅ terminé (Phase 1)

- ✅ **CO1** — Implémenter `GraphServiceImpl::get_schema` (retourne le
  `Schema` actif). *(dépend de S2)*
- ✅ **CO2** — Implémenter `GraphServiceImpl::execute_query` en mode
  mono-partition : parse (D-crate) → valide (D7-D9) → plan (Q1) → exécute
  en appelant directement le `graph-partition-node` local (pas de
  scatter-gather multi-partitions à ce stade) → streame les résultats
  projetés. *(dépend de D9, Q4, PN3, PN4)* — via `remote_executor`, qui
  traduit chaque `PlanStep` en appel gRPC réel (`ResolveStart`/
  `ExpandHop`) contre `graph-proto`, étendu pour transporter ce qu'un
  `PlanStep::ExpandHop` porte réellement (hop range, filtres `WHERE`,
  alias) — le contrat proto d'origine, écrit avant Q1-Q4, ne le
  permettait pas.
- ✅ **CO3** — Implémenter `GraphServiceImpl::get_index_status` (relaie le
  `GenerationMeta` du nœud de partition). *(dépend de PN1)* — nécessite
  d'ajouter `GetIndexStatus` à `PartitionService` (absent du proto
  d'origine) et de changer `IndexStatusResponse.pinned_snapshot_id`
  (scalaire) en `pinned_snapshot_by_table` (map) puisque `GenerationMeta`
  épingle un snapshot par table, pas un seul pour toute la génération.
- ✅ **CO4** — Test d'intégration MVP : lancer `graph-partition-node` +
  `graph-coordinator` en mono-partition, exécuter une requête k-hop de
  bout en bout via le client gRPC généré. *(dépend de CO2)* — les deux
  services tournent réellement (vrais serveurs tonic sur TCP loopback,
  pas des appels de trait in-process), connectés par de vrais clients
  gRPC générés. **C'est le jalon Phase 1 : MVP mono-partition complet.**

> **Jalon** : CO4 qui passe = MVP mono-partition complet (spec §12 Phase
> 1) — modèle de données et DSL validés de bout en bout avant d'attaquer
> la distribution.

### `GetNodeProperties` + `graph-viewer-server` — ✅ terminé

Motivation : `execute_query` ne projetait qu'un `NodeId` bit-casté par
alias — suffisant pour un test d'intégration, mais inutilisable pour un
client humain (aucun moyen de savoir *quel* nœud a matché). Ajout :

- **`graph-index`** — `IndexGeneration` retient désormais `node_records:
  HashMap<NodeId, NodeRecord>` (label + propriétés complètes),
  peuplé dans `IcebergIndexBuilder::build` à partir des lignes déjà
  scannées pour construire la topologie/l'index de propriétés — aucun
  I/O supplémentaire, juste ce qui était jusque-là jeté après le build.
- **`graph-proto`** — nouvelle RPC `PartitionService::GetNodeProperties`
  (lookup par lot d'ids) et `QueryResult.properties` (map alias →
  `NodeProperties`), en plus de `projection` conservé tel quel.
- **`graph-partition-node`** — `GetNodeProperties` répond depuis
  `node_records` de la génération actuellement servie.
- **`graph-coordinator`** — après avoir résolu les bindings, `execute_query`
  déduplique les `NodeId` référencés par les alias du `RETURN`, appelle
  `GetNodeProperties` une seule fois, et hydrate chaque ligne du
  résultat avec les propriétés réelles.
- **`bin/graph-viewer-server`** (nouveau) — pont HTTP↔gRPC : sert
  `graph-engine/viewer/` en statique et expose `POST /api/query`, qui
  relaie le DSL tapé tel quel à un `graph-coordinator` réel via
  `GraphService::ExecuteQuery` et retourne les lignes en JSON. Aucune
  logique de requête ne tourne dans le navigateur — `viewer/index.html`
  appelle ce serveur, qui appelle le moteur réel.

---

## Phase 2 — Distribution — ✅ terminé

> **Jalon** : Phase 2 complète — cluster multi-partitions réel (hash
> stable, discovery Kubernetes, rebalancement, scatter-gather
> cross-partition, observabilité câblée) plutôt qu'un mono-partition
> extensible en théorie seulement. Vérifié par un test bout en bout
> (`graph-query`, Q8) où une traversée `*1..3` franchit réellement une
> frontière de partition en re-routant sur le réseau (simulé en process
> pour le test, identique au chemin réel `bin/graph-coordinator` ↔
> `bin/graph-partition-node` — voir CO5), et par le test d'intégration
> CO4 existant, toujours vert, tournant maintenant à travers le nouveau
> chemin distribué en mode 1 partition plutôt que l'ancien chemin
> mono-partition dédié (supprimé — voir CO5).

### graph-cluster — ✅ terminé (Phase 2)

- ✅ **CL1** — `xxh3_64` (crate `xxhash-rust`) remplace le placeholder
  modulo. *Décision* : la formule canonique `hash(node_id) % n_partitions`
  a été déplacée dans `graph-schema::partitioning` (fondation sans
  dépendance interne) plutôt que de rester dans `graph-cluster` — la
  builder de `graph-index` (IX3, révisée ci-dessous) doit calculer cette
  même formule pour détecter les arêtes cross-partition, et
  `graph-index` ne peut pas dépendre de `graph-cluster` (qui dépend
  lui-même de `graph-index` via `PartitionId`) sans créer un cycle. Un
  seul point de définition, deux points d'appel — l'invariant "jamais de
  drift" du §6.2 reste vrai. Tests : déterminisme, non-préservation de
  l'ordre (contrairement au placeholder modulo), distribution sur un
  échantillon.
- ✅ **CL2** — `KubernetesDiscovery` via `kube::Api<Pod>` (pas
  `Endpoints`/`EndpointSlice`). *Décision* : lit directement les objets
  `Pod` plutôt que l'API `Endpoints` d'un `Service` headless — la
  propriété de partition (quelles partitions logiques une réplique sert)
  est portée par une **annotation Pod** (`graph.io/partitions`,
  liste d'ids séparés par des virgules), que seule l'API Pods expose
  nativement sans un second aller-retour pour croiser Endpoint → Pod.
  *Décision* : cette annotation est **lue**, pas calculée, par
  `Discovery` — le calcul de placement reste le rôle de
  `RebalancePlanner` (CL3) ; un opérateur/outillage de déploiement
  (hors scope v1) est responsable de l'appliquer sur les Pods.
  `StaticDiscovery` (fixe, en mémoire) ajoutée en complément pour le dev
  local et les tests (`bin/graph-coordinator` en mode
  `GRAPH_DISCOVERY_MODE=static`, cf. CO5).
- ✅ **CL3** — `RendezvousRebalancePlanner`. *Décision* : hachage de
  rendez-vous (HRW — plus haut score aléatoire stable) plutôt qu'un
  simple `partition_id % machines.len()`. Le modulo réassigne la
  majorité des partitions à chaque changement de machine (problème
  classique du modulo-hashing) — contredit directement l'objectif du
  §6.2 ("seules les partitions déplacées ont leur index reconstruit").
  Le hachage de rendez-vous donne cette propriété de "perturbation
  minimale" sans structure d'anneau complète : pour chaque partition, un
  score stable `hash(partition, machine)` est calculé pour chaque
  machine candidate, les `N` meilleures sont retenues (`N` =
  facteur de réplication, §6.4). Testé : placement initial, ajout d'une
  machine ne déplace qu'une minorité des partitions, replanification
  idempotente à entrées inchangées.
- ✅ **CL4** — Tests `PartitionMap::healthy_replicas` : filtre bien les
  répliques marquées non-saines, liste vide si partition inconnue ou
  toutes répliques non-saines.

### graph-query (exécution distribuée) — ✅ terminé (Phase 2)

- ✅ **Q5** — `ScatterGatherExecutor::execute` (remplace le trait stub
  `DistributedExecutor`, supprimé). *Décision, révision du libellé de
  tâche d'origine* : `ResolveStart` est **diffusé à toutes les
  partitions** plutôt que routé vers une seule via `PartitionHasher`.
  L'index de propriété (§5.2) est construit par partition, sur les
  nœuds locaux uniquement (`graph-index`, builder) — il n'existe pas
  d'index secondaire global en v1. `PartitionHasher` répond à "quelle
  partition possède cet id *connu*" — exactement ce dont chaque hop
  *suivant* a besoin (Q6), mais `ResolveStart` filtre par une propriété
  (`{name: "Alice"}`), pas par un id à hacher. Chaque partition est donc
  interrogée ; la propriété d'appartenance disjointe garantit qu'aucun
  doublon ne peut apparaître entre les réponses.
- ✅ **Q6** — Boucle scatter-gather **par hop physique**, pas par étape
  de plan. *Décision* : un `PlanStep::ExpandHop{hops: min..max}` est
  décomposé en `max` rounds réseau successifs d'un seul saut chacun —
  lecture littérale du §7.4 ("à chaque hop... ré-envoie pour le hop
  suivant"), plutôt que d'envoyer toute la plage à une seule partition
  (ce que fait encore, par simplicité, le chemin mono-partition
  historique — désormais un cas particulier du même exécuteur, pas un
  chemin de code séparé, voir CO5). *Changement associé (IX3, révisé)* :
  `local_executor::SimpleLocalExecutor::expand_hop` peuple enfin
  `Frontier.remote` (toujours vide en Phase 1) — un voisin `RemoteRef`
  rencontré pendant le hop local est renvoyé avec `to_alias` déjà lié,
  prêt à réamorcer le round suivant sur la partition propriétaire.
  *Décision (filtres WHERE)* : évalués **après coup**, une fois par
  étape `ExpandHop` (pas par round, pas en push-down par partition) via
  `filter_frontier`, qui hydrate les propriétés des candidats via
  `GetNodeProperties` (déjà utilisé pour l'hydratation `RETURN`) et
  compare en mémoire côté coordinateur plutôt que de re-router vers
  l'index de propriété de chaque partition pour un lot d'ids déjà
  connus. Correct car un `PropertyFilter` ne contredit jamais un autre
  binding (§7.1) : le filtrage commute avec l'union
  (`filter(A ∪ B) = filter(A) ∪ filter(B)`).
- ✅ **Q7** — Déduplication par clé `(alias, node_id)` triée (pas de
  `Hash` sur `HashMap`), appliquée à la fusion `ResolveStart`, à chaque
  round, et sur le résultat final — dédoublonnage uniquement, jamais de
  réduction/agrégat (§7.1/§1.4).
- ✅ **Q8** — Test bout en bout : 2 partitions, Alice/Bob sur la
  partition 0, Carol/Dave sur la partition 1 — la chaîne `KNOWS`
  franchit la frontière exactement au hop 2 (Bob→Carol). Même jeu de
  données et même requête que le test Q4 mono-partition ; même résultat
  final attendu, prouvant l'équivalence observable des deux chemins.

*Décision de contrat réseau (rattachée à CO5, cf. plus bas)* :
`PartitionServiceImpl::expand_hop` (côté `graph-partition-node`) fusionne
`Frontier.local` et `Frontier.remote` dans le **même flux gRPC** plutôt
que d'étendre le message `ExpandHopResponse` pour les distinguer sur le
fil — le coordinateur ré-hache systématiquement chaque binding reçu via
`PartitionHasher` à chaque round, donc il n'a jamais besoin de savoir
côté fil laquelle des deux catégories Rust a produit un binding donné.
Évite un changement de contrat `graph.proto`.

### graph-observability (câblage réel) — ✅ terminé (Phase 2)

- ✅ **OB1** — `tracing-subscriber` (formatteur JSON, `with_current_span`,
  `with_span_list`) + `EnvFilter` (`RUST_LOG`, défaut `info`) (§9.3).
- ✅ **OB2** — Exporteur OTLP (`opentelemetry-otlp` 0.17 +
  `opentelemetry_sdk` 0.24) dans `init_tracing`. *Décision* : transport
  **HTTP**, pas gRPC — évite d'épingler une seconde version de `tonic`
  dans l'arbre de dépendances (l'exportateur OTLP utiliserait
  autrement `tonic` 0.12 en interne, indépendant du `tonic` 0.11 de
  `graph-proto` ; elles coexistent sans conflit, mais HTTP évite la
  question). Tout collecteur compatible OTLP accepte les deux
  transports. *Décision* : un collecteur injoignable ne bloque jamais le
  démarrage — repli sur logs JSON seuls avec une ligne d'avertissement
  explicite (l'observabilité est une aide opérationnelle, pas une
  dépendance de disponibilité, §9).
- ✅ **OB3** — `inject_trace_context` (client, `bin/graph-coordinator`) /
  `extract_and_continue` (serveur, `bin/graph-partition-node`), tous deux
  de la forme `fn(Request<()>) -> Result<Request<()>, Status>` que
  l'`Interceptor` de `tonic` accepte nativement. Remplace le struct
  `TraceContext` placeholder (supprimé, jamais référencé ailleurs) par
  la propagation réelle du contexte `tracing`/OpenTelemetry via les
  métadonnées gRPC (propagateur `traceparent` W3C par défaut
  d'OpenTelemetry).
- ✅ **OB4** — `GET /metrics` (format texte Prometheus) sur un port dédié
  (`graph_observability::serve_metrics`, convention Prometheus standard
  — port distinct du trafic applicatif) sur les deux binaires. Chaque
  constante de `graph-observability::metrics` est désormais un
  collecteur réel enregistré dans un `Registry` process-wide, avec des
  points d'instrumentation réels câblés à des endroits représentatifs :
  latence/erreurs/hops de requête côté coordinateur
  (`GraphServiceImpl::execute_query`), taille d'index/durée de
  rebuild/âge de snapshot côté partition-node (`rebuild.rs`), latence de
  hop local (`PartitionServiceImpl::expand_hop`). `CROSS_PARTITION_HOP_RATIO`
  reste déclaré (le `/metrics` l'expose dès le démarrage, cohérent avec
  les autres) mais pas encore observé en pratique — instrumentation
  possible en future itération, pas un TBD bloquant.

### bin/graph-coordinator (mode distribué) — ✅ terminé (Phase 2)

- ✅ **CO5** — `remote_executor.rs` (chemin mono-partition dédié)
  supprimé, remplacé par `grpc_partition_rpc::GrpcPartitionRpc`
  (implémente `graph_query::PartitionRpc` contre le client gRPC généré
  de `graph-proto`) piloté par `ScatterGatherExecutor` (Q5-Q7) et
  `graph_cluster::Discovery`. *Décision* : pas de code séparé pour le
  cas mono-partition — `n_partitions: 1` est un cas particulier
  correctement géré par le même exécuteur (une seule partition à
  diffuser/router), validé par le test CO4 qui tourne désormais à
  travers ce chemin. *Décision* : le mode `Discovery` est sélectionné à
  l'exécution (`GRAPH_DISCOVERY_MODE=static|kubernetes`) — `static`
  (adresses fixes `partition_id=host:port`) pour le dev local et les
  tests, `kubernetes` (CL2) pour un vrai cluster — plutôt que de figer
  un seul mécanisme au moment de la compilation. *Gap documenté* :
  `GetIndexStatus` (CO3) relaie la partition de plus petit id comme
  échantillon représentatif plutôt que d'agréger l'état des N
  partitions — le message `IndexStatusResponse` (§8.2) n'a pas été conçu
  pour une vue multi-partitions, et la spec ne définit pas de forme
  agrégée ; documenté ici plutôt que masqué.

---

## Extension DSL hors roadmap — moindre privilège via télémétrie — ✅ terminé

> **Jalon** : le schéma dédié `schema/least_privilege.graphidl` (use
> case "moindre privilège prouvé par la télémétrie" — croiser entités,
> permissions déclarées et logs d'accès observés pour trouver les
> permissions IAM jamais utilisées) documentait à l'origine un **gap** :
> la requête centrale du use case (`déclaré MOINS observé`) n'était pas
> exprimable dans le DSL v1 — pas d'alias d'arête, pas de comparaison
> propriété-propriété inter-alias, pas de `NOT EXISTS`/anti-jointure. Ce
> jalon lève ce gap : la requête centrale tourne désormais telle quelle,
> vérifié bout en bout (`bin/graph-coordinator/tests/lp_end_to_end.rs`,
> LP1) sur deux partitions réelles avec vrai gRPC — pas seulement au
> niveau unitaire. La requête, verbatim :
>
> ```
> MATCH (w:Workload)-[:ASSUMES]->(r:IAMRole)-[g:GRANTS]->(d:DataStore)
> WHERE d.data_class = "sensitive"
>   AND NOT EXISTS {
>     MATCH (w)-[a:ACCESSED]->(d)
>     WHERE a.action = g.action AND a.last_seen >= "2024-05-01T00:00:00Z"
>   }
> RETURN w.workload_id, w.team, r.arn, g.action, d.arn
> ```

### graph-dsl (extension) — ✅ terminé (extension hors roadmap)

- ✅ **D10** — Grammaire `pest` : `edge_alias` (`[g:GRANTS]`, `[a:ACCESSED]`,
  réutilise la position déjà occupée par le type d'arête dans
  `edge_detail`) ; `return_item` étendu pour accepter `alias.propriété`
  en plus d'un alias nu (`RETURN w.team, r.arn` autant que `RETURN w`,
  mélangés dans la même clause) ; `property_property_comparison`
  (`a.action = g.action`, à distinguer de `WhereCondition::Property`
  *(alias.propriété OP littéral)* et de `AliasComparison` *(alias OP
  alias)* — les trois lectures des mêmes tokens auraient été plausibles) ;
  `not_exists_clause` (`AND NOT EXISTS { MATCH ... [WHERE ...] }`),
  imbriqué dans la grammaire `WHERE` existante plutôt qu'une clause à
  part, pour permettre `d.data_class = "sensitive" AND NOT EXISTS {...}`
  dans une seule clause `WHERE`.
- ✅ **D11** — AST : `EdgePattern.alias: Option<String>` ; nouveaux
  `PropertyRef { alias, property }`, `ExistsSubquery { pattern,
  where_conditions }`, `ReturnItem { alias, property: Option<String> }`
  (remplace le `Vec<String>` de `Query.returns` — un alias nu est
  `ReturnItem { property: None, .. }`, pas un type distinct, pour que le
  planificateur/exécuteur n'aient qu'une seule forme à traiter) ;
  `WhereCondition::{PropertyComparison, NotExists}`. *(dépend de D10)*
- ✅ **D12** — `PestParser` : parsing pour toutes les nouvelles règles.
  Décision de scope : un alias d'arête n'est accepté que sur un hop à
  hop fixe (`hops == {1,1}`) — un alias sur `*1..3` désignerait quelle
  arête de la chaîne ? Rejeté à la validation (D13,
  `EdgeAliasOnVariableLengthHop`) plutôt qu'à la grammaire, pour un
  message d'erreur explicite. *(dépend de D11)*
- ✅ **D13** — `SchemaValidator` réécrit autour d'un `Scope` (alias →
  label/type liés par le `MATCH` externe) : `validate_where_condition`
  récursif (couvre le nouveau `NotExists` imbriqué), `validate_not_exists`,
  `validate_return_item`, `validate_property_ref`,
  `validate_edge_property`. Nouvelles règles actées :
  - Un alias référencé dans `NOT EXISTS { MATCH (x)... }` sans
    label/propriétés doit déjà être lié par le `MATCH` externe
    (`NotExistsAnchorNotPreBound`) — c'est ce qui rend la sous-requête
    *corrélée* plutôt qu'un second pattern indépendant ; lui redonner un
    label serait un second nœud sans lien avec l'alias externe, rejeté
    aussi (`a_not_exists_anchor_redeclaring_a_label_fails`).
  - `NOT EXISTS` n'accepte qu'un unique hop **sortant** entre deux alias
    déjà liés (`NotExistsWrongShape`, `NotExistsMustBeOutgoing`) — motivé
    par l'exécution distribuée (voir Q11 : l'enregistrement d'une arête
    vit sur la partition de sa source, un hop entrant obligerait à
    interroger la partition de la destination pour une info qui n'y est
    pas).
  - Pas de `NOT EXISTS` imbriqué (`NestedNotExists`) — hors scope, le
    use case n'en a jamais eu besoin et la corrélation à plus d'un
    niveau complexifierait Q11 sans bénéfice démontré.
  Testé directement sur la requête centrale du use case, chargée depuis
  le vrai fichier schéma via `include_str!`
  (`the_least_privilege_example_validates`) plutôt que sur un exemple
  jouet — pour que ce test casse si le schéma et le validateur divergent.
  *(dépend de D12)*

*Décision transverse (D10-D13)* : aucune fonction `datetime()`/
`duration()` n'a été ajoutée au DSL — `a.last_seen >= "2024-05-01T…Z"`
compare un littéral `String` RFC3339 à une propriété `Timestamp`
directement. La coercion (RFC3339 → microsecondes) est faite côté
exécution (voir Q9/`filter_eval.rs`), pas dans l'AST : ajouter un
langage d'expressions dédié aux dates aurait été disproportionné pour un
seul point d'usage, et le DSL reste sans fonctions au sens large (§7.1).

### graph-index (extension) — ✅ terminé (extension hors roadmap)

- ✅ **IX9** — `EdgeRecord { edge_type, properties }` et
  `IndexGeneration.edge_records: HashMap<EdgeId, EdgeRecord>` — jusqu'ici
  seules les propriétés des *nœuds* étaient conservées après
  construction d'index (`node_records`), les arêtes n'existaient que
  comme entrées CSR sans propriétés attachées. Nécessaire pour évaluer
  `a.action = g.action` (propriétés d'arête) et projeter
  `RETURN g.action`.
- ✅ **IX10** — `IcebergIndexBuilder::build` peuple `edge_records`,
  filtré par `partition_of(row.src, n_partitions) == partition`.
  *Décision* : l'enregistrement d'une arête est possédé par la partition
  de sa **source**, exactement le même critère que `build_csr` utilise
  déjà pour décider si une arête a une entrée d'adjacence sortante
  locale — cohérence délibérée plutôt qu'une seconde règle de
  répartition à mémoriser séparément (voir aussi la note dans le
  doc-comment de `builder.rs`). Conséquence directe pour Q11 : une
  requête `GetEdgeProperties`/anti-jointure pour une `EdgeId` donnée
  n'a jamais besoin d'atteindre plus d'une partition, la même qui a émis
  cette arête via `ExpandHop`/`ResolveStart` en premier lieu. Testé
  (`edge_records_hold_the_full_property_set_per_locally_owned_edge`,
  `edge_records_follow_the_source_nodes_partition`). *(dépend de IX9)*

### graph-proto (extension) — ✅ terminé (extension hors roadmap)

- ✅ **PROTO1** — `.proto` étendu sans casser le contrat existant :
  `ExpandHopRequest.edge_alias` (optionnel — absent si l'appelant ne
  demande pas à lier l'arête), `Binding.edge_ids_by_alias` (le pendant
  arête de la map nœud existante), `ResolveAll`/`ResolveAllRequest`
  (scan complet d'un label sans filtre — nécessaire pour
  `MATCH (w:Workload)` sans `{...}`, jusqu'ici toute requête supposait un
  filtre de départ), `CheckAntiJoin`/`AntiJoinRequest`/
  `AntiJoinResponse` (l'anti-jointure côté partition — §description
  Q11), `GetEdgeProperties` (symétrique de `GetNodeProperties`, Phase 1),
  `QueryResult.edge_properties` et projection étendue pour porter
  `"alias.propriété"` comme clé plate plutôt que d'ajouter un message
  dédié par forme de projection (voir Q12).

### graph-query (extension) — ✅ terminé (extension hors roadmap)

- ✅ **Q9** — `filter_eval.rs` (nouveau, factorisé hors de
  `local_executor.rs`/`distributed_executor.rs` — la même évaluation de
  filtre sert désormais aux deux) : coercion RFC3339 → microsecondes
  spécifiquement pour la paire `(PropertyValue::Timestamp,
  Literal::String)`, via `chrono`. *Gap détecté et corrigé en cours de
  route* : sans cette coercion, `a.last_seen >= "2024-05-01T…Z"` aurait
  silencieusement ne jamais matché (comparaison d'un `i64` en
  microsecondes à une chaîne de caractères).
- ✅ **Q10** — `Binding` : `type Binding = HashMap<String, NodeId>`
  (Phase 1/2) devient une struct `{ nodes: HashMap<String, NodeId>,
  edges: HashMap<String, EdgeId> }` — une seule ligne de résultat lie
  maintenant des alias de nœuds *et* d'arêtes. `PlanStep::ResolveAll`
  (scan sans filtre), `AntiJoinStep { anchor_alias, edge_alias,
  target_alias, edge_type, direction, edge_conditions,
  outer_conditions }`, `ExpandHop.edge_alias`. *Décision multigraphe* :
  la première implémentation dédoublonnait les bindings d'un `ExpandHop`
  par `(binding_idx, dst)`, ce qui fusionnait silencieusement deux
  arêtes parallèles entre les mêmes nœuds en un seul binding — perdant
  l'alias d'une des deux arêtes. Corrigé : la branche avec alias d'arête
  n'applique aucun dédoublonnage (chaque entrée d'adjacence est déjà
  distincte par construction), seule la branche sans alias (qui ne peut
  pas observer la différence) continue de dédoublonner.
- ✅ **Q11** — `LocalExecutor::{resolve_all, check_anti_join}` et
  `planner::compile_anti_join`. *Décision* : `NOT EXISTS` compile en un
  `PlanStep::AntiJoin` séparé, ajouté après la chaîne principale plutôt
  qu'imbriqué dans le plan — un anti-join filtre des bindings déjà
  résolus, il n'a pas besoin d'être entrelacé avec `ResolveStart`/
  `ExpandHop`.
- ✅ **Q12** — `ScatterGatherExecutor::execute_anti_join` : route
  `PlanStep::AntiJoin` en RPC `CheckAntiJoin`. *Décision (la plus
  significative de cette phase)* : les `outer_conditions` d'un
  `AntiJoinStep` (ex. `a.action = g.action`) référencent une propriété
  d'un alias *externe* dont la valeur varie par binding (le `g` lié par
  cette ligne précise) — impossible à résoudre une seule fois pour toute
  la frontière comme le fait `WHERE` après un `ExpandHop` classique.
  Résolu par `resolve_and_group_by_condition` : hydrate, pour chaque
  binding, les propriétés nœud/arête nécessaires (`GetNodeProperties`/
  `GetEdgeProperties`, routées vers la bonne partition via IX10), puis
  regroupe les bindings qui partagent la même liste de conditions
  *déjà résolues* avant d'émettre une requête `CheckAntiJoin` — un appel
  par `(partition, groupe de conditions)`, pas un appel par binding.
  `RETURN alias.propriété` projeté sur le fil sans faire exploser le
  schéma du message : réutilise le `projection: map<string, Value>`
  existant avec une clé composite `"alias.propriété"`, les alias nus
  continuant d'utiliser les maps `properties`/`edge_properties`
  existantes pour l'enregistrement complet.

### bin/graph-partition-node, bin/graph-coordinator (extension) — ✅ terminé (extension hors roadmap)

- ✅ **PN6/CO6** — `PartitionServiceImpl`/`GraphServiceImpl` réécrits pour
  porter tout ce qui précède sur le fil : `ResolveAllStream`,
  `resolve_all`/`check_anti_join`/`get_edge_properties`, `expand_hop`
  câblé pour `edge_alias`, `binding_to_proto`/`proto_binding_to_binding`
  étendus pour la map d'arêtes. Côté coordinateur, la projection finale
  distingue par binding si un alias est un alias de nœud ou d'arête
  (`alias_is_edge`, résolu en observant simplement dans quelle map du
  premier binding disponible l'alias apparaît) avant de choisir entre
  projection d'enregistrement complet (alias nu) et projection scalaire
  (`alias.propriété`).
- ✅ **LP1** — Test d'intégration bout en bout dédié
  (`bin/graph-coordinator/tests/lp_end_to_end.rs`), même forme que CO4
  mais sur `schema/least_privilege.graphidl`, **deux** partitions réelles
  (pas une seule comme CO4) pour que `CheckAntiJoin` franchisse
  effectivement une frontière de partition, et la requête centrale du
  use case verbatim. Jeu de données : un rôle/un datastore sensible
  partagés par quatre workloads répartis sur les deux partitions (vérifié
  par `assert!` plutôt que supposé) — un seul a utilisé le droit accordé
  dans la fenêtre de confiance, les trois autres (jamais accédé, action
  différente, accès hors fenêtre) sont les trois permissions non
  utilisées attendues. Vérifie aussi que `RETURN
  w.workload_id, w.team, r.arn, g.action, d.arn` projette correctement un
  mélange d'alias de nœud et d'arête avec propriété.

---

## Vue d'ensemble des dépendances inter-crates

```
S (schema)  →  D (dsl), ST (storage)
ST          →  IX (index)
IX, D       →  Q (query, local: Q1-Q4)
Q(local), ST, IX  →  PN (bin partition-node)
Q(local), PN, D   →  CO (bin coordinator, mono-partition) — jalon MVP
CL (cluster)      →  Q (distribué: Q5-Q8), CO (mode distribué: CO5)
```
