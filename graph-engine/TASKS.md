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

### graph-schema

- **S1** — Écrire la grammaire `pest` de l'IDL (syntaxe `schema`, `node`,
  `edge`, types scalaires §3.2) dans un fichier `.pest`.
- **S2** — Implémenter `SchemaParser::parse` : IDL → `Schema` en utilisant
  S1. *(dépend de S1)*
- **S3** — Tests : parser un schéma valide multi-nœuds/arêtes (l'exemple
  §3.4) et vérifier le `Schema` produit. *(dépend de S2)*
- **S4** — Tests : cas d'erreur (label dupliqué, type de relation avec
  label source/cible inconnu, propriété sans type) → `SchemaError::Parse`.
  *(dépend de S2)*
- **S5** — Implémenter `SchemaEvolution::diff` : cas compatible (ajout de
  propriété optionnelle, ajout de label/type de relation).
- **S6** — Implémenter `SchemaEvolution::diff` : cas incompatible
  (suppression de propriété, renommage, changement de type).
  *(dépend de S5)*
- **S7** — Tests `SchemaEvolution::diff` pour chaque cas de S5/S6.
  *(dépend de S6)*

### graph-dsl

- **D1** — Écrire la grammaire `pest` du DSL : `MATCH` avec motif
  nœud/arête, hop range (`*1..3`), `WHERE`, `RETURN` (§7.1).
- **D2** — Implémenter `Parser::parse` pour un `NodePattern` seul (ex:
  `(p:Person {name: "Alice"})`). *(dépend de D1)*
- **D3** — Implémenter `Parser::parse` pour une chaîne `Pattern` complète
  (nœuds + arêtes typées + hop range). *(dépend de D2)*
- **D4** — Implémenter `Parser::parse` pour la clause `WHERE`
  (`PropertyFilter` + `ComparisonOp`). *(dépend de D3)*
- **D5** — Tests parser : les deux exemples du spec §7.1 (k-hop filtré,
  pattern matching) donnent l'AST attendu. *(dépend de D4)*
- **D6** — Tests parser : cas d'erreur de syntaxe → `DslError::Syntax`.
  *(dépend de D4)*
- **D7** — Implémenter `Validator::validate` : existence des labels/types
  de relation contre le `Schema`. *(dépend de S2)*
- **D8** — Implémenter `Validator::validate` : compatibilité type de
  propriété ↔ `ComparisonOp` utilisé dans `WHERE`. *(dépend de D7)*
- **D9** — Tests validateur : requête valide passe, requête avec label
  inconnu / opérateur incompatible échoue avec le bon `ValidationError`.
  *(dépend de D8)*

### graph-storage

- **ST1** — Choisir et intégrer une crate Iceberg Rust (`apache/iceberg-rust`
  ou équivalent) ; définir la config de catalogue (local/filesystem pour
  dev, candidat prod à trancher séparément).
- **ST2** — Implémenter `latest_snapshot` pour une table donnée.
  *(dépend de ST1)*
- **ST3** — Implémenter la désérialisation ligne Parquet → `PropertyValue`
  pour chaque variante de `ScalarType` (§3.2). *(dépend de ST1)*
- **ST4** — Implémenter `scan_nodes` (filtré par `label`) en s'appuyant sur
  ST2/ST3. *(dépend de ST2, ST3)*
- **ST5** — Implémenter `scan_edges` (filtré par `edge_type`).
  *(dépend de ST2, ST3)*
- **ST6** — Test d'intégration : écrire une table Iceberg de test (fixture
  minimale), vérifier `scan_nodes`/`scan_edges` retournent les lignes
  attendues. *(dépend de ST4, ST5)*

---

## Phase 1 — MVP mono-partition

### graph-index

- **IX1** — Implémenter la construction du tableau CSR sortant (tri des
  arêtes par `src_node_id`, calcul des offsets) dans `IndexBuilder::build`.
  *(dépend de ST4, ST5)*
- **IX2** — Implémenter la construction du tableau CSR entrant (même
  logique, trié par `dst_node_id`). *(dépend de IX1)*
- **IX3** — Détection de référence distante (`RemoteRef`) : un voisin dont
  le `node_id` hache vers une autre partition. *(dépend de IX2 ; nécessite
  `PartitionHasher`, cf. CL1 — no-op en Phase 1 mono-partition où tout est
  local)*
- **IX4** — Implémenter la construction de `PropertyIndex` (une seule
  propriété indexée pour commencer) à partir des lignes scannées.
  *(dépend de ST4)*
- **IX5** — Ajouter le lookup par plage (`range`) sur `PropertyIndex`, en
  plus de `lookup_eq` déjà présent. *(dépend de IX4)*
- **IX6** — Tests `TopologicalIndex` : voisins sortants/entrants corrects
  sur un petit graphe synthétique (5-10 nœuds). *(dépend de IX2)*
- **IX7** — Tests `PropertyIndex` : égalité et plage retournent les bons
  `NodeId`. *(dépend de IX5)*
- **IX8** — Test `GenerationHandle` : une requête qui a acquis une
  génération continue de la voir après un `swap()` concurrent (pas
  d'incohérence, pas de panic). *(dépend de IX1..IX5)*

### graph-query (exécution locale)

- **Q1** — Implémenter `Planner::plan` : lowering naïf d'un `Pattern` en
  séquence `ResolveStart` + `ExpandHop*` dans l'ordre du motif.
  *(dépend de D5)*
- **Q2** — Implémenter `LocalExecutor::resolve_start` (délègue à
  `PropertyIndex::lookup_eq`/range). *(dépend de IX5, Q1)*
- **Q3** — Implémenter `LocalExecutor::expand_hop` (délègue à
  `TopologicalIndex::out_neighbors`/`in_neighbors`, applique les filtres
  `WHERE` au plus tôt). *(dépend de IX2, Q1)*
- **Q4** — Test bout en bout local : plan + exécute la requête k-hop
  d'exemple (§7.1) contre une `IndexGeneration` construite en mémoire pour
  le test, vérifie les `Binding` obtenus. *(dépend de Q2, Q3)*

### bin/graph-partition-node

- **PN1** — Implémenter `rebuild::bootstrap` : construit `IcebergReader` +
  `IndexBuilder` concrets, premier build synchrone. *(dépend de ST1, IX1)*
- **PN2** — Implémenter le corps de `periodic_rebuild_loop` (appelle
  `IndexBuilder::build`, puis `GenerationHandle::swap` si succès, logge et
  garde l'ancienne génération si échec). *(dépend de PN1)*
- **PN3** — Implémenter `PartitionServiceImpl::resolve_start` (délègue à
  `LocalExecutor::resolve_start`). *(dépend de Q2)*
- **PN4** — Implémenter `PartitionServiceImpl::expand_hop` (délègue à
  `LocalExecutor::expand_hop`). *(dépend de Q3)*
- **PN5** — Config du binaire : lecture de `n_partitions`/`partition_id`/
  intervalle de rebuild depuis l'environnement.

### bin/graph-coordinator (mode mono-partition)

- **CO1** — Implémenter `GraphServiceImpl::get_schema` (retourne le
  `Schema` actif). *(dépend de S2)*
- **CO2** — Implémenter `GraphServiceImpl::execute_query` en mode
  mono-partition : parse (D-crate) → valide (D7-D9) → plan (Q1) → exécute
  en appelant directement le `graph-partition-node` local (pas de
  scatter-gather multi-partitions à ce stade) → streame les résultats
  projetés. *(dépend de D9, Q4, PN3, PN4)*
- **CO3** — Implémenter `GraphServiceImpl::get_index_status` (relaie le
  `GenerationMeta` du nœud de partition). *(dépend de PN1)*
- **CO4** — Test d'intégration MVP : lancer `graph-partition-node` +
  `graph-coordinator` en mono-partition, exécuter une requête k-hop de
  bout en bout via le client gRPC généré. *(dépend de CO2)*

> **Jalon** : CO4 qui passe = MVP mono-partition complet (spec §12 Phase
> 1) — modèle de données et DSL validés de bout en bout avant d'attaquer
> la distribution.

---

## Phase 2 — Distribution

### graph-cluster

- **CL1** — Choisir une fonction de hash stable (ex: xxhash) et l'intégrer
  dans `PartitionHasher` (remplace le placeholder modulo actuel).
- **CL2** — Implémenter `Discovery` pour Kubernetes (crate `kube`,
  Pods/Endpoints ou headless Service, §6.3). *(dépend de CL1)*
- **CL3** — Implémenter `RebalancePlanner` (stratégie de base : répartition
  égale des partitions logiques sur les machines disponibles).
  *(dépend de CL2)*
- **CL4** — Tests `PartitionMap::healthy_replicas` : filtre bien les
  répliques marquées non-saines.

### graph-query (exécution distribuée)

- **Q5** — Implémenter `DistributedExecutor::execute` : résoudre la
  partition de départ (CL1), envoyer `ResolveStart` via `graph-proto` à
  une réplique saine (via `graph-cluster::PartitionMap`). *(dépend de CL1,
  Q2, graph-proto client)*
- **Q6** — Étendre `DistributedExecutor` : boucle scatter-gather par hop —
  fan-out `ExpandHop` vers toutes les partitions touchées par la frontière
  courante, y compris via les `RemoteRef` (IX3). *(dépend de Q5, IX3)*
- **Q7** — Déduplication de frontière lors de la fusion des réponses
  (pas de réduction, §7.1 — juste éliminer les doublons de binding).
  *(dépend de Q6)*
- **Q8** — Test bout en bout distribué : 2 partitions, une requête dont le
  hop 2 traverse une frontière de partition, vérifie le résultat final.
  *(dépend de Q7)*

### graph-observability (câblage réel)

- **OB1** — Wiring `tracing-subscriber` + formatteur JSON (§9.3).
- **OB2** — Wiring exporteur OpenTelemetry (OTLP) dans `init_tracing`
  (§9.2). *(dépend de OB1)*
- **OB3** — Interceptor `tonic` de propagation de `TraceContext` sur les
  appels coordinateur → partition. *(dépend de OB2, Q6)*
- **OB4** — Endpoint `/metrics` (Prometheus) sur les deux binaires, à
  partir des constantes déjà définies dans `graph-observability::metrics`.

### bin/graph-coordinator (mode distribué)

- **CO5** — Remplacer l'appel direct de CO2 par `DistributedExecutor`
  (Q5-Q7) + `graph-cluster::Discovery` (CL2) pour router réellement à
  travers plusieurs nœuds de partition. *(dépend de Q7, CL2)*

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
