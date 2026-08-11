# Architecture — Knowledge Graph Engine (Rust)

Ce document traduit `docs/graph-engine-spec.md` en une architecture de code
concrète : découpage en crates, dépendances entre elles, et flux d'une
requête de bout en bout. Toute décision citée ici (`§X.Y`) renvoie à la
spec — ce document ne re-justifie pas les choix, il les fait correspondre
à des unités de code.

Statut : Phase 1 de la roadmap (spec §12) implémentée et testée de bout
en bout — schéma, DSL, stockage Iceberg, index, exécution mono-partition,
et les deux binaires réseau (`graph-coordinator`/`graph-partition-node`)
communiquant en vrai gRPC (voir §5 plus bas). Reste : distribution
multi-partitions et un catalogue Iceberg persistant pour un déploiement
multi-process réel (Phase 2 — voir la limite connue documentée dans
`IMPLEMENTATION.md`, section `bin/graph-partition-node`).

## 1. Vue d'ensemble du workspace

```
graph-engine/
├── crates/
│   ├── graph-schema/         IDL, types de domaine, versioning (§3)
│   ├── graph-storage/        lecture Iceberg seule (§4)
│   ├── graph-index/          index CSR + B-Tree, rebuild, swap atomique (§5)
│   ├── graph-dsl/            AST, parser, validation statique (§7.1, §7.2)
│   ├── graph-query/          planification + exécution locale/distribuée (§7.3, §7.4)
│   ├── graph-proto/          définitions gRPC/Protobuf (§8)
│   ├── graph-cluster/        partitionnement, discovery, réplication (§6)
│   └── graph-observability/  métriques + tracing partagés (§9)
└── bin/
    ├── graph-coordinator/    rôle coordinateur (§6.1)
    └── graph-partition-node/ rôle nœud de partition (§6.1)
```

Chaque crate de `crates/` est une librairie sans état de processus ; les
deux binaires de `bin/` sont les seuls points d'entrée réseau, et
correspondent exactement aux deux rôles du cluster définis en §6.1.

## 2. Graphe de dépendances

```
graph-schema  (aucune dépendance interne — fondation)
     ▲
     ├── graph-storage
     ├── graph-dsl
     └── graph-cluster
              ▲
graph-storage ┤
     ▲        │
     └── graph-index
              ▲
     ┌────────┴────────┐
graph-dsl          graph-cluster
     ▲                  ▲
     └──── graph-query ─┘
              ▲
     ┌────────┴────────┐
graph-proto        graph-observability
     ▲                  ▲
     └── graph-coordinator / graph-partition-node ──┘
```

Règle suivie : les crates du bas (`graph-schema`, `graph-storage`) ne
savent rien des crates du haut. `graph-index` ne connaît ni le DSL ni le
réseau. `graph-query` orchestre `graph-index` mais ignore gRPC. Le
découplage correspond à ce qui doit pouvoir être testé indépendamment (ex:
tester le planificateur de requêtes sans serveur gRPC qui tourne).

## 3. Correspondance crate ↔ section du spec

| Crate | Section(s) spec | Rôle |
|---|---|---|
| `graph-schema` | §3 | Modèle de données, IDL, évolution de schéma |
| `graph-storage` | §4 | Lecture Iceberg seule, résolution de snapshot |
| `graph-index` | §5 | Index CSR (§5.1) + B-Tree (§5.2), génération immuable + swap atomique (§5.3) |
| `graph-dsl` | §7.1, §7.2 | AST, parsing, validation statique contre le schéma |
| `graph-query` | §7.3, §7.4 | Planification, exécution locale (mono-partition) et distribuée (scatter-gather) |
| `graph-proto` | §8 | Service gRPC client (`GraphService`) + service interne (`PartitionService`) |
| `graph-cluster` | §6 | Hash-partitioning, sur-partitionnement fixe, discovery K8s, réplication sans consensus |
| `graph-observability` | §9 | Noms de métriques Prometheus, propagation de trace OpenTelemetry |
| `graph-coordinator` (bin) | §6.1 | Process coordinateur — Kubernetes `Deployment` (§10) |
| `graph-partition-node` (bin) | §6.1 | Process nœud de partition — Kubernetes `StatefulSet` (§10) |

## 4. Flux d'une requête (k-hop filtré)

Exemple : `MATCH (p:Person {name:"Alice"})-[:KNOWS*1..3]->(friend:Person) WHERE friend.birth_year > 1990 RETURN friend`

1. Le client envoie `ExecuteQuery` au **coordinateur** via gRPC (`graph-proto::GraphService`).
2. Le coordinateur parse le DSL (`graph-dsl::Parser`) puis valide contre le schéma actif (`graph-dsl::Validator`, §7.2 — fail-fast, erreurs retournées avant toute exécution).
3. `graph-query::Planner` transforme la requête validée en `QueryPlan` : un `ResolveStart` (résolution d'Alice via l'index de propriété) suivi de `ExpandHop` répétés (§7.3).
4. `graph-query::DistributedExecutor` exécute le plan en scatter-gather (§7.4) :
   - Résout la partition d'Alice via `graph-cluster::PartitionHasher`.
   - Envoie `ResolveStart` à une réplique saine de cette partition (`graph-cluster::PartitionMap::healthy_replicas`) via `PartitionService`.
   - À chaque hop, envoie la frontière courante à toutes les partitions concernées (`ExpandHop`), y compris via des `RemoteRef` quand un voisin est hors partition.
5. Chaque **nœud de partition** exécute son bout localement (`graph-query::LocalExecutor` contre son `graph_index::GenerationHandle::acquire()` — la génération d'index actuellement servie, §5.3).
6. Le coordinateur ré-agrège (déduplication, pas de réduction — §7.1) et streame les résultats projetés au client au fur et à mesure (§7.5).

En parallèle, sur chaque nœud de partition, `rebuild::periodic_rebuild_loop`
tourne en tâche de fond (§5.3) : reconstruit une nouvelle génération
d'index depuis un snapshot Iceberg épinglé, puis `GenerationHandle::swap`
la publie atomiquement — sans jamais interrompre les requêtes en cours sur
l'ancienne génération.

## 5. Ce qui est fait vs. ce qui reste (Phase 0/1, spec §12)

Fait — le détail crate par crate est dans `IMPLEMENTATION.md` et l'état
tâche par tâche dans `TASKS.md` (tout ce qui y est coché `✅`) :
- Modèle de données, IDL et son parser, évolution de schéma
  (`graph-schema`, S1-S7).
- DSL complet (grammaire, parser, validateur statique) sur les deux
  formes de requête prioritaires du spec §7.1 (`graph-dsl`, D1-D9).
- Intégration Apache Iceberg réelle (`apache/iceberg-rust`), lecture
  seule, catalogue mémoire + FileIO local pour le dev (`graph-storage`,
  ST1-ST6).
- Index CSR + B-Tree construits depuis Iceberg, `GenerationHandle` avec
  swap atomique testé sous accès concurrent (`graph-index`, IX1-IX8).
- Planificateur naïf + exécuteur local mono-partition, bout en bout sur
  la requête k-hop exemple du spec (`graph-query`, Q1-Q4).
- Un exécutable de démonstration (`examples/demo`) qui enchaîne tout ce
  qui précède in-process : ingère un petit graphe dans un warehouse
  Iceberg local, construit l'index, puis exécute la requête k-hop exemple
  du spec §7.1 de bout en bout. Lancer avec `cargo run -p
  graph-engine-demo` depuis `graph-engine/`.
- `bin/graph-partition-node` et `bin/graph-coordinator` (PN1-PN5,
  CO1-CO4) : les deux process réseau qui exposent ce qui précède via
  `graph-proto`, communiquant en vrai gRPC sur de vrais sockets TCP —
  vérifié par un test d'intégration qui lance les deux et exécute la
  requête k-hop de bout en bout à travers eux (jalon MVP mono-partition,
  spec §12 Phase 1).

⚠️ **Limite connue avant tout déploiement multi-process réel** : le
catalogue Iceberg dev (`MemoryCatalog`, décision ST1) a un registre de
tables en mémoire, propre à chaque processus — un `graph-partition-node`
lancé séparément d'un job d'ingestion ne verra pas ses tables, même si
les fichiers Parquet existent sur disque (vérifié empiriquement). Un
déploiement réel (VM, cluster) demande de basculer vers un catalogue
persistant (candidat déjà identifié en ST1 : `iceberg-catalog-sql` +
SQLite, ou un catalogue REST) — détaillé dans `IMPLEMENTATION.md`.

Reste à faire (Phase 2) :
- Distribution multi-partitions (`graph-cluster`, `DistributedExecutor`,
  `CO5`).
- Câblage observabilité réel (`graph-observability`).
- Le seul TBD encore ouvert côté spec (§13) : processus de migration de
  schéma incompatible — sans impact sur cette architecture, à traiter au
  moment venu.
