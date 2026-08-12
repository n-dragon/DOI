# Architecture — Knowledge Graph Engine (Rust)

Ce document traduit `docs/graph-engine-spec.md` en une architecture de code
concrète : découpage en crates, dépendances entre elles, et flux d'une
requête de bout en bout. Toute décision citée ici (`§X.Y`) renvoie à la
spec — ce document ne re-justifie pas les choix, il les fait correspondre
à des unités de code.

Statut : Phase 1 **et** Phase 2 de la roadmap (spec §12) implémentées et
testées — schéma, DSL, stockage Iceberg (catalogue persistant), index
CSR + propriété partition-aware, exécution mono- **et** multi-partition
(scatter-gather réel avec franchissement de frontière de partition),
discovery Kubernetes, rebalancement, observabilité câblée (traces
OpenTelemetry propagées à travers le cluster, métriques Prometheus). Les
deux binaires réseau (`graph-coordinator`/`graph-partition-node`)
communiquent en vrai gRPC entre process séparés (voir §5 plus bas), y
compris à travers plusieurs partitions. Le DSL a depuis été étendu
au-delà des deux formes prioritaires du spec §7.1 — hors roadmap
d'origine (§12 ne prévoit que Phase 0-3, celle-ci n'y figure pas ;
voir §6 plus bas) : alias d'arête, comparaison propriété-propriété
inter-alias, `NOT EXISTS`/anti-jointure corrélée, pour rendre
exprimable en une seule requête un use case concret de plateforme de
données référentielle (moindre privilège prouvé par la télémétrie —
`schema/least_privilege.graphidl`). Détail par tâche : `TASKS.md`
(Phase 2 et l'extension hors roadmap, toutes tâches cochées ✅, avec
les décisions prises pendant l'implémentation documentées en ligne).

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
deux binaires ci-dessus sont les seuls points d'entrée réseau du
*cluster* et correspondent exactement aux deux rôles définis en §6.1.
`bin/graph-viewer-server` (§5 ci-dessous) est un troisième binaire, hors
cluster : un client de `graph-coordinator` comme un autre, pas un rôle
du spec.

## 2. Graphe de dépendances

```
graph-schema  (aucune dépendance interne — fondation ; héberge
               `partitioning` : hash(node_id) % n_partitions, §6.2 —
               voir §2.1 ci-dessous pour pourquoi c'est ici)
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
     └──── graph-query ─┘  (dépend aussi de graph-storage,
              ▲              pour NodeRecord/PropertyValue —
     ┌────────┴────────┐     GetNodeProperties, §5.2)
graph-proto        graph-observability
     ▲                  ▲
     └── graph-coordinator / graph-partition-node ──┘
```

Règle suivie : les crates du bas (`graph-schema`, `graph-storage`) ne
savent rien des crates du haut. `graph-index` ne connaît ni le DSL ni le
réseau. `graph-query` orchestre `graph-index`/`graph-cluster` mais
ignore gRPC — même son exécuteur distribué (`ScatterGatherExecutor`,
Phase 2) parle à un trait `PartitionRpc` qu'il définit lui-même, pas à
`graph-proto`/`tonic` directement (voir `graph-query/src/
distributed_executor.rs`). Le découplage correspond à ce qui doit
pouvoir être testé indépendamment (ex: tester le scatter-gather avec un
`PartitionRpc` en mémoire, sans serveur gRPC qui tourne — c'est
exactement le test Q8).

### 2.1. Pourquoi `graph_schema::partitioning` et pas `graph-cluster`

La formule `hash(node_id) % n_partitions` (§6.2) a été introduite dans
`graph-cluster` (task CL1) mais déplacée dans `graph-schema` dès qu'un
second point d'appel en a eu besoin : `graph-index`'s builder (task IX3,
révisée en Phase 2) doit savoir, pendant la construction d'une
génération d'index, si le voisin d'une arête appartient à la partition
en cours de construction ou à une autre (pour émettre un `RemoteRef`).
`graph-index` ne peut pas dépendre de `graph-cluster` — `graph-cluster`
dépend déjà de `graph-index` (`PartitionId`, `ReplicaEndpoint`) — donc la
formule ne pouvait pas rester dans `graph-cluster` sans dupliquer sa
logique dans `graph-index` (exactement le risque de drift que le
commentaire d'origine de `PartitionHasher` voulait éviter). `graph-schema`
est la seule crate en amont des deux, d'où son nouveau rôle de foyer
canonique pour cette formule ; `graph-cluster::PartitionHasher` et
`graph-cluster::hash` restent le point d'entrée documenté (« pourquoi
xxh3 ») mais délèguent l'implémentation.

## 3. Correspondance crate ↔ section du spec

| Crate | Section(s) spec | Rôle |
|---|---|---|
| `graph-schema` | §3 | Modèle de données, IDL, évolution de schéma |
| `graph-storage` | §4 | Lecture Iceberg seule, résolution de snapshot |
| `graph-index` | §5 | Index CSR (§5.1) + B-Tree (§5.2), génération immuable + swap atomique (§5.3) |
| `graph-dsl` | §7.1, §7.2 | AST, parsing, validation statique contre le schéma |
| `graph-query` | §7.3, §7.4 | Planification, exécution locale (mono-partition) et distribuée (scatter-gather, `ScatterGatherExecutor`) |
| `graph-proto` | §8 | Service gRPC client (`GraphService`) + service interne (`PartitionService`) |
| `graph-cluster` | §6 | Hash-partitioning (délègue à `graph-schema::partitioning`), sur-partitionnement fixe, discovery K8s (`kube::Api<Pod>`), rebalancement (hachage de rendez-vous), réplication sans consensus |
| `graph-observability` | §9 | Métriques Prometheus (`/metrics` réel, collecteurs enregistrés), tracing OpenTelemetry (OTLP/HTTP) + propagation de contexte via interceptors `tonic` |
| `graph-coordinator` (bin) | §6.1 | Process coordinateur — Kubernetes `Deployment` (§10) |
| `graph-partition-node` (bin) | §6.1 | Process nœud de partition — Kubernetes `StatefulSet` (§10) |

## 4. Flux d'une requête (k-hop filtré)

Exemple : `MATCH (p:Person {name:"Alice"})-[:KNOWS*1..3]->(friend:Person) WHERE friend.birth_year > 1990 RETURN friend`

1. Le client envoie `ExecuteQuery` au **coordinateur** via gRPC (`graph-proto::GraphService`).
2. Le coordinateur parse le DSL (`graph-dsl::Parser`) puis valide contre le schéma actif (`graph-dsl::Validator`, §7.2 — fail-fast, erreurs retournées avant toute exécution).
3. `graph-query::Planner` transforme la requête validée en `QueryPlan` : un `ResolveStart` (résolution d'Alice via l'index de propriété) suivi de `ExpandHop` répétés (§7.3).
4. Le coordinateur interroge `graph_cluster::Discovery` (Kubernetes ou statique selon `GRAPH_DISCOVERY_MODE`, cf. §6.3) pour obtenir la `PartitionMap` courante, puis `graph-query::ScatterGatherExecutor` exécute le plan en scatter-gather (§7.4) :
   - `ResolveStart` : diffusé à **toutes** les partitions (l'index de propriété est local à chaque partition, §5.2 — pas d'index global v1) via `PartitionService::ResolveStart`, résultats fusionnés (dédoublonnage, disjonction garantie par construction).
   - Chaque `ExpandHop` (y compris `*1..3`) est décomposé en autant de rounds réseau qu'il y a de hops max — à chaque round, la frontière courante est groupée par partition propriétaire (`graph-cluster::PartitionHasher::partition_of`, appliqué au `node_id` déjà résolu de ce round) et envoyée en `ExpandHop` à une réplique saine (`PartitionMap::healthy_replicas`) de chaque partition concernée. Un voisin `RemoteRef` (arête cross-partition, §5.1) rentre dans le round suivant sans être perdu.
   - `WHERE` est évalué une fois par étape `ExpandHop`, après le dernier round, via `GetNodeProperties` (hydratation + comparaison côté coordinateur, cf. `TASKS.md` Q6).
5. Chaque **nœud de partition** exécute son bout localement (`graph-query::LocalExecutor` contre son `graph_index::GenerationHandle::acquire()` — la génération d'index actuellement servie, §5.3), en connaissant désormais ses propres frontières de partition (IX3 révisée : la construction d'index filtre aux nœuds possédés et calcule les `RemoteRef` réels).
6. Le coordinateur ré-agrège (déduplication, pas de réduction — §7.1) et streame les résultats projetés au client au fur et à mesure (§7.5).

Un query mono-partition (`n_partitions: 1`, ou `GRAPH_DISCOVERY_MODE=static`
avec une seule partition configurée) suit exactement le même chemin —
`ScatterGatherExecutor` n'a pas de branche séparée pour ce cas, il se
contente d'avoir une seule partition à diffuser/router (voir CO5,
`TASKS.md`).

En parallèle, sur chaque nœud de partition, `rebuild::periodic_rebuild_loop`
tourne en tâche de fond (§5.3) : reconstruit une nouvelle génération
d'index depuis un snapshot Iceberg épinglé, puis `GenerationHandle::swap`
la publie atomiquement — sans jamais interrompre les requêtes en cours sur
l'ancienne génération.

## 5. Ce qui est fait vs. ce qui reste (Phase 0/1/2, spec §12)

Fait — le détail crate par crate est dans `IMPLEMENTATION.md` et l'état
tâche par tâche dans `TASKS.md` (tout ce qui y est coché `✅`) :
- Modèle de données, IDL et son parser, évolution de schéma
  (`graph-schema`, S1-S7).
- DSL complet (grammaire, parser, validateur statique) sur les deux
  formes de requête prioritaires du spec §7.1 (`graph-dsl`, D1-D9).
- Intégration Apache Iceberg réelle (`apache/iceberg-rust`), lecture
  seule, catalogue SQLite persistant + FileIO local pour le dev
  (`graph-storage`, ST1-ST6).
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
- Déploiement multi-process réel, vérifié pour de vrai (pas juste en
  théorie) : `examples/ingest-cloud-cost` (ingestion),
  `graph-partition-node`, `graph-coordinator`, `bin/graph-viewer-server`
  (pont HTTP↔gRPC pour `viewer/index.html`) et `examples/query-client`
  (CLI de requête) lancés comme cinq process séparés, partageant le
  catalogue SQLite persistant (`graph_storage::open_sql_catalog`) —
  requête correcte de bout en bout à travers eux, y compris via un vrai
  appel HTTP navigateur → `graph-viewer-server` → `graph-coordinator`.
  `GetNodeProperties`, ajouté à `PartitionService`, hydrate chaque
  `NodeId` résolu avec ses propriétés réelles avant de streamer le
  résultat — le client n'a plus à connaître le jeu de données pour
  afficher autre chose qu'un id opaque.

Fait (Phase 2, spec §12) — détail des décisions dans `TASKS.md` :
- Hash de partitionnement stable (`graph_schema::partitioning`, xxh3),
  discovery Kubernetes (`graph-cluster::KubernetesDiscovery`, `kube::Api<Pod>`
  + annotation `graph.io/partitions`) et statique (`StaticDiscovery`),
  rebalancement par hachage de rendez-vous (`RendezvousRebalancePlanner`)
  (`graph-cluster`, CL1-CL4).
- Exécuteur distribué réel (`graph-query::ScatterGatherExecutor`) :
  `ResolveStart` diffusé à toutes les partitions, `ExpandHop` décomposé en
  rounds réseau par hop physique avec re-routage sur `RemoteRef`,
  `WHERE` évalué après coup via `GetNodeProperties`, déduplication de
  frontière (Q5-Q8) — testé bout en bout avec une traversée qui franchit
  réellement une frontière de partition (Q8).
- `graph-index`'s builder (IX3, révisée) : filtre chaque scan aux nœuds
  possédés par la partition en construction et calcule les `RemoteRef`
  réels pour les arêtes cross-partition (au lieu du no-op Phase 1) —
  testé explicitement (`remote_edges_are_flagged_as_remote_ref_across_partitions`).
- Observabilité réelle : tracing JSON + export OpenTelemetry/OTLP avec
  repli silencieux si le collecteur est injoignable, propagation du
  contexte de trace à travers gRPC via des interceptors `tonic`
  (`inject_trace_context`/`extract_and_continue`), endpoint `/metrics`
  Prometheus sur les deux binaires avec des collecteurs réellement
  instrumentés (latence/erreurs/hops de requête, taille/durée de
  rebuild/âge d'index, latence de hop) (`graph-observability`, OB1-OB4).
- `bin/graph-coordinator` en mode distribué (CO5) : l'ancien chemin
  mono-partition dédié (`remote_executor.rs`) est supprimé, remplacé par
  `GrpcPartitionRpc` (implémente `graph_query::PartitionRpc` contre le
  client gRPC généré) piloté par `ScatterGatherExecutor` +
  `graph_cluster::Discovery`, sélectionnable par
  `GRAPH_DISCOVERY_MODE=static|kubernetes`. Le test d'intégration CO4
  (mono-partition) tourne désormais à travers ce même chemin distribué —
  aucune régression, plus de code séparé à maintenir pour le cas
  mono-partition.

Reste à faire :
- Le seul TBD encore ouvert côté spec (§13) : processus de migration de
  schéma incompatible — sans impact sur cette architecture, à traiter au
  moment venu.
- Non couvert par ce cadrage (Phase 3+, spec §12) : réplication/HA
  opérée pour de vrai (le modèle §6.4 est implémenté — répliques
  indépendantes, pas de consensus — mais pas encore exercé par un test
  de bascule), objectifs de performance chiffrés/benchmarking, cible de
  déploiement K8s finalisée (manifests réels).
- `CROSS_PARTITION_HOP_RATIO` (§9.1) reste déclaré mais pas encore
  observé en pratique dans `ScatterGatherExecutor` — voir `TASKS.md`
  OB4.
- `GetIndexStatus` en mode distribué relaie une partition représentative
  plutôt que d'agréger l'état du cluster — voir `TASKS.md` CO5.

## 6. Extension DSL hors roadmap : moindre privilège via télémétrie

Cette extension part d'un use case concret plutôt que de la roadmap
`§12` : identifier les permissions IAM déclarées mais jamais utilisées,
en croisant ce qu'une identité *peut* faire (accès déclaré,
`ASSUMES`/`GRANTS`) avec ce qu'elle *a fait* (accès observé par
télémétrie, `RAN_AS`/`ACCESSED`) — `schema/least_privilege.graphidl`
documente le modèle de données complet. La requête centrale du use case
est un `déclaré MOINS observé` : pour chaque permission accordée, existe-
t-il une preuve d'usage dans la fenêtre de confiance ? Absente, c'est un
candidat de moindre privilège. Cette forme de requête (anti-jointure
corrélée sur deux propriétés d'arête liées par le même alias de nœud
externe) n'existait dans aucune des deux formes prioritaires du DSL
(§7.1) — d'où l'extension, détaillée tâche par tâche dans `TASKS.md`
(« Extension DSL hors roadmap »). Décisions transverses, à l'échelle de
l'architecture plutôt que d'une seule crate :

- **Alias d'arête restreint à un hop fixe.** `[g:GRANTS]` ne peut lier
  qu'une arête à hop `min == max == 1` (validateur, D12/D13) — un alias
  sur `*1..3` désignerait une arête ambiguë de la chaîne. Portée
  volontairement étroite : le use case n'a jamais besoin d'un alias sur
  un hop variable, l'étendre y compris à ce cas ne peut se justifier que
  par un second use case encore hypothétique.
- **Pas de langage de dates.** `a.last_seen >= "2024-05-01T00:00:00Z"`
  compare un littéral `String` RFC3339 à une propriété `Timestamp` —
  aucune fonction `datetime()`/`duration()` n'a été ajoutée au DSL. La
  coercion est faite côté exécution (`graph-query::filter_eval`, Q9),
  pas dans l'AST : un seul point d'usage ne justifie pas un langage
  d'expressions dédié.
- **`NOT EXISTS` restreint à un hop sortant unique, non imbriqué, entre
  deux alias déjà liés par le `MATCH` externe.** Cette restriction n'est
  pas arbitraire : elle vient directement de la règle de placement
  physique posée en Phase 1/2 pour les nœuds — *l'enregistrement d'une
  arête est possédé par la partition de sa source* (`IX10`, même
  critère que `build_csr` pour l'adjacence sortante locale). Un hop
  entrant obligerait à interroger la partition de la *destination* pour
  une information (les propriétés de l'arête) qui n'y vit pas ; un hop
  imbriqué multiplierait les allers-retours réseau pour un besoin que ce
  use case ne démontre pas. La sous-requête doit être *corrélée* — son
  nœud d'ancrage doit déjà être lié à l'extérieur, jamais redéclaré avec
  son propre label — c'est ce qui la distingue d'un second pattern
  indépendant et lui donne un sens d'anti-jointure plutôt que
  d'existence isolée.
- **Résolution des conditions externes par binding, puis regroupement.**
  `a.action = g.action` référence la propriété d'un alias externe (`g`)
  dont la valeur varie par ligne de résultat — impossible à résoudre une
  seule fois pour toute la frontière, à la différence d'un `WHERE`
  classique après `ExpandHop`. `ScatterGatherExecutor::execute_anti_join`
  hydrate ces valeurs par binding (une seule fois, via
  `GetNodeProperties`/`GetEdgeProperties`) puis regroupe les bindings qui
  partagent la même liste de conditions déjà résolues avant d'émettre
  une requête `CheckAntiJoin` — un appel par `(partition, groupe)`, pas
  par binding. Sans ce regroupement, l'anti-jointure distribuée aurait
  un coût réseau proportionnel au nombre de bindings plutôt qu'au nombre
  de valeurs distinctes réellement en jeu.

Vérifié bout en bout sur deux partitions réelles, vraie infrastructure
Iceberg, vrai gRPC (`bin/graph-coordinator/tests/lp_end_to_end.rs`, LP1)
— pas seulement au niveau unitaire, pour la même raison que CO4/Q8 l'ont
été en Phase 1/2 : un anti-join qui fonctionne en mémoire mais jamais
testé à travers une vraie frontière de partition serait une preuve
incomplète.
