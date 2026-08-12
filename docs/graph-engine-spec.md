# Spécification — Knowledge Graph Engine (Rust)

Statut : **Draft v0.1** — issu d'un cadrage interactif, plusieurs points sont
volontairement marqués `TBD` (to be decided) et devront être tranchés avant
implémentation.

---

## 1. Vue d'ensemble

### 1.1 Objectif

Concevoir et implémenter un **moteur de graphe de connaissances (knowledge
graph)** dont l'usage principal est l'**exploration interactive à partir d'un
nœud** : voisinage k-hop filtré, et recherche de motifs (pattern matching) sur
des sous-graphes. Le moteur s'appuie sur **Apache Iceberg** comme couche de
stockage durable, avec un ou plusieurs **index en mémoire** (topologique +
propriétés) reconstruits périodiquement pour servir les requêtes à faible
latence.

### 1.2 Décisions de cadrage actées

| Dimension | Décision |
|---|---|
| Langage d'implémentation | Rust |
| Modèle de données | Property graph **statiquement typé** |
| Stockage durable | Apache Iceberg (tables sur object storage) |
| Index | Topologique (adjacence) + index par propriété |
| Runtime | Serveur avec API réseau (pas de mode embarqué) |
| Langage de requête | DSL de traversal type Cypher/Gremlin |
| Opérations prioritaires | Voisinage k-hop filtré, pattern matching |
| Agrégation | **Aucune dans le moteur** — délégation en aval (voir §7.1, §1.4) |
| Ingestion | Batch externe (Spark/Flink → Iceberg), pas d'écriture directe via le serveur |
| Cohérence | Éventuelle, snapshot isolation en lecture (time-travel Iceberg) |
| Rafraîchissement d'index | Reconstruction complète périodique (pas d'incrémental) |
| Échelle | Cluster distribué, graphe partitionné |
| Partitionnement | Hash-partitioning par `node_id`, sur-partitionnement fixe (§6.2) |
| Réplication / haute disponibilité | Répliques indépendantes sans consensus (pas de Raft), voir §6.4 |
| Exécution distribuée multi-partitions | Scatter-gather piloté par le coordinateur (voir §7.4) |
| Schéma | Déclaratif, versionné, IDL dédiée, migrations |
| Observabilité | Métriques (Prometheus) + tracing distribué (OpenTelemetry) |
| Sécurité / AuthN-AuthZ | **Hors scope v1** — mis de côté délibérément (voir §1.4, §8.3) |
| Cible de déploiement | Kubernetes (voir §10) |
| Objectifs de performance | Pas de cible chiffrée pour l'instant — décision différée (voir §13) |

### 1.3 Pourquoi ces choix structurants

Le tableau §1.2 dit *quoi* ; les décisions les plus discutées (exécution
distribuée, réplication, partitionnement, sécurité, déploiement...) ont
leur justification détaillée dans leur section dédiée (renvois ci-dessous).
Pour les choix fondateurs qui n'ont pas de section dédiée, la
justification est résumée ici :

| Dimension | Pourquoi |
|---|---|
| Rust | Pas de GC → latence prévisible pour un moteur qui sert un index en mémoire partagé entre threads/requêtes concurrentes ; sûreté mémoire (pas de data race) sur des structures concurrentes comme l'index CSR (§5.1) sans le coût d'un langage managé ; écosystème mature pour du réseau/systems programming (`tokio`, `tonic`). |
| Property graph statiquement typé | Valider à l'ingestion plutôt qu'à la lecture (§3.1) suppose de connaître les types à l'avance ; permet un layout mémoire fixe par label (plus compact/rapide qu'une map dynamique par nœud) et la génération de bindings clients typés (§3.4). |
| Apache Iceberg (stockage durable) | Schema evolution native (aligne §3.5 sans réimplémenter la logique de migration compatible) ; time-travel = snapshot isolation en lecture "gratuite" (§4.2) ; interopérable avec l'écosystème batch existant (Spark/Flink) plutôt que de réinventer un format de table ; durabilité/réplication déjà gérées par l'object storage sous-jacent — condition qui permet §6.4 (pas de Raft nécessaire). |
| Index = topologique + propriété (rien de plus en v1) | Ce sont exactement les deux structures nécessaires aux deux opérations prioritaires actées (résolution du départ via propriété, §5.2 ; expansion via adjacence, §5.1) — pas de structure supplémentaire tant qu'un besoin ne le justifie pas (cf. index vectoriel/full-text, différés en Phase 4, §5.2). |
| Runtime serveur réseau (pas de mode embarqué) | Un knowledge graph d'entreprise dépasse la mémoire d'un seul process client ; plusieurs clients doivent interroger le même graphe partagé sans dupliquer l'index ; découple le cycle de vie du moteur (rebuild, cluster) de celui des applications clientes. |
| DSL type Cypher/Gremlin | Standard de facto pour le pattern matching sur property graph, syntaxe déjà connue de quiconque a pratiqué Neo4j/TinkerPop ; le `MATCH` correspond directement aux deux opérations prioritaires visées (§1.1) sans traduction conceptuelle. |
| Ingestion batch externe (pas d'écriture directe) | Sépare le chemin de lecture à faible latence du traitement d'ingestion ; réutilise des moteurs de traitement batch/streaming déjà éprouvés (Spark/Flink) plutôt que d'en réimplémenter un dans le moteur graphe ; rend possible le modèle de cohérence simple retenu (pas d'écritures concurrentes à coordonner côté graphe). |
| Cohérence éventuelle, snapshot isolation en lecture | Conséquence directe de la séparation lecture/écriture ci-dessus : fournie nativement par Iceberg (§4.2), suffisante pour un usage d'exploration/analyse où une staleness bornée est acceptable — pas besoin de transactions distribuées pour ce cas d'usage. |
| Reconstruction complète périodique (pas d'incrémental en v1) | Plus simple à implémenter et à raisonner qu'un rebuild incrémental (pas de delta-state à réconcilier, pas de risque de référence orpheline) ; la staleness résultante est acceptable pour le cas d'usage (§5.3) — l'incrémental reste une optimisation future une fois le modèle validé (§12, Phase 4). |
| Échelle distribuée dès la conception | Anticipe la taille réelle d'un knowledge graph d'entreprise (potentiellement au-delà de la mémoire d'une seule machine) ; partitionner dès l'architecture évite une réécriture structurelle majeure plus tard — même si le MVP (§12, Phase 1) démarre volontairement mono-partition pour valider le modèle avant de distribuer. |
| Observabilité (Prometheus + OpenTelemetry) | Standards ouverts de l'écosystème cloud-native, cohérents avec la cible de déploiement Kubernetes (§10) ; large écosystème d'exporters/backends compatibles, pas de verrouillage propriétaire. |

### 1.4 Non-objectifs (pour ce scope)

- **AuthN/AuthZ** (§8.3) — mis de côté délibérément pour ce cadrage : pas
  d'authentification client, pas d'autorisation fine, pas d'audit log en
  v1. Décision de scope assumée (le scope est un moteur mono-tenant en
  environnement de confiance), pas un TBD à trancher plus tard — à
  reconsidérer explicitement si une mise en production hors environnement
  de confiance est envisagée un jour.
- Écritures transactionnelles temps réel multi-nœuds/arêtes avec garanties
  ACID fortes (le modèle retenu est snapshot/eventual consistency — pourquoi,
  voir §1.3 "Cohérence éventuelle").
- Algorithmes analytiques globaux lourds (PageRank, détection de communautés)
  en v1 — ils sont repoussés en phase ultérieure (§12) et pourraient être
  délégués à un moteur externe (Spark/DataFusion) plutôt que réimplémentés :
  même logique que pour l'index vectoriel/full-text (§5.2) — mieux traité
  par un moteur spécialisé que réimplémenté ici.
- Mode embarqué (librairie in-process) — hors scope, le moteur est un
  serveur réseau dès la v1 (pourquoi, voir §1.3 "Runtime serveur réseau").
- **Agrégation** (`COUNT`, `SUM`, `AVG`, `GROUP BY`, `COLLECT`, etc.) — le
  moteur reste un pur moteur de **traversal / pattern-matching**. Toute
  agrégation sur les résultats est déléguée en aval, hors du moteur graphe
  (côté client, ou via un moteur externe type DataFusion/Spark sur les
  résultats bruts exportés/streamés). Décision actée explicitement plutôt
  que TBD — voir détail §7.1.

---

## 2. Terminologie

- **Node (nœud)** : entité du graphe, possède un `label` (type) et des
  propriétés typées.
- **Edge (arête)** : relation dirigée entre deux nœuds, possède un `type` et
  des propriétés typées optionnelles.
- **Schema** : définition versionnée des labels de nœuds, types d'arêtes et
  de leurs propriétés typées.
- **Snapshot** : version immuable des tables Iceberg à un instant donné,
  utilisée pour le time-travel et l'isolation en lecture.
- **Index topologique** : structure d'adjacence en mémoire (style CSR —
  Compressed Sparse Row) permettant de lister rapidement les voisins d'un
  nœud.
- **Index de propriété** : structure permettant de retrouver des nœuds/arêtes
  par valeur de propriété (égalité / range).
- **Partition** : sous-ensemble du graphe hébergé par un nœud de calcul du
  cluster, déterminé par `hash(node_id) % n_partitions`.
- **Coordinateur** : composant qui reçoit une requête cliente, la décompose
  et orchestre son exécution à travers les partitions concernées.

---

## 3. Modèle de données

### 3.1 Principes

Le graphe est un **property graph statiquement typé** :

- Chaque nœud a **exactement un label unique** (type), défini dans le
  schéma. **Décision actée (§13) : pas de multi-label.** Le besoin de
  représenter une entité sous plusieurs facettes (ex: une `Person` qui est
  aussi un `Author`) se modélise via une **arête dédiée** entre deux nœuds
  distincts (ex: `(p:Person)-[:IS_A]->(a:Author)`) plutôt que par des
  labels multiples sur un même nœud — cohérent avec le modèle de pattern
  matching du moteur (§7.1).
- Chaque arête a **exactement un type de relation**, dirigé, défini dans le
  schéma, avec un label de nœud source et un label de nœud cible autorisés
  (contrainte de typage des extrémités).
- Les propriétés (sur nœuds et arêtes) sont déclarées dans le schéma avec un
  **type scalaire ou composite fixe** — pas de propriété libre non déclarée.
- Toute donnée non conforme au schéma est rejetée **à l'ingestion** (validée
  côté pipeline batch avant/pendant l'écriture Iceberg), pas au moment de la
  lecture.

### 3.2 Types scalaires supportés

| Type | Description |
|---|---|
| `Int64` | Entier signé 64 bits |
| `Float64` | Flottant double précision |
| `Bool` | Booléen |
| `String` | Chaîne UTF-8 |
| `Timestamp` | Horodatage UTC, précision microseconde |
| `Bytes` | Blob binaire |
| `List<T>` | Liste homogène d'un type scalaire ci-dessus |
| `Vector<Float32, N>` | Vecteur dense de dimension fixe `N` (réservé usage futur — embeddings, cf. §13) |

### 3.3 Identité des nœuds et arêtes

- `node_id` : identifiant unique global, `UInt64`. **Décision actée (§13) :
  double mode.**
  - Si la donnée source porte une clé métier stable, le pipeline
    d'ingestion **fournit explicitement** `node_id` (dérivé par hachage
    stable de cette clé) — garantit l'**idempotence** : une réingestion de
    la même entité retombe sur le même `node_id` et met à jour le nœud
    existant au lieu d'en créer un doublon.
  - Sinon (pas de clé métier stable disponible en source), `node_id` est
    **généré** par le pipeline (compteur ou UUID) — pas de garantie
    d'idempotence dans ce cas : une réingestion sans corrélation explicite
    avec la source peut créer un nouveau nœud plutôt que mettre à jour
    l'existant.
  - Sert de clé de partitionnement (hash-partitioning) dans les deux cas.
- `edge_id` : identifiant unique global `UInt64`, indépendant de
  `(src, dst, type)` pour permettre des arêtes multiples entre les deux mêmes
  nœuds (multigraphe).
- Une arête référence `src_node_id`, `dst_node_id`, `edge_type`.

### 3.4 Schéma déclaratif (IDL)

Le schéma est défini dans un fichier déclaratif versionné (syntaxe proche
Protobuf/Avro), source de vérité pour :

1. La validation à l'ingestion.
2. La génération du schéma des tables Iceberg sous-jacentes.
3. La génération de bindings clients typés (Rust en priorité).
4. La validation statique des requêtes DSL (labels/types de relation valides,
   types de propriétés).

Exemple illustratif (syntaxe à finaliser) :

```
schema graph_v1 {

  node Person {
    id: NodeId
    name: String
    birth_year: Int64?
  }

  node Organization {
    id: NodeId
    name: String
  }

  edge WORKS_AT {
    from: Person
    to: Organization
    since: Timestamp?
  }

  edge KNOWS {
    from: Person
    to: Person
    since: Timestamp?
  }
}
```

### 3.5 Évolution du schéma

- Schéma **versionné** (`schema graph_v1`, `graph_v2`, ...).
- Évolutions **compatibles** (ajout de propriété optionnelle, ajout de
  nouveau label/type de relation) : appliquées sans migration bloquante,
  alignées sur le *schema evolution* natif d'Iceberg (ajout de colonne).
- Évolutions **incompatibles** (suppression/renommage de propriété,
  changement de type) : nécessitent une migration explicite versionnée et un
  rebuild complet de l'index. *(Processus détaillé de migration : TBD, §13.)*

---

## 4. Couche de stockage (Apache Iceberg)

### 4.1 Organisation des tables

- Une table Iceberg **par label de nœud** (ex: `nodes_person`,
  `nodes_organization`), colonnes = propriétés déclarées dans le schéma +
  `node_id`.
- Une table Iceberg **par type d'arête** (ex: `edges_works_at`,
  `edges_knows`), colonnes = `edge_id`, `src_node_id`, `dst_node_id` +
  propriétés déclarées.
- Partitionnement physique des tables Iceberg lui-même (partition spec
  Iceberg, indépendante du partitionnement logique du cluster de calcul,
  §7) : **TBD** — candidat : bucket par `node_id`/`src_node_id` pour aligner
  les partitions de lecture avec le partitionnement du cluster et limiter le
  fan-out à la reconstruction d'index.

### 4.2 Time-travel et isolation

- Chaque cycle de rafraîchissement d'index (§5.3) épingle un **snapshot ID
  Iceberg unique** pour toutes les tables lues, garantissant une vue
  cohérente du graphe (toutes les tables lues au même instant logique).
- Les requêtes clientes s'exécutent contre l'index en mémoire construit à
  partir de ce snapshot — donc contre une vue figée jusqu'au prochain
  rafraîchissement (staleness bornée par la fréquence de rebuild, §5.3).

### 4.3 Contrat avec le pipeline d'ingestion

- Le moteur graphe est **lecteur seul** des tables Iceberg : aucune écriture
  n'est effectuée par le serveur.
- Les jobs Spark/Flink externes doivent :
  - Écrire dans les tables conformes au schéma déclaré (§3.4).
  - Committer via l'API Iceberg standard (garantit atomicité par commit).
  - Respecter les contraintes de typage/label (validation **recommandée
    côté pipeline**, le moteur graphe re-valide a minima les types au
    chargement et rejette/logge les lignes non conformes plutôt que de
    planter).

---

## 5. Sous-système d'indexation

### 5.1 Index topologique (adjacence)

- Structure **CSR-like** (Compressed Sparse Row) par partition :
  - Un tableau d'offsets par `node_id` local à la partition.
  - Un tableau contigu des `(dst_node_id, edge_id, edge_type)` sortants
    (et, séparément, entrants — pour supporter les traversals dans les deux
    directions).
- Construit en mémoire à chaque cycle de rebuild à partir des tables
  d'arêtes Iceberg filtrées sur les nœuds de la partition.
- Les nœuds/arêtes dont l'autre extrémité appartient à une autre partition
  sont représentés par une **référence distante** `(partition_id,
  node_id)` — nécessaire pour le routage lors des traversals
  inter-partitions (§7.4).

### 5.2 Index de propriété

- Un index par `(label_ou_type, propriété)` déclarée comme indexable dans le
  schéma (annotation explicite, pas toutes les propriétés par défaut, pour
  borner le coût mémoire).
- Structure : B-Tree ordonné pour supporter égalité + range queries.
- Utilisé pour la résolution des nœuds de départ d'une traversal (ex:
  `MATCH (p:Person {name: "Alice"})`) avant d'entrer dans le moteur de
  traversal topologique.
- Index full-text et index vectoriel (embeddings) : **hors scope v1**, notés
  comme extension future (§12 Phase 4, §13) — non retenus dans le cadrage
  initial en dehors de propriété + topologique. Pourquoi : ce sont des
  paradigmes d'indexation différents (recherche approximative par
  similarité vs recherche exacte par égalité/range) qui répondent à un
  besoin distinct des deux opérations prioritaires actées (k-hop filtré,
  pattern matching, §1.1) ; mieux traités par des moteurs spécialisés
  existants que réimplémentés dans ce moteur — même logique que pour les
  algorithmes analytiques globaux (§1.4).

### 5.3 Stratégie de rafraîchissement

- **Reconstruction complète périodique** (pas d'incrémental en v1) :
  - Intervalle configurable (ex: toutes les N minutes) — **TBD** valeur par
    défaut.
  - À chaque cycle : résolution du dernier snapshot Iceberg par table,
    scan complet, reconstruction des index topologique + propriété en
    mémoire dans une nouvelle génération, puis **swap atomique** avec la
    génération servie (les requêtes en cours continuent sur l'ancienne
    génération jusqu'à leur terminaison — pas d'interruption).
  - Les générations d'index précédentes sont libérées une fois qu'aucune
    requête ne les référence plus.
- Compromis assumé : simplicité d'implémentation, coût CPU/mémoire du
  rebuild complet à chaque cycle, staleness = intervalle de rebuild. Une
  évolution vers un rebuild incrémental (basé sur les snapshots Iceberg
  delta) est une amélioration future possible (§13), pas un prérequis v1.

---

## 6. Architecture du cluster

### 6.1 Rôles

- **Coordinateur** (stateless ou quasi-stateless) : reçoit les requêtes
  clientes, effectue le parsing/planification de la requête DSL, orchestre
  l'exécution distribuée (§7.4), agrège et retourne les résultats.
- **Nœud de partition** (stateful) : héberge une partition du graphe
  (index topologique + propriété en mémoire pour son sous-ensemble de
  nœuds), exécute les hops de traversal локalement, répond aux requêtes du
  coordinateur.
- *(TBD : le rôle coordinateur peut être co-localisé avec les nœuds de
  partition ou déployé séparément — impact sur le déploiement, §10.)*

### 6.2 Partitionnement

- **Hash-partitioning** par `node_id` : `partition_id = hash(node_id) %
  n_partitions`.
- **Décision actée : sur-partitionnement fixe, découplé du nombre de
  machines.** `n_partitions` est un nombre **logique fixé une fois pour
  toutes** à la création du graphe, volontairement sur-dimensionné par
  rapport au nombre de nœuds de calcul initial (ex: 100 partitions
  logiques pour 3 machines). Ce nombre **ne change jamais** — donc
  `hash(node_id) % n_partitions` reste stable dans le temps, et aucun nœud
  ne change jamais de partition logique.
- **Rebalancement = réaffectation physique, pas rehash.** Ajouter ou
  retirer une machine ne touche qu'à la table d'affectation "partition
  logique → machine physique" : on déplace un sous-ensemble de partitions
  logiques existantes d'une machine à une autre. Seules les partitions
  déplacées ont leur index reconstruit sur leur nouvelle machine
  (cohérent avec la stratégie de rebuild périodique déjà retenue, §5.3) —
  pas de rebuild global du graphe. Modèle inspiré de Nebula Graph /
  consistent hashing (Cassandra).
- Réplication des partitions pour la haute disponibilité : voir §6.4.

### 6.3 Membership et découverte

- Mécanisme de découverte des nœuds de partition par le(s) coordinateur(s) :
  **intégration native Kubernetes** (API des Pods/Endpoints, ou headless
  Service) — décision alignée sur le choix de cible de déploiement (§10).
  Pas de registre externe (etcd/Consul) dédié en v1.

### 6.4 Réplication et haute disponibilité

**Décision actée : répliques indépendantes sans protocole de consensus.**

À la différence des bases de graphe qui acceptent des écritures (Neo4j,
Dgraph, Nebula Graph, TigerGraph — toutes s'appuient sur **Raft**, un
protocole de consensus, pour mettre d'accord plusieurs répliques sur
l'ordre des écritures), notre moteur n'a pas de write path (§4.3) : la
donnée durable vit dans Iceberg, déjà répliquée par l'object storage
sous-jacent. L'index en mémoire d'une partition est **entièrement dérivé
et rebuildable** à partir d'un snapshot Iceberg figé (§5.3).

Conséquence : pas besoin de consensus entre répliques.

- Chaque partition logique a **N répliques** (facteur configurable, ex: 3),
  hébergées sur des machines distinctes.
- Chaque réplique **reconstruit indépendamment** le même index à partir du
  même snapshot Iceberg épinglé (§5.3) — aucune coordination ni échange de
  données entre répliques n'est nécessaire pour qu'elles convergent vers
  un état identique.
- Le coordinateur route chaque requête vers **n'importe quelle réplique
  saine** de la partition concernée (round-robin ou least-loaded) —
  toutes les répliques étant équivalentes, il n'y a pas de notion de
  leader/follower à gérer côté lecture.
- **Failover** : en cas de panne d'une réplique, le coordinateur cesse de
  lui router du trafic (détecté via `HealthCheck`, §8.2) ; une nouvelle
  réplique est provisionnée et reconstruit son index en tâche de fond,
  selon le même mécanisme que le rebalancement (§6.2) — pas d'interruption
  de service tant qu'au moins une réplique saine reste disponible par
  partition.

Ce modèle est nettement plus simple à opérer que Raft-par-partition — il
n'est possible que parce que l'architecture est lecture seule avec un état
entièrement dérivable d'une source durable externe, plutôt qu'un
compromis de facilité : c'est une conséquence directe des décisions déjà
actées (§4.3, §5.3).

---

## 7. Moteur de requêtes

### 7.1 Langage de requête (DSL)

DSL de traversal inspiré de **Cypher/Gremlin**, une requête a toujours la
forme `MATCH <motif> [WHERE <conditions>] RETURN <projection>` — pas
d'autre clause de tête. Grammaire formelle : `graph-dsl/src/dsl.pest`
(implémentation `pest`), ce qui suit en est la description narrative.

**Motif (`MATCH`).** Une chaîne alternant nœuds et arêtes :
`(alias[:Label] [{prop: littéral, ...}]) -[...]-> (alias[:Label] ...) ...`

- **Nœud** : `(alias:Label {propriété: littéral, ...})` — le label et le
  bloc de propriétés sont optionnels ; un nœud sans label ni propriétés
  (`(w)`) est une simple *référence* à un alias déjà lié ailleurs dans la
  requête (utilisé par `NOT EXISTS`, voir plus bas), pas une nouvelle
  liaison. Le bloc de propriétés n'accepte que des filtres d'égalité
  (`{name: "Alice"}`) — un filtre d'inégalité/plage sur le nœud de départ
  se fait via `WHERE` (voir plus bas), pas dans le motif lui-même.
- **Arête** : `-[alias?:TYPE? hop_range?]->` (sortante) ou
  `<-[...]-` (entrante) — type et alias sont chacun optionnels, une
  arête sans type (`-->`) matche tout type de relation. `hop_range`
  (`*min..max`, ex. `*1..3`) rend le hop variable ; absent, un hop est
  toujours simple (min = max = 1).
  - **Alias d'arête** (`[g:GRANTS]`) : lie l'instance d'arête traversée
    (pas seulement ses deux extrémités), pour la référencer ensuite dans
    `WHERE`/`RETURN` (`g.action`). Restreint aux hops à cardinalité fixe
    (`hop_range` absent ou `*1..1`) — quelle arête d'une chaîne `*1..3`
    un alias désignerait-il ? Rejeté par le validateur avec un message
    dédié (`EdgeAliasOnVariableLengthHop`), pas par un échec de parsing
    générique.

**Conditions (`WHERE`)**, une conjonction (`AND`) de :
- **Filtre de propriété** — `alias.propriété OP littéral`
  (`friend.birth_year > 1990`). `OP` ∈ `= <> < <= > >=` ; les opérateurs
  d'ordre ne sont valides que sur `Int64`/`Float64`/`Timestamp` (§3.2,
  vérifié par le validateur, §7.2). Un littéral `String` comparé à une
  propriété `Timestamp` est interprété comme un horodatage RFC3339
  (`a.last_seen >= "2024-05-01T00:00:00Z"`) — décision délibérée plutôt
  que d'ajouter une fonction `datetime()`/`duration()` au langage pour un
  seul point d'usage (voir plus bas, "hors scope").
- **Comparaison d'alias** — `alias OP alias` (`colleague <> p`) : compare
  deux alias *entiers* liés par le motif pour (in)égalité de nœud, pas
  une propriété.
- **Comparaison propriété-propriété** — `alias.propriété OP
  alias.propriété` (`a.action = g.action`) : compare la propriété de deux
  alias liés (nœud ou arête indifféremment) entre eux, plutôt que contre
  un littéral. À distinguer des deux formes précédentes malgré une
  syntaxe proche — la présence du `.` de chaque côté du `OP` fait foi.
- **`NOT EXISTS { MATCH ... [WHERE ...] }`** — vérification d'existence
  corrélée : le motif interne ne peut que *référencer* des alias déjà
  liés par le `MATCH` externe (nœuds sans label/propriétés, comme `(w)`
  ci-dessous) et décrire **exactement un hop sortant** de plus depuis
  là :
  ```
  NOT EXISTS {
    MATCH (w)-[a:ACCESSED]->(d)
    WHERE a.action = g.action AND a.last_seen >= "2024-05-01T00:00:00Z"
  }
  ```
  Ni un second motif indépendant, ni une sous-requête générale : la
  portée est intentionnellement celle d'une anti-jointure corrélée à un
  hop, pas un langage de sous-requêtes. Un hop **entrant** ou un
  `NOT EXISTS` imbriqué dans un autre sont syntaxiquement acceptés par la
  grammaire mais rejetés par le validateur (`NotExistsMustBeOutgoing`,
  `NestedNotExists`) — voir §7.2 et `ARCHITECTURE.md` §6 pour la
  justification (placement physique d'une arête par sa partition
  source).

**Projection (`RETURN`)**, une liste de :
- `alias` — projette l'enregistrement complet du nœud (ou de l'arête)
  lié par cet alias.
- `alias.propriété` — projette une seule propriété scalaire de cet alias
  (nœud ou arête).

Les deux formes sont mélangeables dans la même clause
(`RETURN w.service, w.team, r.arn, g.action, d.arn`).

**Deux exemples des opérations prioritaires actées** (§1.1) :

- **Voisinage k-hop filtré** :
  ```
  MATCH (p:Person {name: "Alice"})-[:KNOWS*1..3]->(friend:Person)
  WHERE friend.birth_year > 1990
  RETURN friend
  ```
- **Pattern matching (sous-graphes)** :
  ```
  MATCH (p:Person)-[:WORKS_AT]->(o:Organization)<-[:WORKS_AT]-(colleague:Person)
  WHERE p.name = "Alice" AND colleague <> p
  RETURN colleague, o
  ```

**Un troisième exemple**, motivant à lui seul l'alias d'arête, la
comparaison propriété-propriété et `NOT EXISTS` (use case moindre
privilège prouvé par la télémétrie, `schema/least_privilege.graphidl`,
détail complet dans `TASKS.md`/`ARCHITECTURE.md` §6 du dépôt
`graph-engine`) :
```
MATCH (w:Workload)-[:ASSUMES]->(r:IAMRole)-[g:GRANTS]->(d:DataStore)
WHERE d.data_class = "sensitive"
  AND NOT EXISTS {
    MATCH (w)-[a:ACCESSED]->(d)
    WHERE a.action = g.action AND a.last_seen >= "2024-05-01T00:00:00Z"
  }
RETURN w.workload_id, w.team, r.arn, g.action, d.arn
```

Hors scope v1 (noté explicitement, cf. §1.4 et §12) : `shortest path`,
algorithmes analytiques globaux.

**Pas d'agrégation** : le DSL ne comporte volontairement **aucune**
fonction d'agrégation (`COUNT`, `SUM`, `AVG`, `MIN`/`MAX`, `COLLECT`) ni
clause `GROUP BY`. `RETURN` ne fait que **projeter** les nœuds/arêtes/
propriétés matchés par le `MATCH`, sans les réduire ni les regrouper —
décision actée (§1.4), pas un TBD. Un besoin d'agrégation se traite en
aval de la réponse streamée du moteur (§7.5), côté client ou via un
moteur externe (DataFusion/Spark) sur les résultats bruts exportés.

**Encore hors scope, `TBD`** : `ORDER BY`/`LIMIT`/pagination des
résultats ; `OR` dans `WHERE` (seule la conjonction `AND` existe) ; un
alias d'arête sur un hop à cardinalité variable ; un `NOT EXISTS` imbriqué
ou à plus d'un hop ; un langage d'expressions dédié aux dates
(`datetime()`/`duration()` — la coercion RFC3339 ci-dessus couvre le seul
besoin rencontré à ce jour, pas un cas général). À détailler dans une
spec de langage dédiée si un nouveau use case en a besoin, plutôt
qu'anticipé ici.

### 7.2 Validation statique

- Toute requête est validée contre le **schéma versionné actif** (§3.5)
  avant exécution : labels/types de relation existants, types de propriétés
  compatibles avec les opérateurs utilisés dans les clauses `WHERE`.
- Erreurs de validation retournées au client avant toute exécution
  distribuée (fail-fast).

### 7.3 Planification de requête

- Le coordinateur transforme la requête DSL validée en un plan d'exécution :
  1. Résolution des nœuds de départ via l'**index de propriété** (§5.2) sur
     les partitions concernées.
  2. Expansion topologique **hop par hop** via l'**index d'adjacence**
     (§5.1), en appliquant les filtres (`WHERE`) au plus tôt (pushdown) pour
     limiter la taille des frontières intermédiaires.
  3. Fusion des résultats distribués (déduplication, pas de réduction/
     agrégat — cf. §7.1) et projection (`RETURN`).
- Optimisations envisageables (ordre d'évaluation des filtres, choix de
  l'index de départ le plus sélectif, déduplication de frontière) : **TBD**,
  à approfondir une fois un premier plan d'exécution naïf validé.

### 7.4 Exécution distribuée multi-partitions

**Décision actée : scatter-gather piloté par le coordinateur.**

À chaque hop, le coordinateur envoie les frontières courantes aux
partitions concernées, attend les réponses, ré-agrège (déduplication, cf.
§7.3) et ré-envoie pour le hop suivant. Le coordinateur reste donc le point
de passage obligé de toute traversée multi-partitions, avec un round-trip
réseau par hop.

Raisons du choix :

- Simple à raisonner, à observer et à débugger (un seul point où logguer/
  tracer l'état de la frontière à chaque hop) — cohérent avec le tracing
  distribué visé (§9.2).
- Suffisant pour les profondeurs de traversal ciblées en v1 (k-hop borné,
  `*1..3` dans l'exemple §7.1) : le surcoût d'un round-trip par hop reste
  négligeable devant la complexité d'implémentation d'un modèle pair-à-pair.
- Le modèle **message-passing pair-à-pair (style Pregel)**, où chaque nœud
  de partition relaierait directement les hops sortants vers la partition
  cible sans repasser par le coordinateur, reste une évolution possible si
  des traversées plus profondes ou un débit plus élevé l'exigent — mais il
  introduit une complexité significative (terminaison distribuée,
  agrégation finale, observabilité) non justifiée pour le scope v1. Non
  retenu, documenté ici comme option de Phase 2+ si le besoin apparaît.

Impact : cette décision fixe l'architecture réseau interne du cluster
(§6.1 — le coordinateur est un composant à part entière du chemin de
requête, pas seulement un point d'entrée) et le modèle d'instrumentation du
tracing distribué (§9.2 — une trace = une séquence d'appels coordinateur →
partitions, hop par hop).

### 7.5 Streaming des résultats

- Les résultats doivent pouvoir être **streamés** au client au fur et à
  mesure (plutôt que matérialisés intégralement en mémoire côté
  coordinateur) pour les requêtes à large frontière — cohérent avec un choix
  de protocole réseau supportant le streaming (gRPC pressenti, **TBD** au
  §8.1).

---

## 8. API réseau

### 8.1 Protocole de transport — **TBD**

Non explicitement cadré ; candidat par défaut proposé : **gRPC** (support
natif du streaming bidirectionnel, typage via Protobuf cohérent avec le
modèle statiquement typé et l'IDL de schéma, bon écosystème Rust via
`tonic`). Une **gateway REST/HTTP** optionnelle en façade pourrait être
ajoutée ultérieurement pour les clients non-gRPC.

### 8.2 Opérations exposées (v1)

| Opération | Description |
|---|---|
| `ExecuteQuery(dsl: String) -> Stream<Result>` | Exécute une requête DSL, retourne un flux de résultats. |
| `GetSchema() -> Schema` | Retourne le schéma actif (version courante). |
| `GetIndexStatus() -> IndexStatus` | Métadonnées sur la génération d'index active : snapshot Iceberg épinglé, timestamp de dernier rebuild, nombre de nœuds/arêtes chargés. |
| `HealthCheck() -> Health` | Liveness/readiness du nœud (coordinateur ou partition). |

### 8.3 Authentification / Autorisation — **hors scope v1 (décision actée)**

Mis de côté délibérément pour ce cadrage plutôt que traité comme un TBD à
lever avant implémentation : le moteur v1 est conçu pour un déploiement en
environnement de confiance (réseau interne, pas d'exposition directe à des
clients non fiables), sans couche AuthN/AuthZ dans le serveur lui-même.

Resteraient à traiter si une mise en production hors environnement de
confiance était envisagée (non planifié à ce stade, noté pour mémoire) :
- Authentification des clients (mTLS, tokens, autre).
- Autorisation fine (par label de nœud/type de relation, par propriété) —
  pertinent en contexte knowledge graph d'entreprise avec données
  sensibles.
- Audit log des requêtes/mutations — non retenu explicitement dans le
  cadrage (seule l'observabilité métriques/tracing a été actée, §9).

---

## 9. Observabilité

### 9.1 Métriques (Prometheus)

Métriques minimales à exposer par composant :

**Coordinateur**
- Latence de requête end-to-end (histogramme, par type d'opération DSL).
- Nombre de hops exécutés par requête.
- Taux d'erreurs de validation / d'exécution.

**Nœud de partition**
- Taille de l'index en mémoire (nombre de nœuds/arêtes, empreinte mémoire).
- Durée du dernier rebuild d'index, âge du snapshot Iceberg épinglé
  (staleness).
- Latence de résolution d'un hop local.
- Taux de références distantes (cross-partition) par requête — signal clé
  pour évaluer la qualité du partitionnement (§6.2).

### 9.2 Tracing distribué (OpenTelemetry)

- Trace de bout en bout d'une requête : parsing DSL → planification →
  exécution par hop/partition → agrégation. Chaque saut inter-partition
  (§7.4) doit propager le contexte de trace pour permettre de visualiser le
  fan-out réel d'une requête à travers le cluster.

### 9.3 Logging

- Logs structurés (JSON), niveau configurable, corrélés par `trace_id`.
- Détail précis (format, rétention, centralisation) : **TBD**.

---

## 10. Déploiement

**Décision actée : Kubernetes.**

Pourquoi Kubernetes plutôt qu'une alternative plus simple (VMs/systemd) :
c'est la cible standard de facto pour orchestrer des services distribués
stateful, avec des primitives qui correspondent directement à notre
modèle — `StatefulSet` pour l'identité stable requise par le mapping
partition ↔ pod (§6.2), découverte native de services (§6.3) sans
registre externe à opérer, et intégration directe avec la stack
d'observabilité déjà retenue (Prometheus/OpenTelemetry, §9). Le coût
opérationnel de K8s est assumé au regard de l'échelle visée (cluster
distribué et partitionné dès la conception, §1.3) — une alternative plus
simple resterait pertinente pour un déploiement mono-machine ou à très
petite échelle, ce qui n'est pas le scope ici.

- **StatefulSet** pour les nœuds de partition — identité stable requise pour
  le mapping partition ↔ pod (§6.2).
- **Deployment** pour les coordinateurs si le rôle est séparé du rôle
  partition (§6.1), avec autoscaling horizontal pertinent côté
  coordinateurs uniquement — les partitions étant stateful et liées au
  partitionnement hash, un changement de réplicas y implique un
  rebalancement (§6.2), pas un simple scale-out.
- Découverte des nœuds de partition (§6.3) : intégration native
  Kubernetes (API des Pods/Endpoints ou headless Service), cohérente avec
  ce choix de cible — plus besoin d'un registre externe (etcd/Consul)
  dédié pour ce seul usage.

---

## 11. Modèle de cohérence — résumé

- **Lecture** : chaque cycle de rebuild d'index épingle un snapshot Iceberg
  cohérent (§4.2) ; toutes les requêtes servies par une génération d'index
  donnée voient une vue figée et cohérente du graphe.
- **Écriture** : asynchrone, hors du chemin de requête (pipeline batch
  externe, §4.3) ; aucune garantie de visibilité immédiate — la staleness
  maximale est bornée par l'intervalle de rebuild (§5.3).
- **Pas de transactions multi-entités** portées par le moteur graphe
  lui-même ; l'atomicité des écritures est garantie au niveau des commits
  Iceberg individuels par le pipeline d'ingestion, pas au niveau du graphe
  logique.

---

## 12. Roadmap proposée

| Phase | Contenu |
|---|---|
| **Phase 0 — Fondations** | Définition finale de l'IDL de schéma, mapping schéma → tables Iceberg, parser DSL (sous-ensemble k-hop + pattern matching simple). |
| **Phase 1 — Single-node MVP** | Serveur mono-partition : index topologique + propriété en mémoire, rebuild complet périodique, API réseau (opérations §8.2), pas de distribution. Valide le modèle de données et le DSL de bout en bout. |
| **Phase 2 — Distribution** | Partitionnement hash, rôle coordinateur, choix et implémentation du modèle d'exécution multi-partitions (§7.4), tracing distribué. |
| **Phase 3 — Durcissement production** | Stratégie de réplication/HA (§6.2), cible de déploiement finalisée (§10), objectifs de performance chiffrés et benchmarking. AuthN/AuthZ (§8.3) explicitement hors scope de la roadmap — à réintroduire seulement si un déploiement hors environnement de confiance est un jour requis. |
| **Phase 4 — Extensions** | Index vectoriel (embeddings) pour recherche sémantique, index full-text, algorithmes analytiques (via délégation à un moteur externe ou implémentation native), rebuild incrémental de l'index. |

---

## 13. Questions ouvertes (à trancher avant/pendant l'implémentation)

1. **Migration de schéma incompatible** : processus détaillé non spécifié
   (§3.5) — laissé volontairement en `TBD`, à traiter au moment où le
   besoin se présente plutôt qu'anticipé dans ce cadrage.

> **Mis de côté (décision de scope, pas un TBD)** : index vectoriel
> (embeddings) et index full-text — confirmés hors scope v1, repoussés en
> Phase 4 si le besoin se confirme. Indexation v1 = topologique + propriété
> uniquement (§5.2). Voir §12.
>
> **Résolu** : réplication / haute disponibilité (anciennement point 1) —
> répliques indépendantes sans consensus (pas de Raft), chaque réplique
> reconstruit le même index depuis le même snapshot Iceberg ; possible
> uniquement parce que le moteur est lecture seule avec état dérivable.
> Voir §6.4.
>
> **Résolu** : rebalancement du partitionnement (anciennement point 1) —
> sur-partitionnement fixe découplé du nombre de machines ; rebalancer =
> réaffecter des partitions logiques existantes à d'autres machines, pas
> rehasher. Voir §6.2.
>
> **Résolu** : génération de `node_id` en double mode (anciennement
> point 1) — fourni explicitement (hachage d'une clé métier stable) si la
> source en a une, sinon généré par le pipeline. Voir §3.3.
>
> **Résolu** : label unique par nœud, pas de multi-label (anciennement
> point 2) — le besoin de facettes multiples se modélise par une arête
> dédiée entre nœuds distincts. Voir §3.1.
>
> **Résolu** : la cible de déploiement (anciennement point 1) est actée
> sur Kubernetes — voir §10.
>
> **Différé explicitement (pas un TBD bloquant)** : les objectifs de
> performance chiffrés (latence p99, throughput, taille de graphe
> maximale) ne sont volontairement pas fixés à ce stade du cadrage — ils
> seront définis en Phase 3, une fois un premier MVP mesurable disponible
> (benchmarking sur données réelles plutôt que cibles théoriques a priori).
>
> **Mis de côté (décision de scope, pas un TBD)** : la sécurité
> (AuthN/AuthZ) est explicitement hors scope v1 — voir §1.4, §8.3.

> **Résolu** : l'exécution distribuée multi-partitions (anciennement point
> 1 ci-dessus) est actée en scatter-gather piloté par le coordinateur —
> voir §7.4.
