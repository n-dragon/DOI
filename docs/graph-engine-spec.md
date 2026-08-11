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
| Agrégation | **Aucune dans le moteur** — délégation en aval (voir §7.1, §1.3) |
| Ingestion | Batch externe (Spark/Flink → Iceberg), pas d'écriture directe via le serveur |
| Cohérence | Éventuelle, snapshot isolation en lecture (time-travel Iceberg) |
| Rafraîchissement d'index | Reconstruction complète périodique (pas d'incrémental) |
| Échelle | Cluster distribué, graphe partitionné |
| Partitionnement | Hash-partitioning par `node_id` |
| Exécution distribuée multi-partitions | **Scatter-gather** orchestré par le coordinateur, hop par hop (voir §7.4) |
| Schéma | Déclaratif, versionné, IDL dédiée, migrations |
| Observabilité | Métriques (Prometheus) + tracing distribué (OpenTelemetry) |
| Sécurité / AuthN-AuthZ | **Hors scope v1** — mis de côté délibérément (voir §1.3, §8.3) |
| Cible de déploiement | Kubernetes (voir §10) |
| Objectifs de performance | Pas de cible chiffrée pour l'instant — décision différée (voir §13) |

### 1.3 Non-objectifs (pour ce scope)

- **AuthN/AuthZ** (§8.3) — mis de côté délibérément pour ce cadrage : pas
  d'authentification client, pas d'autorisation fine, pas d'audit log en
  v1. Décision de scope assumée (le scope est un moteur mono-tenant en
  environnement de confiance), pas un TBD à trancher plus tard — à
  reconsidérer explicitement si une mise en production hors environnement
  de confiance est envisagée un jour.
- Écritures transactionnelles temps réel multi-nœuds/arêtes avec garanties
  ACID fortes (le modèle retenu est snapshot/eventual consistency).
- Algorithmes analytiques globaux lourds (PageRank, détection de communautés)
  en v1 — ils sont repoussés en phase ultérieure (§12) et pourraient être
  délégués à un moteur externe (Spark/DataFusion) plutôt que réimplémentés.
- Mode embarqué (librairie in-process) — hors scope, le moteur est un
  serveur réseau dès la v1.
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

- `node_id` : identifiant unique global, `UInt64` généré par le pipeline
  d'ingestion (ou dérivé d'une clé métier via hachage stable — **TBD**, voir
  §13). Sert de clé de partitionnement (hash-partitioning).
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
  comme extension future (§13) — non retenus dans le cadrage initial en
  dehors de propriété + topologique.

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
- Rebalancement lors d'un changement de `n_partitions` : **TBD** — implique
  a minima un rebuild complet des index sur les partitions affectées
  (cohérent avec la stratégie de rebuild périodique déjà retenue).
- Réplication des partitions pour la haute disponibilité : **TBD** (non
  cadré) — à trancher avant mise en production (facteur de réplication,
  stratégie de failover).

### 6.3 Membership et découverte

- Mécanisme de découverte des nœuds de partition par le(s) coordinateur(s) :
  **intégration native Kubernetes** (API des Pods/Endpoints, ou headless
  Service) — décision alignée sur le choix de cible de déploiement (§10).
  Pas de registre externe (etcd/Consul) dédié en v1.

---

## 7. Moteur de requêtes

### 7.1 Langage de requête (DSL)

- DSL de traversal inspiré de **Cypher/Gremlin**, sous-ensemble initial
  centré sur les deux opérations prioritaires actées :
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
- Hors scope v1 (noté explicitement, cf. §1.3 et §12) : `shortest path`,
  algorithmes analytiques globaux.
- **Pas d'agrégation** : le DSL ne comporte volontairement **aucune**
  fonction d'agrégation (`COUNT`, `SUM`, `AVG`, `MIN`/`MAX`, `COLLECT`) ni
  clause `GROUP BY`. `RETURN` ne fait que **projeter** les nœuds/arêtes/
  propriétés matchés par le `MATCH`, sans les réduire ni les regrouper —
  décision actée (§1.3), pas un TBD. Un besoin d'agrégation se traite en
  aval de la réponse streamée du moteur (§7.5), côté client ou via un
  moteur externe (DataFusion/Spark) sur les résultats bruts exportés.
- Grammaire formelle complète, gestion des alias, clauses `ORDER BY` /
  `LIMIT` / pagination des résultats : **TBD**, à détailler dans une
  spec de langage dédiée avant implémentation du parser.

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

### 7.4 Exécution distribuée multi-partitions — **[Décidé : scatter-gather]**

**Décision actée :** le moteur exécute les traversals via un modèle
**scatter-gather orchestré par le coordinateur**, hop par hop. L'alternative
message-passing pair-à-pair (style Pregel) est écartée pour la v1 —
simplicité de raisonnement, d'observabilité et de debugging privilégiées sur
l'efficacité réseau théorique d'un modèle sans coordinateur.

#### 7.4.1 Déroulement d'une requête

1. **Résolution des nœuds de départ** : le coordinateur résout les nœuds
   matchant le premier `MATCH (... {props})` via l'**index de propriété**
   (§5.2), en interrogeant les partitions concernées. Résultat : un ensemble
   `frontier_0` de `(partition_id, node_id)`.
2. **Boucle de hops** (répétée jusqu'à profondeur max de la requête, ex.
   `*1..3`, ou frontière vide) : pour `frontier_i`,
   - le coordinateur **groupe** les nœuds de la frontière par
     `partition_id` (routage direct via `hash(node_id)`, pas de lookup),
   - envoie **un seul appel batché** par partition concernée (`ExpandHop`,
     §7.4.2) contenant tous les `node_id` locaux à explorer + le filtre de
     type d'arête/direction + le prédicat `WHERE` à pousser,
   - chaque partition exécute l'expansion **localement** via son index
     topologique (§5.1), applique le pushdown de filtre, et renvoie les
     voisins matchés (avec leurs propriétés nécessaires à `WHERE`/`RETURN`),
   - le coordinateur **fusionne** les réponses de toutes les partitions en
     `frontier_{i+1}`.
3. **Terminaison** : à la profondeur max atteinte (ou frontière vide avant),
   le coordinateur déclenche la **projection finale** (`RETURN`) sur les
   nœuds/arêtes retenus et **streame** les résultats au client (§7.5).

Pour le **pattern matching** (plusieurs `MATCH` combinés), chaque "jambe" du
motif est exécutée comme une traversal scatter-gather indépendante, puis les
résultats sont **joints côté coordinateur** sur les variables partagées
(ex: `o` dans l'exemple §7.1). *Le détail de la stratégie de join (hash-join
en mémoire côté coordinateur vs join distribué) reste à approfondir — non
bloquant pour un premier plan d'exécution naïf, cf. §7.3.*

#### 7.4.2 Protocole réseau

- Un appel `ExpandHop(partition_id, node_ids: [...], edge_filter, where_pushdown) -> [(node, matched_edges)]`
  par partition et par hop, exécuté **en parallèle** (fan-out asynchrone
  depuis le coordinateur) plutôt qu'un appel par nœud — l'unité de
  granularité réseau est **le hop × la partition**, pas le nœud.
- Porté par le protocole choisi en §8.1 (candidat gRPC/`tonic`, requête
  unaire par hop suffisante — le streaming §7.5 s'applique à la réponse
  finale, pas aux hops intermédiaires).

#### 7.4.3 Déduplication et cycles

- Le coordinateur maintient un **visited-set** `(node_id)` par requête (côté
  coordinateur, en mémoire, portée = durée de vie de la requête) pour :
  - éviter de ré-explorer un nœud déjà visité à un hop précédent dans une
    même requête (obligatoire — le graphe n'est pas garanti acyclique),
  - dédupliquer la frontière avant de la router vers les partitions (un même
    nœud atteint via plusieurs chemins n'est envoyé qu'une fois par hop).
- Taille du visited-set non bornée en théorie sur un graphe très connecté à
  grande profondeur — **TBD** : nécessite une limite/quota mémoire par
  requête (cf. §7.4.5).

#### 7.4.4 Tolérance aux pannes

- Si une partition ne répond pas (timeout) ou échoue pendant un `ExpandHop` :
  **TBD** — deux politiques possibles à trancher :
  - échec de la requête entière (fail-fast, résultat cohérent mais
    disponibilité moindre),
  - résultat **partiel** avec avertissement au client (disponibilité, mais
    résultats potentiellement incomplets sans que ce soit vérifiable côté
    client).
- Nombre de retries, backoff, timeout par hop : **TBD**.
- Lié à §6.2 (réplication) : sans réplication des partitions, toute panne
  d'un nœud de partition est nécessairement bloquante pour les requêtes
  touchant sa portion du graphe.

#### 7.4.5 Limites et backpressure

- **TBD** : une frontière peut croître exponentiellement avec la profondeur
  (`*1..3` sur un nœud à fort degré). Nécessaire de définir :
  - une taille max de frontière par hop (au-delà, requête rejetée ou
    tronquée avec avertissement),
  - un budget mémoire par requête au niveau du coordinateur,
  - potentiellement un `LIMIT` appliqué **pendant** l'expansion plutôt
    qu'après (cohérent avec le pushdown déjà retenu en §7.3), une fois la
    clause `LIMIT` elle-même spécifiée (actuellement TBD, §7.1).

#### 7.4.6 Conséquences sur l'observabilité (§9)

Cette décision rend concrètes les métriques déjà prévues en §9.1 :
nombre de hops, taux de références distantes (cross-partition) — chaque hop
scatter-gather correspond exactement à un round-trip coordinateur ↔
partitions concernées, directement mesurable et traçable (span par hop dans
la trace OpenTelemetry, §9.2).

**Impact sur l'architecture (§6.1) :** cette décision fait du coordinateur
un composant à part entière du **chemin de requête** (pas seulement un
point d'entrée réseau) — chaque hop transite par lui, ce qui en fait
potentiellement un point chaud à dimensionner/scaler indépendamment des
nœuds de partition.

Le modèle **message-passing pair-à-pair (style Pregel)** — où chaque nœud de
partition relaierait directement les hops sortants vers la partition cible
sans repasser par le coordinateur — reste documenté comme évolution possible
de Phase 2+ si des traversées plus profondes ou un débit plus élevé
l'exigeaient, mais n'est pas retenu pour v1 : la complexité additionnelle
(terminaison distribuée, agrégation finale, observabilité) n'est pas
justifiée pour les profondeurs bornées ciblées (`*1..3`, §7.1).

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

1. **Génération de `node_id`** : généré par le pipeline d'ingestion vs
   dérivé d'une clé métier par hachage stable — impacte l'idempotence des
   réingestions.
2. **Rebalancement du partitionnement** en cas de changement du nombre de
   partitions.
3. **Réplication / haute disponibilité** des nœuds de partition.
4. **Migration de schéma incompatible** : processus détaillé non spécifié
   (§3.5).
5. **Index vectoriel / embeddings et full-text** : évoqués comme pertinents
   pour un knowledge graph mais explicitement repoussés hors du cadrage
   initial (indexation retenue = topologique + propriété uniquement) —
   à réévaluer en Phase 4.
6. **Protocole de transport de l'API** (§8.1) : gRPC proposé par défaut,
   non confirmé.
7. **Partition spec Iceberg** (§4.1) : alignement du partitionnement
   physique des tables avec le partitionnement logique du cluster, non
   tranché.
8. **Stratégie de join pour le pattern matching multi-jambes** (§7.4.1) :
   hash-join en mémoire côté coordinateur vs join distribué — non bloquant
   pour un premier plan d'exécution naïf, mais à approfondir.
9. **Politique de tolérance aux pannes du scatter-gather** (§7.4.4) :
   fail-fast vs résultats partiels en cas de timeout/échec d'une partition
   pendant un hop ; nombre de retries et backoff.
10. **Limites/backpressure de frontière** (§7.4.5) : taille max de
    frontière par hop, budget mémoire par requête côté coordinateur, et
    lien avec une clause `LIMIT` du DSL (elle-même non encore spécifiée,
    §7.1).

> **Résolu** : label unique par nœud, pas de multi-label — le besoin de
> facettes multiples se modélise par une arête dédiée entre nœuds distincts.
> Voir §3.1.
>
> **Résolu** : la cible de déploiement est actée sur Kubernetes — voir §10.
>
> **Résolu** : l'exécution distribuée multi-partitions est actée en
> scatter-gather piloté par le coordinateur — voir §7.4.
>
> **Différé explicitement (pas un TBD bloquant)** : les objectifs de
> performance chiffrés (latence p99, throughput, taille de graphe
> maximale) ne sont volontairement pas fixés à ce stade du cadrage — ils
> seront définis en Phase 3, une fois un premier MVP mesurable disponible
> (benchmarking sur données réelles plutôt que cibles théoriques a priori).
>
> **Mis de côté (décision de scope, pas un TBD)** : la sécurité
> (AuthN/AuthZ) est explicitement hors scope v1 — voir §1.3, §8.3.
