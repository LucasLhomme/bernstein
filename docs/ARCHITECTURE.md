# Bernstein — Architecture & Justifications Techniques

Ce document constitue le dossier d'architecture technique et de justification des choix d'ingénierie pour le déploiement Kubernetes de la **Poll Application** (Projet Bernstein, G-DOP-500).

---

## Sommaire

1. [Architecture Globale & Flux Applicatif](#1-architecture-globale--flux-applicatif)
2. [Topologie du Cluster & Nombre de Nœuds](#2-topologie-du-cluster--nombre-de-nœuds)
3. [Politiques d'Affinité, Anti-Affinité & Disruption Budgets](#3-politiques-daffinité-anti-affinité--disruption-budgets)
4. [Justification Détaillée du Nombre de Réplicas](#4-justification-détaillée-du-nombre-de-réplicas)
5. [Stratégie Réseau & Load Balancing](#5-stratégie-réseau--load-balancing)
6. [Autoscaling (HPA, Worker, Cluster Autoscaler)](#6-autoscaling-hpa-worker-cluster-autoscaler)
7. [Politiques de Redémarrage (Restart Policies) & Cycle de Vie](#7-politiques-de-redémarrage-restart-policies--cycle-de-vie)
8. [Allocation des Ressources & Classes de Qualité de Service (QoS)](#8-allocation-des-ressources--classes-de-qualité-de-service-qos)
9. [Persistance des Données & Stratégies de Déploiement](#9-persistance-des-données--stratégies-de-déploiement)
10. [Sondes de Santé (Probes) & Déploiement sans Coupure (Zero-Downtime)](#10-sondes-de-santé-probes--déploiement-sans-coupure-zero-downtime)

---

## 1. Architecture Globale & Flux Applicatif

L'application est découpée en microservices asynchrones reliés par une file de messages en mémoire et une base relationnelle :

```text
[ Votant (Navigateur) ]
          │ (HTTP POST vote)
          ▼
   [ Service: poll ] (type: LoadBalancer, Port 80)
          │
    [ Pods poll ] (x2..10 via HPA, Python/Flask + gunicorn)
          │
          │ RPUSH votes
          ▼
   [ Service: redis ] (type: ClusterIP, Port 6379)
          │
    [ Pod redis ] (x1 STRICT, redis:8-alpine)
          ▲
          │ BLPOP votes
   [ Pod worker ] (x1, Java 25 / Jedis)
          │
          │ UPSERT INTO votes
          ▼
 [ Service: postgres ] (type: ClusterIP, Port 5432)
          │
   [ Pod postgres ] (x1 STRICT, postgres:15-alpine, PVC ReadWriteOnce)
          ▲
          │ SELECT COUNT(id) GROUP BY vote (toutes les secondes)
   [ Pods result ] (x2..10 via HPA, Node.js + Socket.IO)
          ▲
          │ HTTP long-polling Socket.IO (SessionAffinity: ClientIP)
   [ Service: result ] (type: LoadBalancer, Port 80, externalTrafficPolicy: Local)
          │
[ Utilisateur (Navigateur - Dashboard en temps réel) ]
```

---

## 2. Topologie du Cluster & Nombre de Nœuds

### 2.1. Control Plane vs Worker Nodes

| Rôle | Composants hébergés | Fonction | Isolation & Taints |
| :--- | :--- | :--- | :--- |
| **Control Plane** | `kube-apiserver`, `etcd`, `kube-scheduler`, `kube-controller-manager` | Cerveau du cluster. Maintient l'état désiré, orchestre les pods et gère les événements. | Sur un cluster auto-géré, protégé par une **Taint** (`node-role.kubernetes.io/control-plane:NoSchedule`). Sur AKS, le Control Plane est entièrement géré par Azure et n'apparaît même pas dans `kubectl get nodes`. |
| **Worker Nodes** | `kubelet`, `kube-proxy`, Container Runtime (`containerd`) | Exécutent les conteneurs applicatifs et gèrent le routage réseau local. | Reçoivent toutes nos charges de travail. |

### 2.2. Pourquoi plusieurs nœuds ?

Un cluster à nœud unique est un **SPOF** (*Single Point of Failure*) :

1. **Haute disponibilité** : si un nœud tombe (panne matérielle, crash d'hyperviseur), les pods `poll` et `result` présents sur l'autre nœud continuent de servir le trafic.
2. **Maintenance sans coupure** : on peut vider un nœud (`kubectl drain`) pour le mettre à jour pendant que l'autre sert le trafic.
3. **L'anti-affinité n'a de sens qu'avec ≥ 2 nœuds** : avec un seul nœud, nos 2 réplicas finissent forcément sur la même machine.

### 2.3. Pourquoi 2 nœuds (et pas 1 ou 5) : le calcul

Le scheduler place les pods en fonction de leurs **`requests`** (cf. §8). Somme des `requests` de l'application :

| Composant | CPU / pod | Mémoire / pod | Au repos (HPA min) | Au maximum (HPA max) |
| :--- | :---: | :---: | :---: | :---: |
| `poll` | 100m | 128Mi | ×2 → 200m / 256Mi | ×10 → 1000m / 1280Mi |
| `result` | 100m | 128Mi | ×2 → 200m / 256Mi | ×10 → 1000m / 1280Mi |
| `worker` | 100m | 256Mi | ×1 → 100m / 256Mi | ×1 → 100m / 256Mi |
| `redis` | 200m | 128Mi | ×1 → 200m / 128Mi | ×1 → 200m / 128Mi |
| `postgres` | 250m | 512Mi | ×1 → 250m / 512Mi | ×1 → 250m / 512Mi |
| **Total application** | | | **≈ 0,95 vCPU / 1,4 Gi** | **≈ 2,55 vCPU / 3,4 Gi** |

À cela s'ajoutent les pods système d'AKS (CoreDNS, metrics-server, kube-proxy, CSI…) : environ 0,5 vCPU / 0,6 Gi.

Un nœud **Standard_B2s** (2 vCPU / 4 Go, le moins cher utilisable sur AKS) offre environ **1,9 vCPU / 2,5–2,9 Gi allouables** (Azure réserve une partie pour le kubelet et le système ; valeur exacte via `kubectl describe nodes`, section `Allocatable`).

* **Au repos** (~1,5 vCPU / ~2 Gi) : tout tiendrait sur 1 nœud, mais sans aucune haute disponibilité → **minimum 2 nœuds**.
* **Au maximum de l'HPA** (~3,05 vCPU / ~4 Gi) : 2 nœuds (~3,8 vCPU / ~5 Gi) suffisent encore.
* **Au-delà de 2** : on paierait des machines vides la plupart du temps. Le **Cluster Autoscaler** (§6.3) ajoute un 3ᵉ nœud seulement si des pods restent `Pending`.

**Conclusion : 2 nœuds `Standard_B2s`, Cluster Autoscaler entre 2 et 3.** C'est le minimum pour la haute disponibilité, et c'est suffisant pour le pire cas de l'autoscaling. Cela respecte aussi la consigne de gestion des coûts du sujet.

```bash
# Vérifier le nombre de nœuds et leur capacité
kubectl get nodes
kubectl describe nodes | grep -A 6 Allocatable

# Passer un cluster AKS existant à 2 nœuds, avec autoscaling 2 → 3
az aks update -g <resource-group> -n <cluster> --enable-cluster-autoscaler --min-count 2 --max-count 3
```

---

## 3. Politiques d'Affinité, Anti-Affinité & Disruption Budgets

Avoir plusieurs nœuds ne garantit rien si le `kube-scheduler` place toutes les répliques d'un service sur la même machine. Les **nœuds** sont les machines ; l'affinité décide **sur quelle machine** chaque pod atterrit.

### 3.1. Pod Anti-Affinity sur `poll` et `result`

```yaml
affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchExpressions:
              - key: app
                operator: In
                values:
                  - poll # ou 'result'
          topologyKey: "kubernetes.io/hostname"
```

* **`topologyKey: kubernetes.io/hostname`** : le « domaine » à éviter est le nœud. Deux pods `poll` évitent d'être sur le même nœud.
* **Pourquoi `preferred` plutôt que `required` ?**
  * `required` (règle stricte) : avec 2 nœuds et un HPA qui monte à 10 pods, le 3ᵉ pod `poll` ne trouverait aucun nœud « libre de poll » et resterait **`Pending`** à vie. Même chose pendant un rolling update (`maxSurge: 1` ⇒ 3 pods temporaires) ou si un nœud est en maintenance.
  * `preferred` (poids 100) : le scheduler sépare les pods dès que c'est possible (les 2 premiers sont sur 2 nœuds différents), mais accepte d'en empiler quand il n'y a pas le choix.
* **`IgnoredDuringExecution`** : la règle n'est évaluée qu'au placement. Un pod déjà lancé n'est jamais déplacé (si un nœud revient après une panne, les pods ne se « rééquilibrent » pas seuls).

### 3.2. Pourquoi pas d'affinité sur `redis`, `postgres` et `worker` ?

* Ils n'ont qu'un réplica : l'anti-affinité n'a rien à séparer.
* **Pas de `nodeAffinity`** : nos nœuds sont tous identiques (même taille, même pool). Il n'y a aucun nœud « spécial » à cibler. En production, on pourrait épingler `postgres` sur un pool de nœuds à disques rapides (`nodeSelector: disktype: ssd`).
* **`podAffinity` worker → redis** (les rapprocher sur le même nœud) : envisagé, puis écarté. Le gain de latence est négligeable pour quelques votes par seconde, et cela ajoute une contrainte au scheduler.

### 3.3. PodDisruptionBudget (PDB) sur `poll` et `result`

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: poll   # ou 'result'
```

Un PDB protège contre les **interruptions volontaires** : `kubectl drain`, mise à jour des nœuds AKS, Cluster Autoscaler qui retire un nœud. Kubernetes **refuse d'évincer** un pod si cela laisse moins de `minAvailable` pods prêts. Le drain attend alors qu'un remplaçant soit prêt ailleurs.

* **Pas de PDB sur `redis` / `postgres`** : avec un seul réplica, `minAvailable: 1` bloquerait **tout** drain indéfiniment (le nœud ne pourrait jamais être vidé). On accepte une courte interruption de ces deux composants lors d'une maintenance (cf. §4).
* Un PDB ne protège **pas** contre une panne brutale de nœud (interruption involontaire) : c'est le rôle des réplicas + anti-affinité.

---

## 4. Justification Détaillée du Nombre de Réplicas

| Composant | Réplicas | Justification Technique & Risques |
| :--- | :---: | :--- |
| **`poll`** | **2 (min) à 10** | **Sans état (stateless)**. 2 minimum : un pod par nœud (haute dispo) et zéro-downtime pendant un `RollingUpdate`. Le champ `replicas` est **volontairement absent** du Deployment : c'est le HPA qui décide. S'il était fixé à 2, chaque `kubectl apply` ramènerait brutalement le service à 2 pods, même en pleine montée en charge. |
| **`result`** | **2 (min) à 10** | **Sans état**. Même principe que `poll`. Chaque pod interroge PostgreSQL toutes les secondes et pousse les scores à ses clients Socket.IO. |
| **`worker`** | **1** | **Consommateur de file**. Un seul worker traite largement des centaines de votes par seconde. Le scaler serait **sans danger** : `BLPOP` est atomique (un vote n'est livré qu'à un seul worker) et l'écriture est un `UPSERT` idempotent. Mais ce n'est pas utile à notre échelle (cf. §6.2). |
| **`redis`** | **STRICTEMENT 1** | ⚠️ **Redis ne doit pas être répliqué dans cette configuration.** Deux pods Redis seraient deux bases **indépendantes** (aucune réplication entre elles), avec des données différentes. Le Service fait du round-robin : `poll` écrirait dans le Pod A et `worker` lirait le Pod B. Des votes resteraient bloqués indéfiniment, jamais comptés. Pour cette raison, le Deployment utilise aussi **`strategy: Recreate`** : lors d'une mise à jour, l'ancien pod est supprimé **avant** la création du nouveau. Il n'existe donc jamais deux Redis simultanément, même quelques secondes. Répliquer Redis demanderait *Redis Sentinel* ou *Redis Cluster*. |
| **`postgres`** | **STRICTEMENT 1** | Base relationnelle avec un disque **ReadWriteOnce** (attachable à un seul nœud). Plusieurs pods PostgreSQL sur des données partagées les corrompraient. Une base répliquée demanderait un opérateur dédié (Patroni, CloudNativePG) avec réplication primaire/secondaire. |

---

## 5. Stratégie Réseau & Load Balancing

### 5.1. Découpage Réseau : `ClusterIP` vs `LoadBalancer`

1. **Services internes (`ClusterIP`) — `postgres` & `redis`** : aucune IP externe. Les ports 5432 et 6379 ne sont joignables que depuis l'intérieur du cluster. Il n'y a aucune surface d'attaque depuis Internet sur la base ou la file.
2. **Services externes (`LoadBalancer`) — `poll` & `result`** : Kubernetes demande au cloud (Azure Load Balancer) une IP publique par service. Le Load Balancer Azure répartit les connexions entre les nœuds ; `kube-proxy` les répartit ensuite entre les pods.
3. **`worker`** : aucun Service. Personne ne l'appelle, c'est lui qui va chercher le travail dans Redis.

### 5.2. Socket.IO & affinité de session sur `result`

Le client de `result` force le transport **HTTP long-polling** (`io.connect({transports:['polling']})` dans `result/views/app.js`). Il n'y a **pas de WebSocket** : le navigateur envoie une suite de requêtes HTTP qui portent toutes le même identifiant de session Socket.IO.

* **Le problème** : avec 2 réplicas et une répartition aléatoire, la requête n°1 arrive sur le Pod 1 (qui crée la session) et la n°2 sur le Pod 2. Le Pod 2 ne connaît pas cette session et répond `HTTP 400 "Session ID unknown"`, d'où des déconnexions en boucle.
* **La solution** : une affinité de session par IP client, plus une politique de trafic externe `Local`.

  ```yaml
  spec:
    type: LoadBalancer
    externalTrafficPolicy: Local
    sessionAffinity: ClientIP
    sessionAffinityConfig:
      clientIP:
        timeoutSeconds: 10800
  ```

* **Pourquoi `externalTrafficPolicy: Local` ?**
  * Par défaut (`Cluster`), le Load Balancer envoie le trafic à **n'importe quel nœud**, qui peut le **rebondir** vers un pod d'un autre nœud. Ce rebond remplace l'IP du client par celle du nœud (SNAT). L'affinité `ClientIP` peut alors se baser sur une mauvaise IP, et chaque nœud tient sa propre table d'affinité.
  * Avec `Local`, le Load Balancer n'envoie le trafic **qu'aux nœuds qui hébergent un pod `result`** (il le vérifie via une sonde de santé dédiée), sans rebond. L'**IP réelle du client est conservée** et l'affinité devient fiable. On évite aussi un saut réseau.
  * Contrepartie : la charge est répartie par nœud et non par pod. Avec l'anti-affinité (1 pod par nœud au repos), l'équilibre reste bon.

### 5.3. Pourquoi pas d'Ingress Controller (L7) ?

| | Service `LoadBalancer` (L4, notre choix) | Ingress Controller (L7) |
| :--- | :--- | :--- |
| IP publiques | 1 par service exposé (ici 2) | 1 seule pour tout le cluster |
| Routage | Par IP/port | Par nom d'hôte ou chemin d'URL (`/`, `/result`) |
| TLS / HTTPS | À gérer dans chaque appli | Centralisé (cert-manager + Let's Encrypt) |
| Affinité | Par IP client | Par cookie (plus fiable derrière un NAT) |
| Complexité | Natif Kubernetes, rien à installer | Un composant de plus à installer, sécuriser et mettre à jour |

Avec **seulement 2 applications exposées, en HTTP, sans nom de domaine**, le Service `LoadBalancer` est le choix le plus simple et le plus robuste. Le coût d'une 2ᵉ IP publique reste marginal.

Un Ingress deviendrait pertinent avec plus de services, un nom de domaine et HTTPS. Le projet historique *ingress-nginx* a été mis à la retraite en mars 2026. Aujourd'hui on partirait plutôt sur la **Gateway API** (Envoy Gateway, Traefik, ou la passerelle managée d'AKS).

---

## 6. Autoscaling (HPA, Worker, Cluster Autoscaler)

Il existe trois niveaux d'autoscaling dans Kubernetes. Voici ce que nous utilisons et pourquoi.

### 6.1. Horizontal Pod Autoscaler (HPA) sur `poll` et `result`

Défini dans [`k8s/hpa.yaml`](../k8s/hpa.yaml). Le contrôleur HPA lit la consommation CPU des pods via le `metrics-server` (installé par défaut sur AKS) toutes les 15 secondes, puis applique :

$$\text{Réplicas désirés} = \left\lceil \text{Réplicas actuels} \times \frac{\text{Utilisation actuelle}}{\text{Utilisation cible}} \right\rceil$$

* **Cible : 70 % du CPU *demandé*** (`requests: 100m`), soit environ 70m par pod. 70 % laisse une marge pour absorber un pic pendant que les nouveaux pods démarrent.
* **Bornes** : minimum 2 (haute dispo, zéro-downtime), maximum 10 (tient sur 2 nœuds, cf. §2.3, et protège PostgreSQL d'un trop grand nombre de connexions de `result`).
* **Pourquoi uniquement le CPU, pas la mémoire ?** Python et Node.js libèrent mal leur mémoire après un pic. Un HPA sur la mémoire monterait mais ne redescendrait jamais.
* **`behavior`** (vitesse de réaction) :
  * `scaleUp` : réaction immédiate, jusqu'à doubler le nombre de pods toutes les 15 s.
  * `scaleDown` : 5 minutes de stabilisation, puis 1 pod retiré par minute. On évite le « yo-yo » (supprimer des pods qu'il faudrait recréer 30 s plus tard).

```bash
kubectl get hpa -w          # observer en direct
kubectl top pods            # consommation réelle (metrics-server)
```

### 6.2. Et le `worker` ? (autoscaling « événementiel »)

Un HPA sur le CPU serait **inutile** pour le worker. Quand 10 000 votes attendent dans Redis, le worker reste un simple processus qui enchaîne `BLPOP` → `UPSERT`. Il est limité par les allers-retours réseau, pas par le CPU, qui reste bas. Le HPA ne verrait jamais de surcharge.

La bonne métrique est la **longueur de la file `votes`** dans Redis. Kubernetes ne la connaît pas nativement. La solution standard est **KEDA** (*Kubernetes Event-Driven Autoscaling*), qui lit la longueur de la liste Redis et pilote un HPA :

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
spec:
  scaleTargetRef: { name: worker }
  minReplicaCount: 1
  maxReplicaCount: 5
  triggers:
    - type: redis
      metadata: { address: redis.default.svc.cluster.local:6379, listName: votes, listLength: "20" }
```

**Choix : non déployé.** À notre volume (quelques votes par seconde), la file est vide en permanence et KEDA ajouterait un opérateur entier à installer et maintenir, sans aucun gain. Si le trafic justifiait plusieurs workers, ce serait la première chose à ajouter. Le worker est prêt pour cela (cf. §4).

### 6.3. Cluster Autoscaler (niveau nœuds)

Le HPA ajoute des **pods**. Si les nœuds sont pleins, les nouveaux pods restent `Pending`. Le **Cluster Autoscaler** d'AKS détecte ces pods `Pending` et **ajoute une VM** au pool de nœuds. À l'inverse, il retire un nœud sous-utilisé depuis environ 10 minutes, en respectant les PDB (§3.3).

```bash
az aks update -g <resource-group> -n <cluster> --enable-cluster-autoscaler --min-count 2 --max-count 3
```

Bornes 2 → 3 : 2 pour la haute dispo (§2.3), 3 comme marge de sécurité tout en plafonnant la facture.

### 6.4. Vertical Pod Autoscaler (VPA) : non utilisé

Le VPA ajuste les `requests` / `limits` d'un pod au lieu d'en ajouter. Il est incompatible avec un HPA qui scale sur la même métrique (CPU) : les deux se contrediraient. Il pourrait servir en mode « recommandation seule » pour affiner nos valeurs de ressources.

---

## 7. Politiques de Redémarrage (Restart Policies) & Cycle de Vie

### 7.1. La directive `restartPolicy: Always`

Déclarée **explicitement** dans tous nos manifests (`spec.template.spec.restartPolicy: Always`). La politique s'applique au niveau du Pod et c'est le **kubelet** du nœud qui l'exécute :

* **`Always`** : la seule valeur autorisée pour un `Deployment` (et un `StatefulSet` / `DaemonSet`). Quel que soit le code de sortie du conteneur (0 ou erreur), il est redémarré. C'est logique pour un service censé tourner en permanence.
* **`OnFailure`** : pour les `Job` / `CronJob` (redémarre seulement si le code de sortie ≠ 0).
* **`Never`** : tâches ponctuelles à ne jamais relancer.

Exemples concrets dans notre application :

* Le `worker` fait `System.exit(1)` s'il perd la connexion à la base. Avant de quitter, il remet le vote en cours dans Redis (`LPUSH`), puis le kubelet le redémarre.
* `result` fait `process.exit(1)` si sa requête SQL échoue. Même mécanisme.

### 7.2. Redémarrage par le conteneur vs recréation par le contrôleur

* **`restartPolicy`** : le kubelet **redémarre le conteneur** dans le même pod, sur le même nœud (le compteur `RESTARTS` augmente).
* **Le contrôleur (Deployment / ReplicaSet)** : si le **pod** disparaît (nœud en panne, pod supprimé, éviction), il **recrée un nouveau pod**, éventuellement sur un autre nœud.

### 7.3. Mécanisme du `CrashLoopBackOff`

Si un conteneur plante en boucle (ex. mauvais mot de passe PostgreSQL), le kubelet espace les redémarrages de façon exponentielle : 10 s, 20 s, 40 s… jusqu'à 5 minutes maximum. Le délai revient à zéro après 10 minutes de fonctionnement normal. Cela évite d'épuiser le CPU du nœud, de saturer les logs et de bombarder la base de tentatives de connexion.

---

## 8. Allocation des Ressources & Classes de Qualité de Service (QoS)

### 8.1. `requests` vs `limits`

* **`requests`** (réservation) : utilisé par le scheduler pour choisir un nœud. La somme des `requests` d'un nœud ne dépasse jamais sa capacité allouable. C'est aussi la base de calcul du HPA (70 % des `requests`).
* **`limits`** (plafond), appliqué par le noyau Linux via les cgroups :
  * **CPU** : au-delà, le conteneur est **bridé (throttling)**. Il ralentit mais n'est pas tué.
  * **Mémoire** : au-delà, le noyau **tue** le conteneur (**OOMKilled**, code de sortie 137), puis il est redémarré (`restartPolicy: Always`).

### 8.2. Valeurs par service

| Composant | Requests (CPU / RAM) | Limits (CPU / RAM) | Classe QoS | Justification |
| :--- | :---: | :---: | :---: | :--- |
| `poll` | 100m / 128Mi | 500m / 256Mi | Burstable | Flask + gunicorn : ~40-60 Mi au repos. Le pic CPU possible (×5) absorbe une rafale de votes pendant que le HPA réagit. |
| `result` | 100m / 128Mi | 500m / 256Mi | Burstable | Node.js : ~50-70 Mi au repos, une requête SQL par seconde. Même logique que `poll`. |
| `worker` | 100m / 256Mi | 500m / **384Mi** | Burstable | **JVM** : ~150-200 Mi au repos (heap + metaspace + threads). Avec 256Mi de limite, risque d'OOMKill au moindre pic. `-XX:MaxRAMPercentage=75` pour que le heap utilise 75 % de la limite (25 % par défaut). |
| `redis` | 200m / 128Mi | 200m / 128Mi | **Guaranteed** | Composant à état unique : s'il est évincé, la file s'arrête. Redis est mono-thread et léger (quelques Mi pour notre file). |
| `postgres` | 250m / 512Mi | 250m / 512Mi | **Guaranteed** | Le composant le plus critique (données persistantes). Le `shared_buffers` par défaut (128 Mo), plus les connexions et le cache, tiennent dans 512Mi. |

### 8.3. Pourquoi deux classes de QoS différentes ?

Kubernetes classe les pods en 3 catégories, qui déterminent **l'ordre d'éviction** quand un nœud manque de mémoire :

1. **BestEffort** (aucune ressource déclarée) : évincés en premier. Nous n'en avons aucun.
2. **Burstable** (`requests` < `limits`) : évincés ensuite.
3. **Guaranteed** (`requests` == `limits`) : évincés en dernier.

* **Applis sans état (`poll`, `result`, `worker`) → Burstable** : elles peuvent dépasser temporairement leur réservation pour absorber un pic. Si l'une est évincée, un autre réplica prend le relais et le Deployment la recrée.
* **Composants à état unique (`redis`, `postgres`) → Guaranteed** : un seul réplica et aucun remplaçant. Ce sont eux qu'il faut protéger en priorité. La contrepartie : ils ne peuvent pas dépasser leur réservation (consommation prévisible).

Ces valeurs sont un point de départ. On les valide sous charge avec `kubectl top pods`, puis on les ajuste.

---

## 9. Persistance des Données & Stratégies de Déploiement

### 9.1. Persistance PostgreSQL (`PersistentVolumeClaim`)

PostgreSQL stocke ses données dans un volume persistant de 1 Gio en mode **`ReadWriteOnce` (RWO)**. Sur AKS, c'est un disque managé Azure.

* `ReadWriteOnce` : le volume n'est monté en écriture que par **un seul nœud** à la fois. C'est le mode des disques cloud blocs (Azure Disk, AWS EBS).
* Le PVC est un objet **indépendant du pod**. Si le pod `postgres` est supprimé ou déplacé, le nouveau pod réattache le même disque et retrouve les votes.
* Variable `PGDATA` vers un sous-dossier :
  ```yaml
  env:
    - name: PGDATA
      value: /var/lib/postgresql/data/pgdata
  ```
  Un disque cloud fraîchement formaté contient un dossier `lost+found` à sa racine. `initdb` refuse d'initialiser un dossier non vide ; le sous-dossier `pgdata` contourne le problème.

### 9.2. `strategy: Recreate` pour PostgreSQL et Redis

* **Le piège du `RollingUpdate` (stratégie par défaut)** : Kubernetes crée le nouveau pod **avant** de supprimer l'ancien.
  * **PostgreSQL** : le nouveau pod tente d'attacher un disque RWO déjà attaché ailleurs, ce qui donne une erreur `Multi-Attach error for volume`. Il reste bloqué.
  * **Redis** : pendant quelques secondes, deux Redis indépendants coexistent et la file est coupée en deux (cf. §4).
* **`strategy: Recreate`** : Kubernetes supprime d'abord l'ancien pod, puis crée le nouveau. Le prix est une courte interruption de ces composants pendant leur mise à jour. Elle est acceptable, car `poll` et `result` gèrent la reconnexion. Une base sans interruption demanderait de la réplication (cf. §4).

---

## 10. Sondes de Santé (Probes) & Déploiement sans Coupure (Zero-Downtime)

### 10.1. Rôles respectifs de `livenessProbe` et `readinessProbe`

* **`livenessProbe`** (« le processus est-il vivant ? ») : en cas d'échecs répétés, Kubernetes **tue et redémarre** le conteneur. Nos endpoints `/healthz` répondent toujours 200 tant que le serveur HTTP tourne. On ne veut pas redémarrer `poll` juste parce que Redis est indisponible : ça ne réglerait rien.
* **`readinessProbe`** (« peut-il recevoir du trafic ? ») : en cas d'échec, le pod n'est **pas redémarré**, mais il est **retiré du Service** (plus aucun trafic). Nos endpoints `/readyz` vérifient la dépendance réelle : `ping` Redis pour `poll`, `SELECT 1` pour `result`, Redis et Postgres pour `worker`.
* `redis` et `postgres` utilisent des sondes `exec` (`redis-cli ping`, `pg_isready`), car ils n'exposent pas d'HTTP.

### 10.2. Implémentation du Zero-Downtime Rolling Update (`poll` et `result`)

```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxSurge: 1
    maxUnavailable: 0
```

1. **`maxUnavailable: 0`** : Kubernetes n'a pas le droit de supprimer un ancien pod tant que son remplaçant n'est pas prêt.
2. **`maxSurge: 1`** : il peut créer un pod en plus pendant la bascule (3 pods temporairement).
3. **La `readinessProbe` est le pivot** : le nouveau pod ne reçoit du trafic que quand `/readyz` renvoie 200. L'ancien pod n'est arrêté (`SIGTERM`) qu'ensuite.

```text
État initial :   [ Pod v1 (Actif) ]  [ Pod v1 (Actif) ]
                        │                   │
Trafic HTTP  ───────────┴───────────────────┘

Étape 1 (Surge) : [ Pod v1 (Actif) ]  [ Pod v1 (Actif) ]  [ Pod v2 (Démarrage... readyz=503) ]
                        │                   │                     (Aucun trafic reçu)
Trafic HTTP  ───────────┴───────────────────┘

Étape 2 (Bascule):[ Pod v1 (Actif) ]  [ Pod v1 (Arrêt) ]  [ Pod v2 (readyz=200 - Actif) ]
                        │                                         │
Trafic HTTP  ───────────┴─────────────────────────────────────────┘

Étape 3 (Terminé):[ Pod v2 (Actif) ]  [ Pod v2 (Actif) ]
                        │                   │
Trafic HTTP  ───────────┴───────────────────┘
```
