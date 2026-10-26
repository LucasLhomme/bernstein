# Bernstein — Architecture & Justifications Techniques

Ce document constitue le dossier d'architecture technique et de justification des choix d'ingénierie pour le déploiement Kubernetes de la **Poll Application** (Projet Bernstein, G-DOP-500).

---

## Sommaire

1. [Architecture Globale & Flux Applicatif](#1-architecture-globale--flux-applicatif)
2. [Topologie du Cluster & Notion de Nœuds (Multi-Nodes)](#2-topologie-du-cluster--notion-de-nœuds-multi-nodes)
3. [Politiques d'Affinité & Anti-Affinité (Pod & Node Affinity)](#3-politiques-daffinité--anti-affinité-pod--node-affinity)
4. [Justification Détaillée du Nombre de Réplicas](#4-justification-détaillée-du-nombre-de-réplicas)
5. [Stratégie Réseau & Load Balancing (L4 / L7 / Ingress / Sticky Sessions)](#5-stratégie-réseau--load-balancing-l4--l7--ingress--sticky-sessions)
6. [Autoscaling Horizontal des Pods (HPA) & Cluster Autoscaling](#6-autoscaling-horizontal-des-pods-hpa--cluster-autoscaling)
7. [Politiques de Redémarrage (Restart Policies) & Cycle de Vie](#7-politiques-de-redémarrage-restart-policies--cycle-de-vie)
8. [Allocation des Ressources & Classes de Qualité de Service (QoS)](#8-allocation-des-ressources--classes-de-qualité-de-service-qos)
9. [Persistance des Données & Stratégies de Déploiement](#9-persistance-des-données--stratégies-de-déploiement)
10. [Sondes de Santé (Probes) & Déploiement sans Coupure (Zero-Downtime)](#10-sondes-de-santé-probes--déploiement-sans-coupure-zero-downtime)

---

## 1. Architecture Globale & Flux Applicatif

L'application est découpée en microservices asynchrones reliés par un bus de messages en mémoire et une base relationnelle :

```text
[ Votant (Navigateur) ]
          │ (HTTP POST vote)
          ▼
   [ Service: poll ] (type: LoadBalancer, Port 80)
          │
    [ Pods poll ] (x2..10 via HPA, Python/Flask)
          │
          │ RPUSH votes
          ▼
   [ Service: redis ] (type: ClusterIP, Port 6379)
          │
    [ Pod redis ] (x1 STRICT, Redis 8/Alpine)
          ▲
          │ BLPOP votes
   [ Pod worker ] (x1, Java 25 / Jedis)
          │
          │ UPSERT INTO votes
          ▼
 [ Service: postgres ] (type: ClusterIP, Port 5432)
          │
   [ Pod postgres ] (x1 STRICT, PostgreSQL 15, PVC ReadWriteOnce)
          ▲
          │ SELECT COUNT(*) GROUP BY vote
   [ Pods result ] (x2..10 via HPA, Node.js + Socket.IO)
          ▲
          │ WebSockets / HTTP (SessionAffinity: ClientIP)
   [ Service: result ] (type: LoadBalancer, Port 80)
          │
[ Utilisateur (Navigateur - Dashboard en temps réel) ]
```

---

## 2. Topologie du Cluster & Notion de Nœuds (Multi-Nodes)

Un cluster Kubernetes de production sépare physiquement et logiquement le plan de contrôle (*Control Plane*) des nœuds d'exécution (*Worker Nodes*).

### 2.1. Control Plane vs Worker Nodes

| Rôle | Composants hébergés | Fonction | Isolation & Taints |
| :--- | :--- | :--- | :--- |
| **Control Plane** | `kube-apiserver`, `etcd`, `kube-scheduler`, `kube-controller-manager` | Cerveau du cluster. Maintient l'état désiré, orchestre les pods et gère les événements. | Protégé par une **Taint** (`node-role.kubernetes.io/control-plane:NoSchedule`). Aucun pod applicatif n'y est planifié. Sur les clouds managés (AKS, EKS, GKE), le Control Plane est entièrement infogéré et hautement disponible. |
| **Worker Nodes** | `kubelet`, `kube-proxy`, Container Runtime (`containerd`) | Exécute les conteneurs applicatifs et gère le routage réseau local. | Reçoit les charges de travail applicatives définies par les déploiements. |

### 2.2. Justification du Cluster Multi-Nodes (Minimum 2 à 3 Workers)

Un cluster à nœud unique représente un **SPOF** (*Single Point of Failure*) matériel et logiciel. Le choix d'une architecture multi-nœuds (2 à 3 nœuds workers) répond à trois impératifs :

1. **Haute Disponibilité (High Availability - HA)** :
   Si un nœud worker subit une défaillance physique, un crash d'hyperviseur ou un redémarrage pour mise à jour de l'OS (`kubectl drain`), les pods hébergés sur les nœuds restants continuent de servir le trafic sans interruption.
2. **Tolérance aux pannes planifiées** :
   Permet d'effectuer des opérations de maintenance (rotation des nœuds, montée de version de Kubernetes) nœud par nœud sans impacter la disponibilité des services web.
3. **Répartition de charge matérielle** :
   Distribution équilibrée des allocations CPU et mémoire entre plusieurs machines pour éviter la saturation d'un bus mémoire ou d'une interface réseau.

---

## 3. Politiques d'Affinité & Anti-Affinité (Pod & Node Affinity)

La simple présence de plusieurs nœuds ne garantit pas la haute disponibilité si le `kube-scheduler` place accidentellement toutes les répliques d'un service sur la même machine.

### 3.1. Pod Anti-Affinity sur `poll` et `result`

Pour garantir la résilience, nous configurons une règle de **Pod Anti-Affinity** basée sur le label applicatif et la topologie d'hôte :

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

* **Pourquoi `preferredDuringScheduling` plutôt que `requiredDuringScheduling` ?**
  * `requiredDuringScheduling` (règle stricte / hard) refuserait de démarrer un second pod si le cluster ne dispose que d'un seul nœud (ex: environnement de test local Minikube ou nœud unique), laissant le pod bloqué en `Pending`.
  * `preferredDuringScheduling` (règle souple / soft avec poids 100) ordonne au scheduler d'isoler les pods sur des nœuds physiques distincts dès qu'au moins deux nœuds sont disponibles, tout en autorisant l'exécution sur un nœud unique de test si nécessaire.

### 3.2. Node Affinity & Taints / Tolerations

* **Taints du Control Plane** : Garantissent que nos pods applicatifs ne consomment pas les ressources système réservées à `etcd` et à l'API Server.
* **Ségrégation de charges spécifiques** (en extension de production) :
  Possibilité d'assigner `postgres` à des nœuds équipés de disques NVMe locaux ultra-rapides via `nodeSelector` / `nodeAffinity` (`disktype: ssd`).

---

## 4. Justification Détaillée du Nombre de Réplicas

| Composant | Réplicas | Justification Technique & Risques |
| :--- | :---: | :--- |
| **`poll`** | **2 (min) à 10** | **Stateless (Sans état)**. Minimum 2 réplicas indispensables pour assurer le zéro-downtime lors d'un `RollingUpdate` (`maxUnavailable: 0`) et absorber les pannes d'un pod ou d'un nœud. Scalable horizontalement à volonté via HPA. |
| **`result`** | **2 (min) à 10** | **Stateless**. Même principe que `poll`. Chaque pod interroge indépendamment la base PostgreSQL toutes les secondes et diffuse les scores aux clients connectés via Socket.IO. |
| **`worker`** | **1** | **Consommateur d'événements**. Consomme la file d'attente Redis via un appel bloquant atomique `BLPOP`. Un réplica unique suffit amplement pour traiter des centaines de votes par seconde. Bien que scalable (car la table Postgres utilise un `UPSERT ON CONFLICT (id) DO UPDATE`), un worker unique garantit un ordre de traitement séquentiel strict et évite les verrous concurrents sur la base. |
| **`redis`** | **STRICTEMENT 1** | **File d'attente en mémoire vive (Non clusterisée)**.<br>⚠️ **ATTENTION MAJEURE : REDIS NE DOIT PAS ÊTRE RÉPLIQUÉ DANS CETTE CONFIGURATION.**<br>Si Redis est déployé avec `replicas: 2`, Kubernetes crée deux instances indépendantes sans bus de réplication. Le service `redis` ferait du round-robin : `poll` écrirait dans la mémoire du Pod A, tandis que `worker` lirait la mémoire du Pod B. Des votes resteraient bloqués indéfiniment en mémoire sans jamais être insérés en base. Pour répliquer Redis, il faudrait obligatoirement une topologie complexe *Redis Sentinel* ou *Redis Cluster*. |
| **`postgres`** | **STRICTEMENT 1** | **Base de données relationnelle monolithique avec stockage persistant (PVC ReadWriteOnce)**.<br>Multiplier les réplicas d'un déploiement PostgreSQL standard entraînerait une corruption immédiate des données et des conflits de verrouillage de volume. Une architecture multi-nœuds Postgres nécessiterait un orchestrateur de clustering spécialisé (ex: Patroni, CloudNativePG avec réplication en streaming primaire/secondaire). |

---

## 5. Stratégie Réseau & Load Balancing (L4 / L7 / Ingress / Sticky Sessions)

### 5.1. Découpage Réseau : `ClusterIP` vs `LoadBalancer`

Kubernetes offre plusieurs abstractions pour exposer les conteneurs :

1. **Services Internes (`ClusterIP`) — `postgres` & `redis`** :
   * **Sécurité & Isolation** : Aucune adresse IP externe n'est allouée. Les ports 5432 et 6379 ne sont accessibles que depuis l'intérieur du réseau virtuel du cluster (`default.svc.cluster.local`).
   * Aucun vecteur d'attaque direct depuis l'extérieur du cluster n'est possible sur la base de données ou la file de messages.
2. **Services Externes (`LoadBalancer`) — `poll` & `result`** :
   * Déclenche la création d'un répartiteur de charge managé au niveau de l'infrastructure cloud (Azure Load Balancer, AWS NLB, GCP Cloud Load Balancing).
   * Fournit une adresse IP publique dédiée sur le port standard HTTP (80).

### 5.2. Problématique Socket.IO & Sticky Sessions (`sessionAffinity: ClientIP`) sur `result`

Le composant `result` utilise la bibliothèque `socket.io` pour transmettre les scores en temps réel.
* **Le problème** : Socket.IO commence toujours sa connexion par une phase de négociation en HTTP Long-Polling avant d'effectuer un upgrade vers le protocole WebSocket. En présence de 2 réplicas et d'un Load Balancer classique opérant en simple Round-Robin :
  * La première requête de handshake arrive sur le Pod 1.
  * La seconde requête de validation arrive sur le Pod 2.
  * Le Pod 2 ne possédant pas la session en mémoire, il rejette la requête avec une erreur `HTTP 400 Bad Request ("Session ID unknown")`, provoquant des déconnexions en chaîne.
* **La solution mise en œuvre** :
  ```yaml
  spec:
    type: LoadBalancer
    sessionAffinity: ClientIP
    sessionAffinityConfig:
      clientIP:
        timeoutSeconds: 10800
  ```
  L'activation de l'affinité de session garantit qu'un même navigateur reste lié au même pod pendant la durée de la négociation WebSocket.

### 5.3. Comparaison L4 (Service LoadBalancer) vs L7 (Ingress Controller)

* **Approche actuelle (Service `LoadBalancer` - Layer 4 TCP)** :
  * Simple, robuste et nativement gérée par Kubernetes.
  * *Inconvénient* : Alloue une IP publique et un équipement cloud distinct par service (coûts cloud multipliés par deux).
* **Approche Ingress Controller (Layer 7 HTTP/HTTPS - Recommandation production)** :
  * Un seul Load Balancer public mutualisé pour tout le cluster.
  * Routage basé sur les chemins d'URL (`/` vers `poll`, `/results` vers `result`) ou sur les sous-domaines (`vote.domain.com`, `results.domain.com`).
  * Terminaison centralisée du chiffrement TLS/SSL avec certificats automatisés Let's Encrypt via `cert-manager`.

---

## 6. Autoscaling Horizontal des Pods (HPA) & Cluster Autoscaling

Le dimensionnement dynamique repose sur deux niveaux d'autoscaling :

### 6.1. Horizontal Pod Autoscaler (HPA)

Déployé via [`k8s/hpa.yaml`](file:///home/crabe/Dev/Epitech/tek3/DEVOPS/G-DOP-500-LIL-5-1-bernstein-2/k8s/hpa.yaml), le contrôleur HPA interroge le `metrics-server` toutes les 15 secondes et ajuste le nombre de réplicas selon l'algorithme :

$$\text{Replicas désirés} = \left\lceil \text{Replicas actuels} \times \left( \frac{\text{Utilisation métrique actuelle}}{\text{Utilisation métrique cible}} \right) \right\rceil$$

* **Cible** : Utilisation moyenne de **70% du CPU** alloué en `requests`.
* **Bornes de sécurité** :
  * Minimum : 2 réplicas (pour préserver la haute disponibilité et le zéro-downtime).
  * Maximum : 10 réplicas (pour éviter d'épuiser le quota de ressources du cluster ou de surcharger la base de données PostgreSQL).

### 6.2. Cluster Autoscaler (CA)

Lorsque le HPA crée de nouveaux réplicas mais que les nœuds existants n'ont plus de capacité CPU/mémoire suffisante (pods bloqués en état `Pending`), le **Cluster Autoscaler** provisionne automatiquement de nouvelles machines virtuelles dans le pool de nœuds cloud, puis désalloue les machines inutilisées en période creuse (*scale-down*).

---

## 7. Politiques de Redémarrage (Restart Policies) & Cycle de Vie

### 7.1. La directive `restartPolicy: Always`

Dans Kubernetes, la politique de redémarrage est définie au niveau du Pod :
* **`Always`** : Valeur par défaut et **obligatoire** pour les contrôleurs de type `Deployment`, `ReplicaSet` et `StatefulSet`. En cas d'arrêt du conteneur (code de sortie 0 ou non-nul, crash de l'interpréteur Python, exception non interceptée), le composant `kubelet` redémarre immédiatement le conteneur.
* **`OnFailure`** : Réservé aux `Jobs` et `CronJobs` batch (redémarre uniquement si le code de sortie indique une erreur).
* **`Never`** : Réservé aux tâches one-shot qui ne doivent jamais être relancées en cas d'échec.

### 7.2. Mécanisme du `CrashLoopBackOff`

Si un conteneur échoue de manière répétée au démarrage (par exemple si les identifiants PostgreSQL sont erronés), Kubernetes applique un algorithme de temporisation exponentielle (*Exponential Backoff*) : 10s, 20s, 40s, 80s, jusqu'à un plafond de 5 minutes.
* **Bénéfice architectural** : Évite d'engorger le CPU du nœud hôte, de saturer l'espace disque avec des millions de lignes de logs d'erreur, et de submerger les services tiers de tentatives de connexion infructueuses.

---

## 8. Allocation des Ressources & Classes de Qualité de Service (QoS)

Chaque conteneur dispose d'une déclaration explicite de ses besoins et de ses plafonds :

```yaml
resources:
  requests:
    cpu: "100m"
    memory: "128Mi"
  limits:
    cpu: "500m"
    memory: "256Mi"
```

### 8.1. `requests` vs `limits`

* **`requests` (Planification)** :
  * Utilisé par le `kube-scheduler` pour décider sur quel nœud assigner le pod.
  * Le scheduler s'assure que la somme des `requests` de tous les conteneurs d'un nœud ne dépasse jamais la capacité physique allouable de ce nœud.
* **`limits` (Exécution / Runtime)** :
  * Appliqué au niveau du noyau Linux via les `cgroups` (Control Groups) :
    * **Limite CPU** : Gérée par le *CFS Quota* (Completely Fair Scheduler). Si le conteneur dépasse sa limite, il subit un **bridage (throttling)** sans être arrêté.
    * **Limite Mémoire** : Si le conteneur tente d'allouer plus de RAM que sa limite, le noyau Linux déclenche le **OOM Killer** (*Out Of Memory*). Le conteneur est tué avec le signal `SIGKILL` (**Exit Code 137 / OOMKilled**).

### 8.2. Classe de Qualité de Service : `Burstable`

Kubernetes classe automatiquement les pods en trois catégories de QoS :
1. **Guaranteed** : `requests` == `limits` sur CPU et mémoire.
2. **Burstable** : `requests` < `limits`.
3. **BestEffort** : Aucune ressource spécifiée.

Nos composants utilisent la classe **`Burstable`**. Ce choix garantit un socle de ressources réservé en permanence tout en autorisant les conteneurs à consommer temporairement la capacité résiduelle du nœud pour absorber les pics d'affluence. En cas de pression mémoire critique sur le nœud, les pods `BestEffort` sont évincés en premier, protégeant ainsi nos services.

---

## 9. Persistance des Données & Stratégies de Déploiement

### 9.1. Persistance PostgreSQL (`PersistentVolumeClaim`)

PostgreSQL stocke ses données dans un volume persistant de 1 Gio en mode d'accès **`ReadWriteOnce` (RWO)** :
* `ReadWriteOnce` garantit que le volume ne peut être monté en lecture-écriture que par un seul nœud à la fois.
* Configuration du sous-dossier `PGDATA` :
  ```yaml
  env:
    - name: PGDATA
      value: /var/lib/postgresql/data/pgdata
  ```
  Sur les disques cloud (Azure Disk, AWS EBS), un dossier système `lost+found` est créé à la racine du système de fichiers. Isoler les données Postgres dans le sous-dossier `pgdata` empêche l'échec de la commande `initdb` au premier démarrage.

### 9.2. Stratégie de Mise à Jour de PostgreSQL : `strategy: Recreate`

* **Le piège du `RollingUpdate` sur une base RWO** :
  Par défaut, un `Deployment` utilise la stratégie `RollingUpdate`. Lors d'une mise à jour de l'image PostgreSQL, Kubernetes tente de créer le nouveau Pod avant de stopper l'ancien. Le nouveau Pod tente alors d'attacher le même disque cloud, provoquant une erreur `Multi-Attach error for volume` bloquante.
* **La solution** :
  L'utilisation explicite de `strategy: type: Recreate` ordonne à Kubernetes de stopper l'ancien pod, de libérer le volume persistant, puis de démarrer le nouveau pod.

---

## 10. Sondes de Santé (Probes) & Déploiement sans Coupure (Zero-Downtime)

### 10.1. Rôles respectifs de `livenessProbe` et `readinessProbe`

* **`livenessProbe` (Surveillance de la vie)** :
  * Détecte si le processus est bloqué ou dans un état catatonique (deadlock).
  * En cas d'échec consécutif, Kubernetes **tue et redémarre** le conteneur.
* **`readinessProbe` (Surveillance de la disponibilité)** :
  * Détecte si le conteneur est prêt à recevoir du trafic réseau utilisateur.
  * En cas d'échec, le pod **n'est pas redémarré**, mais son adresse IP est retirée des endpoints du Service / Load Balancer.

### 10.2. Implémentation du Zero-Downtime Rolling Update (`poll` et `result`)

Pour mettre à jour une application sans interrompre le service utilisateur, les déploiements combinent une stratégie de rolling update maîtrisée et des sondes réactives :

```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxSurge: 1
    maxUnavailable: 0
```

1. **`maxUnavailable: 0`** : Interdit formellement à Kubernetes d'éteindre un pod existant tant que le remplaçant n'est pas opérationnel.
2. **`maxSurge: 1`** : Autorise la création temporaire d'un pod surnuméraire pendant la bascule.
3. **Le rôle pivot de la `readinessProbe`** :
   Le nouveau pod démarre. Tant que sa sonde `/readyz` ne renvoie pas un code `HTTP 200` (ce qui valide la connexion active à Redis ou Postgres), aucun trafic ne lui est envoyé. Dès validation, le nouveau pod intègre le pool d'équilibrage et l'ancien pod reçoit un signal d'arrêt gracieux (`SIGTERM`).

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

