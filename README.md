# Bernstein - Poll Application

[![Epitech: Tek3](https://img.shields.io/badge/Epitech-Tek3-005696.svg)](https://www.epitech.eu/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)


Projet d'orchestration et de déploiement de la **Poll Application** sur un cluster **Kubernetes**, avec gestion des conteneurs via un registre privé (**GitHub Packages / GHCR**), persistance des données, tolérance aux pannes (**Zero-Downtime Update**), **Autoscaling (HPA)**, **Pod Anti-Affinity** et **PodDisruptionBudgets**.

> 📖 **Dossier technique & justifications d'architecture :** [Consultez `docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) pour l'ensemble des justifications (nombre de nœuds, affinité, réplicas, autoscaling, load balancing, restart policies, ressources et classes de QoS).

---

## 🏛️ Architecture

L'application est découpée en cinq composants interconnectés :

```text
poll --(RPUSH votes)--> Redis --(BLPOP votes)--> worker --(UPSERT)--> PostgreSQL <--(SELECT)-- result
```

* **poll** (Python / Flask) : Interface web permettant aux utilisateurs de voter. Les votes sont envoyés dans une file Redis (`votes`). Exposé publiquement via un `LoadBalancer`.
* **redis** (`redis:8-alpine`) : File d'attente en mémoire tampon reliant l'application de vote et le worker. Service interne `ClusterIP`.
* **worker** (Java 25) : Consomme les messages de Redis et les insère / met à jour dans la base PostgreSQL.
* **postgres** (`postgres:15-alpine`) : Base relationnelle stockant les votes de manière persistante via un `PersistentVolumeClaim` (PVC). Initialisée au premier démarrage avec `poll/schema.sql`.
* **result** (Node.js / Express + Socket.IO) : Affiche en temps réel le décompte des votes aux utilisateurs. Exposé publiquement via un `LoadBalancer`.

---

## 📋 Prérequis

* Un cluster Kubernetes opérationnel avec **au moins 2 nœuds** (AKS recommandé, voir [docs/ARCHITECTURE.md §2.3](docs/ARCHITECTURE.md#23-pourquoi-2-nœuds-et-pas-1-ou-5--le-calcul)) et le `metrics-server` (présent par défaut sur AKS, nécessaire au HPA).
* `kubectl` configuré et connecté au cluster.
* `docker` installé pour le build des images.
* Un compte GitHub avec un Personal Access Token (PAT) disposant des droits `read:packages` et `write:packages` sur GHCR.

---

## ⚙️ Configuration (`.env`)

Conformément aux exigences du sujet, les identifiants et secrets sensibles ne sont **jamais** versionnés dans Git.

1. Créez votre fichier `.env` à partir du template :
   ```bash
   cp .env.example .env
   ```

2. Renseignez les variables nécessaires :
   ```dotenv
   # Registre d'images GHCR
   GITHUB_USER=VotreUtilisateurGitHub
   CR_PAT=ghp_VotrePersonalAccessToken
   GITHUB_EMAIL=votre.email@epitech.eu
   ORGA=votre-organisation-github
   TAG=v1

   # Identifiants PostgreSQL
   POSTGRES_DB=polls
   POSTGRES_USER=polls
   POSTGRES_PASSWORD=mon_super_password

   # Redis (docker-compose uniquement, non utilisé sur Kubernetes)
   REDIS_PASSWORD=polls

   # Namespace Kubernetes (optionnel)
   K8S_NAMESPACE=default
   ```

---

## 🚀 How to install

### Option 1 : Déploiement automatique (Recommandé)

Le script [deploy.sh](deploy.sh) automatise l'ensemble des étapes : chargement du `.env`, création idempotente des secrets (`ghcr-secret` et `postgres-secret`), création du `ConfigMap` pour PostgreSQL, puis application des manifests Kubernetes.

```bash
chmod +x deploy.sh
./deploy.sh
```

---

### Option 2 : Déploiement manuel étape par étape

Si vous souhaitez déployer manuellement sans passer par le script :

#### 1. Créer le secret pour le registre privé GHCR

Permet aux nœuds Kubernetes de télécharger les images privées depuis GitHub Container Registry :

```bash
kubectl create secret docker-registry ghcr-secret \
  --docker-server=ghcr.io \
  --docker-username="<GITHUB_USER>" \
  --docker-password="<CR_PAT>" \
  --docker-email="<GITHUB_EMAIL>"
```

#### 2. Configuration de PostgreSQL

##### A. Créer le Secret (avec la valeur de ton mot de passe) :
```bash
kubectl create secret generic postgres-secret \
  --from-literal=POSTGRES_PASSWORD="mon_super_password"
```

##### B. Créer le ConfigMap (en lisant directement ton fichier schema.sql) :
```bash
kubectl create configmap postgres-schema \
  --from-file=schema.sql=poll/schema.sql
```

Ce ConfigMap sera monté dans `/docker-entrypoint-initdb.d/schema.sql` par PostgreSQL pour initialiser la table `votes` dès le premier démarrage.

#### 3. Appliquer les manifests Kubernetes

Déployez les services de stockage et les applications :

```bash
kubectl apply -f k8s/
```

---

## 🐳 Build & Push des images (GHCR)

Pour builder les images Docker (`poll`, `worker`, `result`) et les envoyer sur votre registre privé :

```bash
chmod +x push_images.sh
./push_images.sh v1
```

Le script se charge de :
1. Se connecter à `ghcr.io` avec votre token GitHub.
2. Construire les images multiplateformes (`linux/amd64`).
3. Les pousser avec le tag spécifié (par défaut `v1`).
4. Créer / actualiser le secret `ghcr-secret` sur le cluster si `kubectl` est connecté.

---

## 🔍 Vérification & Accès aux services

1. Vérifiez l'état des Pods :
   ```bash
   kubectl get pods -w
   ```
   Tous les pods doivent être à l'état `Running` et `READY 1/1` (`poll` et `result` : 2 pods chacun, créés par le HPA quelques secondes après le déploiement).

2. Récupérez les adresses IP externes des services :
   ```bash
   kubectl get svc poll result
   ```

* **Poll** : Ouvrez l'adresse `http://<EXTERNAL-IP>` pour soumettre un vote.
* **Result** : Ouvrez l'adresse `http://<EXTERNAL-IP>` pour visualiser les résultats mis à jour en direct (Socket.IO en long-polling HTTP).

3. Vérifiez l'autoscaling et les PodDisruptionBudgets :
   ```bash
   kubectl get hpa,pdb
   ```

*(Sur Minikube en local, exécutez `minikube tunnel` dans un terminal séparé pour allouer les External-IPs des LoadBalancers).*

---

## 📈 Autoscaling Horizontal (HPA)

Le projet intègre un Horizontal Pod Autoscaler sur `poll` et `result` basé sur la consommation CPU (seuil à 70%, 2 à 10 réplicas) :

```bash
kubectl get hpa
```

Pour observer le comportement sous charge ou tester l'autoscaling :
```bash
# Générer du trafic de test vers poll
kubectl run -i --tty load-generator --rm --image=busybox:1.36 --restart=Never -- /bin/sh -c "while true; do wget -q -O- http://poll; done"
```

---

## 📚 Documentation Technique

Pour les détails d'ingénierie et la justification des choix, consultez le dossier technique dédié :
* **[Dossier d'Architecture et Justifications](docs/ARCHITECTURE.md)** (`docs/ARCHITECTURE.md`) :
  1. Architecture globale & flux applicatif
  2. Topologie du cluster & calcul du nombre de nœuds
  3. Affinité, anti-affinité & PodDisruptionBudgets (`kubectl drain`)
  4. Justification du nombre de réplicas (cas critique de Redis)
  5. Stratégie réseau & load balancing (`externalTrafficPolicy`, affinité Socket.IO, L4 vs L7)
  6. Autoscaling : HPA, worker (KEDA), Cluster Autoscaler, VPA
  7. Politiques de redémarrage (`restartPolicy: Always`, CrashLoopBackOff)
  8. Ressources par service & classes de QoS (Burstable / Guaranteed)
  9. Persistance des données & stratégie `Recreate`
  10. Sondes de santé (`readinessProbe` & `livenessProbe`) et zéro-downtime

Le projet implémente une stratégie de **RollingUpdate** couplée à des sondes de santé (`readinessProbe` et `livenessProbe`) sur les services web :

```yaml
strategy:
  type: RollingUpdate
  rollingUpdate:
    maxUnavailable: 0
    maxSurge: 1
```

Pour tester une mise à jour applicative sans coupure :
1. Modifiez le code source (ex. : dans `poll/templates/index.html`).
2. Buildez et poussez la nouvelle version :
   ```bash
   ./push_images.sh v2
   ```
3. Mettez à jour le déploiement sur Kubernetes :
   ```bash
   kubectl set image deployment/poll poll=ghcr.io/<ORGA>/bernstein-poll:v2
   ```
4. Observez le remplacement progressif des pods sans interruption de trafic :
   ```bash
   kubectl rollout status deployment/poll
   ```
