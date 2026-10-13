#!/usr/bin/env bash
set -euo pipefail

# 1. Vérification des variables d'environnement
if [ ! -f .env ]; then
  echo "Erreur : Fichier .env manquant. Copie .env.example vers .env et renseigne les valeurs."
  exit 1
fi

set -a
source .env
set +a

: "${GITHUB_USER:?Variable GITHUB_USER non définie}"
: "${CR_PAT:?Variable CR_PAT non définie}"
: "${POSTGRES_PASSWORD:?Variable POSTGRES_PASSWORD non définie}"
: "${GITHUB_EMAIL:?Variable GITHUB_EMAIL non définie}"

echo "injection des secrets et configurations..."

# 2. Secret GHCR pour les images privées
kubectl create secret docker-registry ghcr-secret \
  --docker-server=ghcr.io \
  --docker-username="$GITHUB_USER" \
  --docker-password="$CR_PAT" \
  --docker-email="$GITHUB_EMAIL" \
  --dry-run=client -o yaml | kubectl apply -f -

# 3. Secret PostgreSQL
kubectl create secret generic postgres-secret \
  --from-literal=POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  --dry-run=client -o yaml | kubectl apply -f -

# 4. ConfigMap pour le schéma initial
kubectl create configmap postgres-schema \
  --from-file=schema.sql=poll/schema.sql \
  --dry-run=client -o yaml | kubectl apply -f -

echo "==> Application des manifests Kubernetes..."
kubectl apply -f k8s/

echo "==> Déploiement initié avec succès."