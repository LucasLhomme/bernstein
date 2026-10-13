#!/usr/bin/env bash
set -e

# Se positionner dans le répertoire du script
cd "$(dirname "$0")"

# Vérification et chargement automatique des variables du .env
if [ -f .env ]; then
  set -a
  source .env
  set +a
else
  echo "Erreur : fichier .env introuvable. Copie .env.example vers .env et renseigne les valeurs."
  exit 1
fi

# Validation des variables indispensables
for VAR in GITHUB_USER CR_PAT ORGA; do
  if [ -z "${!VAR}" ]; then
    echo "Erreur : la variable $VAR n'est pas définie dans .env"
    exit 1
  fi
done

TAG="${1:-v1}"

# Authentification GHCR
echo "$CR_PAT" | docker login ghcr.io -u "$GITHUB_USER" --password-stdin

# Build et push des images applicatives
for APP in poll worker result; do
  IMAGE="ghcr.io/$ORGA/bernstein-$APP:$TAG"
  echo "==> Build & push de $IMAGE..."
  docker build --platform linux/amd64 -t "$IMAGE" "./$APP"
  docker push "$IMAGE"
done

# Création ou mise à jour du secret Kubernetes (si un cluster est accessible)
if command -v kubectl &>/dev/null && kubectl cluster-info &>/dev/null; then
  NAMESPACE="${K8S_NAMESPACE:-default}"
  EMAIL="${GITHUB_EMAIL:-$GITHUB_USER@users.noreply.github.com}"
  echo "==> Mise à jour du secret ghcr-secret dans le namespace '$NAMESPACE'..."
  kubectl create secret docker-registry ghcr-secret \
    --namespace="$NAMESPACE" \
    --docker-server=ghcr.io \
    --docker-username="$GITHUB_USER" \
    --docker-password="$CR_PAT" \
    --docker-email="$EMAIL" \
    --dry-run=client -o yaml | kubectl apply -f -
else
  echo "Note : kubectl non disponible ou non connecté à un cluster. Création du secret ignorée."
fi

echo "==> Succès ! Toutes les images ont été buildées et pushées avec le tag '$TAG'."