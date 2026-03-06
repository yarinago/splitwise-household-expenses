# splitwise-household-expenses

Splitwise household dashboard service.

The app now runs as a web service that computes Splitwise data in the background and serves:
- HTML dashboard: `/`
- Tables screen: `/tables`
- JSON API: `/api/dashboard`
- Health probes: `/healthz`, `/readyz`

## Local Run

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Provide env vars (for example via `.env`):
```bash
SPLITWISE_CLIENT_ID=...
SPLITWISE_CLIENT_SECRET=...
SPLITWISE_ACCESS_TOKEN_JSON={"access_token":"...","token_type":"bearer","refresh_token":"..."}
SPLITWISE_GROUP_ID=12345678
SPLITWISE_MEMBERS={"98765432":"Ally","12312312":"Bob"}
SPLITWISE_FIRST_MONTH=2008-01
SPLITWISE_EXCLUDE_MONTHS=
SPLITWISE_EXCLUDE_DESCRIPTIONS=
SPLITWISE_REFRESH_SECONDS=900
PORT=8080
```

3. Run the web app:
```bash
python web_app.py
```

4. Open:
```text
http://localhost:8080
```

## Background Compute Model

- A background thread refreshes data every `SPLITWISE_REFRESH_SECONDS` (default `900`).
- Latest successful snapshot is cached in memory.
- Web requests read cached snapshot only; they do not call Splitwise directly.
- `POST /refresh` and `POST /api/refresh` trigger manual refresh.
- Dashboard has a 2x2 graph layout with:
  - month totals (chronological)
  - category totals (month filter)
  - per-person owes (month scope)
  - category-over-time (category selector)
- Category bars use distinct colors.
- Tables and full-data summary are split into a dedicated `/tables` page with month and text filters.

## CI Image Build

Workflow: `.github/workflows/splitwise-export.yml`

- Builds Docker image from `Dockerfile`
- Pushes `latest` on `main` (dev track)
- Pushes `release-vprod` and a release tag when pushing Git tags matching `release/v*prod` (prod track)
- Runs build-only on pull requests

## Kubernetes Layout

- `k8s/base`: shared manifests (`Deployment` + `Service`)
- `k8s/overlays/dev`: development overrides
- `k8s/overlays/prod`: production overrides
- `k8s/base/secret.example.yaml`: template for required secret keys

Render test:
```bash
kubectl kustomize k8s/overlays/prod
kubectl kustomize k8s/overlays/dev
```

## Argo CD: App-Of-Apps + ApplicationSet

This repo contains:
- `argocd/splitwise-export-applicationset.yaml`

That `ApplicationSet` generates child Argo `Application` resources for `dev`
and `prod`, each pointing to its matching overlay path.

In your other repo (the one that runs Argo app-of-apps), create a parent `Application` that syncs this repo's `argocd` path:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: splitwise-export-bootstrap
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/yarinago/splitwise-household-expenses.git
    targetRevision: main
    path: argocd
  destination:
    server: https://kubernetes.default.svc
    namespace: argocd
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
```

## Runtime Config Sync (Automated)

Workflow: `.github/workflows/sync-runtime-config.yml`

What it does:
- Reads one shared set of GitHub Variables and Secrets
- Creates/updates `ConfigMap/splitwise-export-config` in each namespace
- Creates/updates `Secret/splitwise-export-secrets` in each namespace
- Applies the same runtime config to both `dev` and `prod` (version differences come from image tags and overlay patch values)

Required GitHub Variables (shared):
- `SPLITWISE_GROUP_ID`
- `SPLITWISE_MEMBERS`
- `SPLITWISE_FIRST_MONTH` (optional, defaults to `2008-01`)
- `SPLITWISE_EXCLUDE_MONTHS` (optional)
- `SPLITWISE_EXCLUDE_DESCRIPTIONS` (optional)
- `SPLITWISE_REFRESH_SECONDS` (optional, defaults to `900`)
- `SPLITWISE_NAMESPACE_DEV` (optional override, default `splitwise-dev`)
- `SPLITWISE_NAMESPACE_PROD` (optional override, default `splitwise`)

Required GitHub Secrets (shared):
- `SPLITWISE_CLIENT_ID`
- `SPLITWISE_CLIENT_SECRET`
- `SPLITWISE_ACCESS_TOKEN_JSON`

## Legacy Excel Export

`splitwise_to_excel.py` is still in the repo for backward compatibility and can still generate workbook output if you run it directly.
