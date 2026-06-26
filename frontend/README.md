# microserviços · actions console

A dynamic, **contract-driven** console for the AWS action-driven microservices in
this repo. Forms are generated at runtime from the API Gateway OpenAPI contract,
so the same UI serves every microservice action — one form per action, with
fields derived from the request envelope plus the action's `params`.

## Stack

- Angular 20, standalone components, **zoneless** change detection, **signals**
- Reactive Forms + Angular control flow (`@if` / `@for` / `@switch`)
- `js-yaml` for parsing YAML contracts
- No external UI library; minimal CSS in `src/styles.css`
- TypeScript strict

## Run

```bash
npm install
npm start        # dev server (ng serve) at http://localhost:4200
npm run build    # production build into dist/
```

> Node 22 LTS is the supported runtime for Angular 20. Newer majors may emit
> engine warnings.

## Routes

| Route                       | Component             | Purpose                                              |
| --------------------------- | --------------------- | ---------------------------------------------------- |
| `/`                         | IntegrationsComponent | All integrations, grouped by contract; links to run  |
| `/run/:contractId/:opId`    | RunActionComponent    | Dynamic form for one action + dry-run + response      |
| `/admin`                    | AdminComponent        | Upload OpenAPI, preview, register, export registry    |
| `/settings`                 | SettingsComponent     | API Gateway base URL (persisted to localStorage)      |

## Contract-driven flow

1. On boot the app loads the versioned seed `src/assets/registry.json` via
   `HttpClient` and merges any localStorage working overrides on top.
2. For every contract, each OpenAPI `path` + `POST` operation becomes an
   **integration**. The request body schema is fully dereferenced (local
   `#/components/schemas/...` `$ref`s resolved recursively, cycle-guarded).
3. The dereferenced JSON Schema drives:
   - a Reactive `FormGroup` tree (`buildFormGroup`), and
   - a recursive renderer (`DynamicFormComponent`) — `string` → text input,
     `enum` → `<select>`, `integer`/`number` → number input, `boolean` →
     checkbox, nested `object` → `<fieldset>`, `array` → add/remove list, and a
     free-form `object` (no `properties`, e.g. `params.spec`, `modifications`) →
     a JSON `<textarea>` validated as JSON.
4. On submit, the form value is converted to the request payload: JSON-textarea
   fields parsed back to objects, optional empty fields pruned (required ones
   kept), `dryRun` preserved. It is `POST`ed to `{baseUrl}{path}` and the HTTP
   status + response body is shown.

## Registry: upload → export → commit

The browser cannot write into the repository, so persistence of registered
contracts is a **versioned JSON file** plus an export bridge:

- **Admin → Carregar contrato**: upload an OpenAPI file (`.json` / `.yaml` /
  `.yml`). It is parsed, integrations are detected and previewed, and **Adicionar**
  stores it in the in-memory + localStorage working registry.
- **Admin → Exportar registry.json**: downloads the full merged
  `{ "contracts": [...] }`. **Commit that file back into
  `src/assets/registry.json`** to make the contracts part of the versioned seed
  everyone loads on boot.

`registry.json` shape:

```json
{
  "contracts": [
    { "id": "…", "title": "…", "addedAt": "ISO-8601", "openapi": { /* full OpenAPI doc */ } }
  ]
}
```

The repo ships seeded with one contract (`microservicos-actions`) built from
`microservicos/api-gateway/openapi.yaml`.

## Authentication

The SPA talks **only to the BFF** (`/bff`). Login is handled by the BFF, which
issues its own JWT and keeps it in an **httpOnly cookie** — the browser never
sees the token. Every call therefore goes out with `withCredentials: true`:

- `AuthService` (`src/app/core/auth.service.ts`) — `login` / `logout` / `loadMe`,
  with `currentUser` exposed as a signal.
- `authGuard` (`src/app/core/auth.guard.ts`) — protects routes; hydrates the
  session via `loadMe()` and redirects to `/login` when unauthenticated.
- `authInterceptor` — sends credentials on every request and bounces to `/login`
  on a `401` (skipping the login call itself).

The default `baseUrl` is `/bff`; in the container the bundled nginx proxies
`/bff/*` to the BFF service (`BFF_UPSTREAM`).

## Docker

Multi-stage build (Node 22 build → nginx serving the SPA). The nginx layer also
reverse-proxies `/bff/*` to the BFF and falls back unknown routes to `index.html`.

```bash
docker build -t actions-frontend .
# BFF_UPSTREAM aponta para o serviço do BFF (default http://bff:8081)
docker run -p 8080:80 -e BFF_UPSTREAM=http://bff:8081 actions-frontend
```

- `nginx.default.conf.template` is rendered at start via envsubst (`BFF_UPSTREAM`,
  `NGINX_LOCAL_RESOLVERS`); the `/bff` proxy resolves the upstream at request time,
  so the container starts even if the BFF is not up yet.
- `GET /healthz` returns `ok` for liveness/readiness probes.
```
