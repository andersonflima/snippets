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

There is **no auth today** (AD/Cognito will be integrated later). An empty
`authInterceptor` (`src/app/core/auth.interceptor.ts`) is already wired into
`provideHttpClient(withInterceptors([authInterceptor]))` with a `TODO` so the
bearer token can be attached without touching call sites. Settings only exposes
the configurable base URL.
```
