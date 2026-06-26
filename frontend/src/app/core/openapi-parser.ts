import { load as loadYaml } from 'js-yaml';
import { JsonSchema } from './json-schema';

/** An OpenAPI document, kept loose: only the parts we read are typed. */
export interface OpenApiDoc {
  info?: { title?: string; version?: string; description?: string };
  paths?: Record<string, Record<string, OpenApiOperation>>;
  components?: { schemas?: Record<string, JsonSchema> };
  [key: string]: unknown;
}

export interface OpenApiOperation {
  operationId?: string;
  summary?: string;
  tags?: string[];
  requestBody?: {
    content?: Record<string, { schema?: JsonSchema }>;
  };
  [key: string]: unknown;
}

/** A single runnable action derived from one OpenAPI operation. */
export interface Integration {
  contractId: string;
  id: string;
  name: string;
  method: string;
  path: string;
  summary?: string;
  /** Fully dereferenced request schema (no `$ref` left). */
  requestSchema: JsonSchema;
}

const HTTP_METHODS = ['post', 'put', 'patch', 'get', 'delete'] as const;

/**
 * Parse a raw contract string into an object. Tries JSON first (fast path),
 * falls back to YAML. Throws a descriptive error if neither parses to an object.
 */
export function parseContract(raw: string): OpenApiDoc {
  const text = raw.trim();
  if (!text) {
    throw new Error('Empty contract.');
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = loadYaml(text);
  }
  if (!parsed || typeof parsed !== 'object') {
    throw new Error('Contract did not parse to an object (expected JSON or YAML OpenAPI).');
  }
  return parsed as OpenApiDoc;
}

/**
 * Dereference a JSON Schema node against the OpenAPI document, resolving local
 * `#/components/schemas/...` refs recursively. Cycles are guarded by tracking the
 * set of refs currently on the resolution stack.
 */
function dereference(
  node: JsonSchema,
  doc: OpenApiDoc,
  seen: ReadonlySet<string>,
): JsonSchema {
  if (node.$ref) {
    const ref = node.$ref;
    if (seen.has(ref)) {
      // Cycle: return a shallow stub so traversal terminates.
      return { type: 'object', description: `(circular ref ${ref})` };
    }
    const target = resolveRef(ref, doc);
    if (!target) {
      return { description: `(unresolved ref ${ref})` };
    }
    const nextSeen = new Set(seen);
    nextSeen.add(ref);
    return dereference(target, doc, nextSeen);
  }

  const out: JsonSchema = { ...node };

  if (node.properties) {
    const props: Record<string, JsonSchema> = {};
    for (const [key, value] of Object.entries(node.properties)) {
      props[key] = dereference(value, doc, seen);
    }
    out.properties = props;
  }

  if (node.items) {
    out.items = dereference(node.items, doc, seen);
  }

  if (node.additionalProperties && typeof node.additionalProperties === 'object') {
    out.additionalProperties = dereference(node.additionalProperties, doc, seen);
  }

  return out;
}

/** Resolve a `#/components/schemas/Name` pointer to its schema node. */
function resolveRef(ref: string, doc: OpenApiDoc): JsonSchema | undefined {
  const prefix = '#/components/schemas/';
  if (!ref.startsWith(prefix)) {
    return undefined;
  }
  const name = ref.slice(prefix.length);
  return doc.components?.schemas?.[name];
}

/** Extract all runnable integrations from a contract document. */
export function extractIntegrations(contractId: string, doc: OpenApiDoc): Integration[] {
  const integrations: Integration[] = [];
  const paths = doc.paths ?? {};

  for (const [path, methods] of Object.entries(paths)) {
    if (!methods || typeof methods !== 'object') {
      continue;
    }
    for (const method of HTTP_METHODS) {
      const operation = methods[method];
      if (!operation || typeof operation !== 'object') {
        continue;
      }
      const requestSchema = resolveRequestSchema(operation, doc);
      integrations.push({
        contractId,
        id: operation.operationId ?? `${method}:${path}`,
        name: operation.tags?.[0] ?? path.replace(/^\//, ''),
        method: method.toUpperCase(),
        path,
        summary: operation.summary,
        requestSchema,
      });
    }
  }

  return integrations;
}

/** Resolve and dereference the JSON request body schema of an operation. */
function resolveRequestSchema(operation: OpenApiOperation, doc: OpenApiDoc): JsonSchema {
  const schema = operation.requestBody?.content?.['application/json']?.schema;
  if (!schema) {
    return { type: 'object', properties: {} };
  }
  return dereference(schema, doc, new Set<string>());
}
