import { JsonSchema, JsonValue, schemaType } from './json-schema';

/**
 * Transform a raw Reactive Forms value into the request payload, driven by the
 * (dereferenced) request schema.
 *
 * Rules:
 *  - free-form object fields (object without `properties`) are stored as JSON
 *    strings in the form and parsed back to objects here;
 *  - optional fields that are empty (null / '' / empty object / empty array) are
 *    pruned; required fields are kept even if empty so the server can validate;
 *  - booleans (e.g. `dryRun`) are kept as-is.
 */
export function buildPayload(schema: JsonSchema, value: unknown): JsonValue {
  return transform(schema, value, true) ?? {};
}

/**
 * @param required whether this node is required by its parent (controls pruning).
 * @returns the transformed value, or `undefined` when the node should be pruned.
 */
function transform(schema: JsonSchema, value: unknown, required: boolean): JsonValue | undefined {
  const type = schemaType(schema);

  if (type === 'object') {
    if (schema.properties) {
      return transformObject(schema, value, required);
    }
    return transformFreeFormObject(value, required);
  }

  if (type === 'array') {
    return transformArray(schema, value, required);
  }

  if (type === 'boolean') {
    return Boolean(value);
  }

  if (type === 'integer' || type === 'number') {
    if (value === null || value === undefined || value === '') {
      return required ? null : undefined;
    }
    const num = Number(value);
    return Number.isNaN(num) ? (required ? null : undefined) : num;
  }

  // string and everything else
  if (value === null || value === undefined || String(value).trim() === '') {
    return required ? '' : undefined;
  }
  return value as JsonValue;
}

function transformObject(
  schema: JsonSchema,
  value: unknown,
  required: boolean,
): JsonValue | undefined {
  const source = (value ?? {}) as Record<string, unknown>;
  const requiredKeys = new Set(schema.required ?? []);
  const result: Record<string, JsonValue> = {};

  for (const [key, childSchema] of Object.entries(schema.properties ?? {})) {
    const childRequired = requiredKeys.has(key);
    const transformed = transform(childSchema, source[key], childRequired);
    if (transformed !== undefined) {
      result[key] = transformed;
    }
  }

  if (!required && Object.keys(result).length === 0) {
    return undefined;
  }
  return result;
}

function transformFreeFormObject(value: unknown, required: boolean): JsonValue | undefined {
  if (value === null || value === undefined || String(value).trim() === '') {
    return required ? {} : undefined;
  }
  try {
    const parsed = JSON.parse(String(value)) as JsonValue;
    if (
      !required &&
      parsed &&
      typeof parsed === 'object' &&
      !Array.isArray(parsed) &&
      Object.keys(parsed).length === 0
    ) {
      return undefined;
    }
    return parsed;
  } catch {
    // Should be blocked by jsonValidator; keep raw string as last resort.
    return String(value);
  }
}

function transformArray(
  schema: JsonSchema,
  value: unknown,
  required: boolean,
): JsonValue | undefined {
  const items = Array.isArray(value) ? value : [];
  const itemSchema = schema.items ?? { type: 'string' };
  const result: JsonValue[] = [];

  for (const item of items) {
    // Array items are always emitted (they were explicitly added by the operator).
    const transformed = transform(itemSchema, item, true);
    if (transformed !== undefined) {
      result.push(transformed);
    }
  }

  if (!required && result.length === 0) {
    return undefined;
  }
  return result;
}
