/**
 * Minimal, intentionally-loose JSON Schema shape used across the dynamic form engine.
 *
 * Full JSON Schema is large and only a relevant subset is consumed here. The
 * interface stays permissive (index signature + optional fields) so arbitrary
 * uploaded contracts parse without fighting the type system, while the fields the
 * engine actually reads are typed.
 */
export interface JsonSchema {
  type?: JsonSchemaType | JsonSchemaType[];
  properties?: Record<string, JsonSchema>;
  required?: string[];
  items?: JsonSchema;
  enum?: ReadonlyArray<string | number | boolean>;
  default?: unknown;
  description?: string;
  pattern?: string;
  minimum?: number;
  maximum?: number;
  format?: string;
  additionalProperties?: boolean | JsonSchema;
  $ref?: string;
  // Permit unknown keywords (e.g. x-amazon-*) without breaking typing.
  [key: string]: unknown;
}

export type JsonSchemaType =
  | 'object'
  | 'array'
  | 'string'
  | 'integer'
  | 'number'
  | 'boolean'
  | 'null';

/** A plain JSON value as produced/consumed by request payloads. */
export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

/** Resolve the effective primary type of a schema node. */
export function schemaType(schema: JsonSchema): JsonSchemaType | undefined {
  const t = schema.type;
  if (Array.isArray(t)) {
    return t.find((x) => x !== 'null');
  }
  return t;
}
