import { JsonSchema, JsonSchemaType, schemaType } from '../core/json-schema';

/** How a single schema property should be rendered. */
export type FieldKind =
  | 'enum'
  | 'boolean'
  | 'number'
  | 'freeform-object'
  | 'object'
  | 'array'
  | 'string';

export interface FieldMeta {
  key: string;
  schema: JsonSchema;
  kind: FieldKind;
  required: boolean;
  label: string;
  description?: string;
  pattern?: string;
  enumValues: ReadonlyArray<string | number | boolean>;
}

/** Classify a schema node into a renderable field kind. */
export function fieldKind(schema: JsonSchema): FieldKind {
  if (schema.enum && schema.enum.length > 0) {
    return 'enum';
  }
  const type: JsonSchemaType | undefined = schemaType(schema);
  switch (type) {
    case 'boolean':
      return 'boolean';
    case 'integer':
    case 'number':
      return 'number';
    case 'array':
      return 'array';
    case 'object':
      return schema.properties ? 'object' : 'freeform-object';
    default:
      return 'string';
  }
}

/** Build ordered render metadata for the properties of an object schema. */
export function objectFields(schema: JsonSchema): FieldMeta[] {
  const requiredKeys = new Set(schema.required ?? []);
  return Object.entries(schema.properties ?? {}).map(([key, child]) => ({
    key,
    schema: child,
    kind: fieldKind(child),
    required: requiredKeys.has(key),
    label: key,
    description: child.description,
    pattern: child.pattern,
    enumValues: child.enum ?? [],
  }));
}
