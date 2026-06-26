import {
  AbstractControl,
  FormArray,
  FormControl,
  FormGroup,
  Validators,
} from '@angular/forms';
import { JsonSchema, schemaType } from '../core/json-schema';
import { jsonValidator, numberValidator } from '../core/validators';

/**
 * Build a Reactive Forms control tree from a (dereferenced) JSON Schema.
 *
 * Mapping:
 *  - object with properties           -> FormGroup (required[] -> Validators.required)
 *  - object without properties        -> FormControl(json string) + jsonValidator
 *  - array                            -> FormArray (starts empty)
 *  - string                           -> FormControl + optional Validators.pattern
 *  - integer/number                   -> FormControl + numberValidator(min,max)
 *  - boolean                          -> FormControl(boolean)
 */
export function buildFormGroup(schema: JsonSchema): FormGroup {
  const control = buildControl(schema, true);
  if (control instanceof FormGroup) {
    return control;
  }
  // Defensive: top-level schema is expected to be an object.
  return new FormGroup({ value: control });
}

/** Build a single control for a schema node. */
export function buildControl(schema: JsonSchema, required: boolean): AbstractControl {
  const type = schemaType(schema);

  if (type === 'object') {
    if (schema.properties) {
      return buildObjectGroup(schema);
    }
    return buildFreeFormControl(schema, required);
  }

  if (type === 'array') {
    return new FormArray<AbstractControl>([]);
  }

  if (type === 'boolean') {
    const def = typeof schema.default === 'boolean' ? schema.default : false;
    return new FormControl(def, { nonNullable: true });
  }

  if (type === 'integer' || type === 'number') {
    return buildNumberControl(schema, required);
  }

  return buildStringControl(schema, required);
}

function buildObjectGroup(schema: JsonSchema): FormGroup {
  const requiredKeys = new Set(schema.required ?? []);
  const controls: Record<string, AbstractControl> = {};
  for (const [key, child] of Object.entries(schema.properties ?? {})) {
    controls[key] = buildControl(child, requiredKeys.has(key));
  }
  return new FormGroup(controls);
}

function buildFreeFormControl(schema: JsonSchema, required: boolean): FormControl {
  const validators = [jsonValidator];
  if (required) {
    validators.push(Validators.required);
  }
  return new FormControl('', { nonNullable: true, validators });
}

function buildStringControl(schema: JsonSchema, required: boolean): FormControl {
  const def = typeof schema.default === 'string' ? schema.default : '';
  const validators = [];
  if (required) {
    validators.push(Validators.required);
  }
  if (schema.pattern) {
    validators.push(Validators.pattern(schema.pattern));
  }
  return new FormControl(def, { nonNullable: true, validators });
}

function buildNumberControl(schema: JsonSchema, required: boolean): FormControl {
  const def =
    typeof schema.default === 'number' ? schema.default : null;
  const validators = [numberValidator(schema.minimum, schema.maximum)];
  if (required) {
    validators.push(Validators.required);
  }
  return new FormControl<number | null>(def, { validators });
}
