import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';
import {
  AbstractControl,
  FormArray,
  FormControl,
  FormGroup,
  ReactiveFormsModule,
} from '@angular/forms';
import { JsonSchema } from '../core/json-schema';
import { buildControl } from './form-schema';
import { FieldMeta, objectFields } from './field-meta';

/**
 * Recursive renderer for a (dereferenced) JSON Schema object backed by a
 * FormGroup. Each property renders by kind; nested objects and array items
 * recurse back into this component.
 */
@Component({
  selector: 'app-dynamic-form',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [ReactiveFormsModule],
  template: `
    @for (field of fields(); track field.key) {
      <div class="field">
        @switch (field.kind) {
          @case ('boolean') {
            <label class="toggle">
              <input type="checkbox" [formControl]="asControl(field.key)" />
              {{ field.label }}
            </label>
          }
          @default {
            <label>
              {{ field.label }}
              @if (field.required) {
                <span class="req">*</span>
              }
            </label>

            @switch (field.kind) {
              @case ('enum') {
                <select [formControl]="asControl(field.key)">
                  @if (!field.required) {
                    <option [ngValue]="''">—</option>
                  }
                  @for (opt of field.enumValues; track opt) {
                    <option [ngValue]="opt">{{ opt }}</option>
                  }
                </select>
              }
              @case ('number') {
                <input type="number" [formControl]="asControl(field.key)" />
              }
              @case ('freeform-object') {
                <textarea
                  [formControl]="asControl(field.key)"
                  placeholder='{ "key": "value" }'
                ></textarea>
                <div class="hint">Free-form object — enter valid JSON.</div>
              }
              @case ('object') {
                <fieldset>
                  <app-dynamic-form
                    [schema]="field.schema"
                    [group]="asGroup(field.key)"
                  />
                </fieldset>
              }
              @case ('array') {
                <div>
                  @for (item of asArray(field.key).controls; track $index) {
                    <div class="array-item">
                      @if (isGroup(item)) {
                        <fieldset>
                          <app-dynamic-form
                            [schema]="itemSchema(field)"
                            [group]="asItemGroup(item)"
                          />
                        </fieldset>
                      } @else {
                        @if (itemIsNumber(field)) {
                          <input type="number" [formControl]="asItemControl(item)" />
                        } @else {
                          <input type="text" [formControl]="asItemControl(item)" />
                        }
                      }
                      <button
                        type="button"
                        class="danger"
                        (click)="removeItem(field, $index)"
                      >
                        remove
                      </button>
                    </div>
                  }
                  <button type="button" (click)="addItem(field)">+ add</button>
                </div>
              }
              @default {
                <input type="text" [formControl]="asControl(field.key)" />
              }
            }

            @if (field.description) {
              <div class="hint">{{ field.description }}</div>
            }
            @if (field.pattern) {
              <div class="hint">pattern: <code>{{ field.pattern }}</code></div>
            }
            @if (controlInvalid(field.key)) {
              <div class="error">{{ errorText(field.key) }}</div>
            }
          }
        }
      </div>
    }
  `,
})
export class DynamicFormComponent {
  readonly schema = input.required<JsonSchema>();
  readonly group = input.required<FormGroup>();

  readonly fields = computed<FieldMeta[]>(() => objectFields(this.schema()));

  asControl(key: string): FormControl {
    return this.group().get(key) as FormControl;
  }

  asGroup(key: string): FormGroup {
    return this.group().get(key) as FormGroup;
  }

  asArray(key: string): FormArray<AbstractControl> {
    return this.group().get(key) as FormArray<AbstractControl>;
  }

  asItemControl(control: AbstractControl): FormControl {
    return control as FormControl;
  }

  asItemGroup(control: AbstractControl): FormGroup {
    return control as FormGroup;
  }

  isGroup(control: AbstractControl): boolean {
    return control instanceof FormGroup;
  }

  itemSchema(field: FieldMeta): JsonSchema {
    return field.schema.items ?? { type: 'string' };
  }

  itemIsNumber(field: FieldMeta): boolean {
    const t = field.schema.items?.type;
    return t === 'integer' || t === 'number';
  }

  addItem(field: FieldMeta): void {
    const itemSchema = this.itemSchema(field);
    this.asArray(field.key).push(buildControl(itemSchema, true));
  }

  removeItem(field: FieldMeta, index: number): void {
    this.asArray(field.key).removeAt(index);
  }

  controlInvalid(key: string): boolean {
    const control = this.group().get(key);
    return !!control && control.invalid && (control.touched || control.dirty);
  }

  errorText(key: string): string {
    const control = this.group().get(key);
    if (!control || !control.errors) {
      return '';
    }
    const errors = control.errors;
    if (errors['required']) {
      return 'Required.';
    }
    if (errors['pattern']) {
      return 'Does not match the expected pattern.';
    }
    if (errors['json']) {
      return 'Must be valid JSON.';
    }
    if (errors['number']) {
      return 'Must be a number.';
    }
    if (errors['min']) {
      return `Must be >= ${errors['min'].min}.`;
    }
    if (errors['max']) {
      return `Must be <= ${errors['max'].max}.`;
    }
    return 'Invalid value.';
  }
}
