import { AbstractControl, ValidationErrors, ValidatorFn } from '@angular/forms';

/**
 * Validates that a string control holds valid JSON. Empty/blank values pass
 * (the field is treated as "not provided" and pruned downstream).
 */
export function jsonValidator(control: AbstractControl): ValidationErrors | null {
  const value = control.value;
  if (value === null || value === undefined || String(value).trim() === '') {
    return null;
  }
  try {
    JSON.parse(String(value));
    return null;
  } catch {
    return { json: true };
  }
}

/**
 * Numeric validator with optional bounds. Empty values pass (required is handled
 * separately); non-numeric values fail.
 */
export function numberValidator(min?: number, max?: number): ValidatorFn {
  return (control: AbstractControl): ValidationErrors | null => {
    const value = control.value;
    if (value === null || value === undefined || value === '') {
      return null;
    }
    const num = Number(value);
    if (Number.isNaN(num)) {
      return { number: true };
    }
    if (min !== undefined && num < min) {
      return { min: { min, actual: num } };
    }
    if (max !== undefined && num > max) {
      return { max: { max, actual: num } };
    }
    return null;
  };
}
