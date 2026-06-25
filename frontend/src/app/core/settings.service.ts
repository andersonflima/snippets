import { Injectable, signal } from '@angular/core';

const STORAGE_KEY = 'msc.baseUrl';
const DEFAULT_BASE_URL = 'https://api.example.com/v1';

/**
 * Holds operator settings. Currently only the API Gateway base URL, persisted to
 * localStorage. Auth (AD/Cognito) settings will join here later.
 */
@Injectable({ providedIn: 'root' })
export class SettingsService {
  private readonly baseUrlSig = signal<string>(this.readInitial());

  readonly baseUrl = this.baseUrlSig.asReadonly();

  setBaseUrl(url: string): void {
    const trimmed = url.trim().replace(/\/+$/, '');
    this.baseUrlSig.set(trimmed);
    try {
      localStorage.setItem(STORAGE_KEY, trimmed);
    } catch {
      // localStorage may be unavailable (private mode); keep in-memory value.
    }
  }

  private readInitial(): string {
    try {
      return localStorage.getItem(STORAGE_KEY) ?? DEFAULT_BASE_URL;
    } catch {
      return DEFAULT_BASE_URL;
    }
  }
}
