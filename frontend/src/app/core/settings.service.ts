import { Injectable, signal } from '@angular/core';

const STORAGE_KEY = 'msc.baseUrl';
const DEFAULT_BASE_URL = '/bff';

/**
 * Holds operator settings. The base URL points at the BFF, the single backend
 * the frontend talks to. The BFF proxies microservice actions and owns auth:
 * it sets an httpOnly session cookie, so calls go out with `withCredentials`
 * and the browser never sees the token. Persisted to localStorage.
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
