import { Injectable, effect, signal } from '@angular/core';

export type ThemeMode = 'dark' | 'light';

const STORAGE_KEY = 'ui.theme';

/**
 * App theme (dark default + light), persisted in localStorage and applied as a
 * `data-theme` attribute on the document root so CSS tokens can switch. Exposed
 * as a signal in the same style as the other core services.
 */
@Injectable({ providedIn: 'root' })
export class ThemeService {
  private readonly mode = signal<ThemeMode>(this.initial());

  readonly theme = this.mode.asReadonly();

  constructor() {
    // Reflect the current theme onto <html> whenever it changes.
    effect(() => {
      const mode = this.mode();
      if (typeof document !== 'undefined') {
        document.documentElement.setAttribute('data-theme', mode);
      }
    });
  }

  toggle(): void {
    this.set(this.mode() === 'dark' ? 'light' : 'dark');
  }

  set(mode: ThemeMode): void {
    this.mode.set(mode);
    try {
      localStorage.setItem(STORAGE_KEY, mode);
    } catch {
      /* storage unavailable — theme stays in-memory only */
    }
  }

  private initial(): ThemeMode {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored === 'dark' || stored === 'light') {
        return stored;
      }
    } catch {
      /* ignore */
    }
    return 'dark';
  }
}
