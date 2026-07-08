import { Injectable, signal } from '@angular/core';

export type ToastKind = 'success' | 'info' | 'warn' | 'error';

export interface Toast {
  id: number;
  kind: ToastKind;
  title: string;
  message?: string;
  /** Auto-dismiss delay in ms. `0` keeps the toast until dismissed. */
  duration: number;
}

/**
 * App-wide, signal-based notification store. Components read {@link toasts} and
 * render them; anything can push via the typed helpers. Auto-dismissal is handled
 * here so callers stay declarative. Isolated from any transport so it works the
 * same for mock and API-backed flows.
 */
@Injectable({ providedIn: 'root' })
export class ToastService {
  private seq = 0;
  private readonly items = signal<Toast[]>([]);

  readonly toasts = this.items.asReadonly();

  success(title: string, message?: string): number {
    return this.show('success', title, message);
  }

  info(title: string, message?: string): number {
    return this.show('info', title, message);
  }

  warn(title: string, message?: string): number {
    return this.show('warn', title, message);
  }

  error(title: string, message?: string): number {
    return this.show('error', title, message, 6000);
  }

  show(kind: ToastKind, title: string, message?: string, duration = 4200): number {
    const id = ++this.seq;
    this.items.update((list) => [...list, { id, kind, title, message, duration }]);
    if (duration > 0 && typeof setTimeout !== 'undefined') {
      setTimeout(() => this.dismiss(id), duration);
    }
    return id;
  }

  dismiss(id: number): void {
    this.items.update((list) => list.filter((t) => t.id !== id));
  }

  clear(): void {
    this.items.set([]);
  }
}
