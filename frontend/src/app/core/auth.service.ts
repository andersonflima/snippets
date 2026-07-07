import { Injectable, computed, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';
import { SettingsService } from './settings.service';

/** The authenticated principal, as returned by the BFF. */
export interface AuthUser {
  username: string;
  roles: string[];
}

interface MeResponse {
  user: AuthUser;
}

/**
 * Hardcoded test-login switch. When `true`, auth is faked entirely in the
 * browser (no BFF calls) for local/demo use — accepts `admin` / `admin` and
 * persists a mock session in localStorage. Set to `false` to use the real BFF
 * endpoints (`/auth/login`, `/auth/logout`, `/auth/me`).
 */
const MOCK_AUTH = true;

/** localStorage key holding the serialized mock session. */
const MOCK_SESSION_KEY = 'ui.mockSession';

/**
 * Session state backed by the BFF.
 *
 * The BFF owns the JWT and stores it in an httpOnly cookie (`bff_session`), so
 * the browser never handles the token directly. Every call therefore goes out
 * with `withCredentials: true` to carry that cookie. State is exposed as signals
 * in the same style as {@link SettingsService}.
 */
@Injectable({ providedIn: 'root' })
export class AuthService {
  private readonly http = inject(HttpClient);
  private readonly settings = inject(SettingsService);

  private readonly currentUserSig = signal<AuthUser | null>(null);

  readonly currentUser = this.currentUserSig.asReadonly();
  readonly isAuthenticated = computed<boolean>(() => this.currentUserSig() !== null);

  /** Authenticate against the BFF; on success the cookie is set server-side. */
  async login(username: string, password: string): Promise<AuthUser> {
    return MOCK_AUTH
      ? this.mockLogin(username, password)
      : this.realLogin(username, password);
  }

  /** Clear the session on the BFF and locally. */
  async logout(): Promise<void> {
    return MOCK_AUTH ? this.mockLogout() : this.realLogout();
  }

  /**
   * Hydrate `currentUser` from the cookie session, if any. Returns the user when
   * authenticated, or null when the BFF answers 401.
   */
  async loadMe(): Promise<AuthUser | null> {
    return MOCK_AUTH ? this.mockLoadMe() : this.realLoadMe();
  }

  private async realLogin(
    username: string,
    password: string,
  ): Promise<AuthUser> {
    const res = await firstValueFrom(
      this.http.post<MeResponse>(
        this.url('/auth/login'),
        { username, password },
        { withCredentials: true },
      ),
    );
    this.currentUserSig.set(res.user);
    return res.user;
  }

  private async realLogout(): Promise<void> {
    try {
      await firstValueFrom(
        this.http.post(this.url('/auth/logout'), null, {
          withCredentials: true,
        }),
      );
    } finally {
      this.currentUserSig.set(null);
    }
  }

  private async realLoadMe(): Promise<AuthUser | null> {
    try {
      const res = await firstValueFrom(
        this.http.get<MeResponse>(this.url('/auth/me'), {
          withCredentials: true,
        }),
      );
      this.currentUserSig.set(res.user);
      return res.user;
    } catch {
      this.currentUserSig.set(null);
      return null;
    }
  }

  private async mockLogin(
    username: string,
    password: string,
  ): Promise<AuthUser> {
    const ok = username.trim().toLowerCase() === 'admin' && password === 'admin';
    if (!ok) {
      throw new Error('Credenciais de teste inválidas — use admin / admin');
    }
    const user: AuthUser = { username: 'admin', roles: ['admin'] };
    localStorage.setItem(MOCK_SESSION_KEY, JSON.stringify(user));
    this.currentUserSig.set(user);
    // Best-effort: also open a real BFF session so authenticated /api/* proxy
    // calls (insights, finops, actions) work locally. Ignored if BFF is down —
    // the mock UI still works, API-backed panels just show their error state.
    await this.ensureBffSession();
    return user;
  }

  /**
   * Ensures a valid BFF cookie session exists (probe /auth/me, log in with the
   * local admin/admin if missing). Signal-safe: it never mutates `currentUser`,
   * so the mock session stays intact even when the BFF is unreachable.
   */
  private async ensureBffSession(): Promise<void> {
    try {
      await firstValueFrom(
        this.http.get(this.url('/auth/me'), { withCredentials: true }),
      );
      return; // cookie already valid
    } catch {
      /* no/expired cookie — try to establish one below */
    }
    try {
      await firstValueFrom(
        this.http.post(
          this.url('/auth/login'),
          { username: 'admin', password: 'admin' },
          { withCredentials: true },
        ),
      );
    } catch {
      /* BFF unreachable — stay on the local mock session */
    }
  }

  private async mockLogout(): Promise<void> {
    localStorage.removeItem(MOCK_SESSION_KEY);
    this.currentUserSig.set(null);
  }

  private async mockLoadMe(): Promise<AuthUser | null> {
    const user = this.readMockSession();
    this.currentUserSig.set(user);
    // Refresh the BFF cookie in the background so API-backed panels keep working
    // after a reload; does not gate the guard decision.
    if (user) {
      void this.ensureBffSession();
    }
    return user;
  }

  private readMockSession(): AuthUser | null {
    const raw = localStorage.getItem(MOCK_SESSION_KEY);
    if (!raw) {
      return null;
    }
    try {
      const parsed = JSON.parse(raw) as Partial<AuthUser> | null;
      if (parsed && typeof parsed.username === 'string') {
        return {
          username: parsed.username,
          roles: Array.isArray(parsed.roles) ? parsed.roles : [],
        };
      }
      return null;
    } catch {
      return null;
    }
  }

  private url(path: string): string {
    return `${this.settings.baseUrl()}${path}`;
  }
}
