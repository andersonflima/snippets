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

  /** Clear the session on the BFF and locally. */
  async logout(): Promise<void> {
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

  /**
   * Hydrate `currentUser` from the cookie session, if any. Returns the user when
   * authenticated, or null when the BFF answers 401.
   */
  async loadMe(): Promise<AuthUser | null> {
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

  private url(path: string): string {
    return `${this.settings.baseUrl()}${path}`;
  }
}
