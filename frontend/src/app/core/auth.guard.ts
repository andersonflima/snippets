import { inject } from '@angular/core';
import { CanActivateFn, Router } from '@angular/router';
import { AuthService } from './auth.service';

/**
 * Functional guard for routes that require a BFF session. Uses the in-memory
 * user when present, otherwise probes `/auth/me` (cookie session); on failure it
 * redirects to `/login`.
 */
export const authGuard: CanActivateFn = async () => {
  const auth = inject(AuthService);
  const router = inject(Router);

  if (auth.currentUser()) {
    return true;
  }

  const user = await auth.loadMe();
  return user ? true : router.createUrlTree(['/login']);
};
