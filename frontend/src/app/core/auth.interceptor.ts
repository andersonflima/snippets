import { HttpErrorResponse, HttpInterceptorFn } from '@angular/common/http';
import { inject } from '@angular/core';
import { Router } from '@angular/router';
import { catchError, throwError } from 'rxjs';

/**
 * Auth interceptor for the cookie-based BFF session.
 *
 * The BFF keeps the JWT in an httpOnly cookie (`bff_session`), so every request
 * carries `withCredentials: true` to send it; there is no bearer header. On a
 * 401 the session is gone, so we bounce to `/login` — skipping the login call
 * itself to avoid a redirect loop while signing in.
 */
export const authInterceptor: HttpInterceptorFn = (req, next) => {
  const router = inject(Router);
  const authed = req.clone({ withCredentials: true });

  return next(authed).pipe(
    catchError((err: unknown) => {
      if (
        err instanceof HttpErrorResponse &&
        err.status === 401 &&
        !req.url.endsWith('/auth/login')
      ) {
        void router.navigateByUrl('/login');
      }
      return throwError(() => err);
    }),
  );
};
