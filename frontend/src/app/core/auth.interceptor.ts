import { HttpInterceptorFn } from '@angular/common/http';

/**
 * Auth interceptor placeholder.
 *
 * No authentication is wired today. This hook exists so Active Directory /
 * Cognito JWT can be plugged in later without touching call sites.
 *
 * TODO(auth): acquire the bearer token (AD/Cognito) and attach it, e.g.:
 *   const token = inject(AuthService).accessToken();
 *   if (token) {
 *     req = req.clone({ setHeaders: { Authorization: `Bearer ${token}` } });
 *   }
 */
export const authInterceptor: HttpInterceptorFn = (req, next) => {
  return next(req);
};
