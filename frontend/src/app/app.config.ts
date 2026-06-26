import {
  ApplicationConfig,
  provideZonelessChangeDetection,
  inject,
  provideAppInitializer,
} from '@angular/core';
import { provideRouter } from '@angular/router';
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { routes } from './app.routes';
import { authInterceptor } from './core/auth.interceptor';
import { RegistryService } from './core/registry.service';

export const appConfig: ApplicationConfig = {
  providers: [
    provideZonelessChangeDetection(),
    provideRouter(routes),
    // authInterceptor sends the httpOnly session cookie and bounces 401s to /login.
    provideHttpClient(withInterceptors([authInterceptor])),
    // Load the versioned registry seed before the app renders.
    provideAppInitializer(() => inject(RegistryService).load()),
  ],
};
