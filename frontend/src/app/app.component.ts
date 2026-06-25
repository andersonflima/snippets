import { ChangeDetectionStrategy, Component, inject } from '@angular/core';
import { RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';
import { SettingsService } from './core/settings.service';

/** Application shell: top navigation and router outlet. */
@Component({
  selector: 'app-root',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterOutlet, RouterLink, RouterLinkActive],
  template: `
    <header class="topnav">
      <span class="brand">microserviços · actions</span>
      <nav>
        <a routerLink="/" routerLinkActive="active" [routerLinkActiveOptions]="{ exact: true }">
          Integrações
        </a>
        <a routerLink="/admin" routerLinkActive="active">Admin</a>
        <a routerLink="/settings" routerLinkActive="active">Settings</a>
      </nav>
      <span class="baseurl">{{ baseUrl() }}</span>
    </header>
    <router-outlet />
  `,
})
export class AppComponent {
  private readonly settings = inject(SettingsService);
  readonly baseUrl = this.settings.baseUrl;
}
