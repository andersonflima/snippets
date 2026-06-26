import { ChangeDetectionStrategy, Component, inject } from '@angular/core';
import { Router, RouterLink, RouterLinkActive, RouterOutlet } from '@angular/router';
import { SettingsService } from './core/settings.service';
import { AuthService } from './core/auth.service';

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
      @if (currentUser(); as user) {
        <span class="user">{{ user.username }}</span>
        <button type="button" (click)="logout()">Sair</button>
      }
    </header>
    <router-outlet />
  `,
})
export class AppComponent {
  private readonly settings = inject(SettingsService);
  private readonly auth = inject(AuthService);
  private readonly router = inject(Router);

  readonly baseUrl = this.settings.baseUrl;
  readonly currentUser = this.auth.currentUser;

  async logout(): Promise<void> {
    await this.auth.logout();
    await this.router.navigateByUrl('/login');
  }
}
