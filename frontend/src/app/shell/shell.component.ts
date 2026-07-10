import {
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  computed,
  inject,
  signal,
} from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import {
  NavigationEnd,
  Router,
  RouterLink,
  RouterLinkActive,
  RouterOutlet,
} from '@angular/router';
import { filter } from 'rxjs';
import { IconComponent } from '../shared/icon.component';
import { ThemeService } from '../core/theme.service';
import { AuthService } from '../core/auth.service';
import { ToastService } from '../shared/toast.service';

interface NavItem {
  path: string;
  label: string;
  icon: string;
  exact?: boolean;
}

const NAV: NavItem[] = [
  { path: '/', label: 'Dashboard', icon: 'dashboard', exact: true },
  { path: '/resources', label: 'Recursos', icon: 'server' },
  { path: '/db-performance', label: 'Performance BD', icon: 'database' },
  { path: '/finops', label: 'FinOps', icon: 'chart' },
  { path: '/integrations', label: 'Integrações', icon: 'layers' },
  { path: '/gmud', label: 'GMUD', icon: 'shield' },
  { path: '/admin', label: 'Admin', icon: 'shield' },
  { path: '/settings', label: 'Configurações', icon: 'gear' },
];

/** App shell: fixed sidebar + sticky header wrapping the routed content. */
@Component({
  selector: 'app-shell',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterOutlet, RouterLink, RouterLinkActive, IconComponent],
  template: `
    <div class="shell" [class.collapsed]="collapsed()" [class.mobile-open]="mobileOpen()">
      <aside class="sidebar">
        <div class="brand">
          <span class="logo"><app-icon name="bolt" [size]="18" /></span>
          <span class="brand-text">Actions<b>Console</b></span>
        </div>

        <span class="nav-section nav-label">Navegação</span>
        <nav class="nav">
          @for (item of nav; track item.path) {
            <a
              [routerLink]="item.path"
              routerLinkActive="active"
              [routerLinkActiveOptions]="{ exact: !!item.exact }"
              (click)="mobileOpen.set(false)"
            >
              <app-icon [name]="item.icon" [size]="19" />
              <span class="nav-label">{{ item.label }}</span>
            </a>
          }
        </nav>

        <div class="side-foot">
          <div class="sys-status">
            <span class="sys-dot"></span>
            <div class="sys-text nav-label">
              <b>Sistema operacional</b>
              <span>todos os serviços ativos</span>
            </div>
          </div>
          <button type="button" class="ghost collapse-btn" (click)="collapsed.set(!collapsed())">
            <app-icon [name]="collapsed() ? 'layers' : 'menu'" [size]="18" />
            <span class="nav-label">Recolher</span>
          </button>
        </div>
      </aside>

      <div class="scrim" (click)="mobileOpen.set(false)"></div>

      <div class="main">
        <header class="appbar">
          <button type="button" class="ghost icon-btn burger" (click)="mobileOpen.set(!mobileOpen())">
            <app-icon name="menu" [size]="20" />
          </button>
          <div class="appbar-title">
            <span class="appbar-eyebrow">CONSOLE</span>
            <h2>{{ title() }}</h2>
          </div>

          <div class="appbar-actions">
            <button type="button" class="ghost icon-btn" title="Notificações" (click)="notifications()">
              <app-icon name="bell" [size]="18" />
            </button>
            <button
              type="button"
              class="ghost icon-btn"
              (click)="toggleTheme()"
              [title]="theme.theme() === 'dark' ? 'Tema claro' : 'Tema escuro'"
            >
              <app-icon [name]="theme.theme() === 'dark' ? 'sun' : 'moon'" [size]="18" />
            </button>

            @if (currentUser(); as user) {
              <div class="user-chip">
                <span class="avatar">{{ initials(user.username) }}</span>
                <span class="user-name">{{ user.username }}</span>
              </div>
              <button type="button" class="ghost icon-btn" (click)="logout()" title="Sair">
                <app-icon name="logout" [size]="18" />
              </button>
            }
          </div>
        </header>

        <main class="content">
          <router-outlet />
        </main>
      </div>
    </div>
  `,
})
export class ShellComponent {
  private readonly router = inject(Router);
  private readonly auth = inject(AuthService);
  private readonly toast = inject(ToastService);
  readonly theme = inject(ThemeService);

  readonly nav = NAV;
  readonly collapsed = signal(false);
  readonly mobileOpen = signal(false);
  readonly currentUser = this.auth.currentUser;

  private readonly url = signal(this.router.url);
  readonly title = computed(() => {
    const match = NAV.find((n) =>
      n.exact ? this.url() === n.path : this.url().startsWith(n.path),
    );
    return match?.label ?? 'Console';
  });

  constructor() {
    this.router.events
      .pipe(
        filter((e): e is NavigationEnd => e instanceof NavigationEnd),
        takeUntilDestroyed(inject(DestroyRef)),
      )
      .subscribe((e) => this.url.set(e.urlAfterRedirects));
  }

  initials(name: string): string {
    return name.slice(0, 2).toUpperCase();
  }

  toggleTheme(): void {
    this.theme.toggle();
    const mode = this.theme.theme();
    this.toast.info(mode === 'dark' ? 'Tema escuro ativado' : 'Tema claro ativado');
  }

  notifications(): void {
    this.toast.info('Notificações', 'Você está em dia — nada novo por aqui.');
  }

  async logout(): Promise<void> {
    await this.auth.logout();
    this.toast.success('Sessão encerrada', 'Você saiu com segurança.');
    await this.router.navigateByUrl('/login');
  }
}
