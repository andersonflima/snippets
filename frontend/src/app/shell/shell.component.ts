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
import { SettingsService } from '../core/settings.service';
import { ToastService } from '../shared/toast.service';

interface NavItem {
  path: string;
  label: string;
  icon: string;
  exact?: boolean;
}

interface NavGroup {
  title: string;
  items: NavItem[];
}

const NAV_GROUPS: NavGroup[] = [
  {
    title: 'Visão geral',
    items: [{ path: '/', label: 'Dashboard', icon: 'dashboard', exact: true }],
  },
  {
    title: 'Operações',
    items: [
      { path: '/resources', label: 'Recursos', icon: 'server' },
      { path: '/db-performance', label: 'Performance BD', icon: 'database' },
      { path: '/dbca', label: 'DBCA', icon: 'spark' },
      { path: '/finops', label: 'FinOps', icon: 'chart' },
    ],
  },
  {
    title: 'Governança',
    items: [
      { path: '/integrations', label: 'Integrações', icon: 'layers' },
      { path: '/gmud', label: 'GMUD', icon: 'shield' },
      { path: '/admin', label: 'Admin', icon: 'lock' },
    ],
  },
  {
    title: 'Sistema',
    items: [{ path: '/settings', label: 'Configurações', icon: 'gear' }],
  },
];

const NAV: NavItem[] = NAV_GROUPS.flatMap((g) => g.items);

/** App shell: fixed sidebar + sticky header wrapping the routed content. */
@Component({
  selector: 'app-shell',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterOutlet, RouterLink, RouterLinkActive, IconComponent],
  styles: [
    `
      .env-badge {
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
        padding: 0.34rem 0.7rem;
        border-radius: 999px;
        border: 1px solid var(--border);
        background: var(--panel-2);
        color: var(--text);
        font-size: 0.78rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        transition:
          border-color 0.15s ease,
          background-color 0.15s ease;
      }
      .env-badge:hover {
        text-decoration: none;
        border-color: var(--border-strong);
      }
      .env-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: var(--accent);
        box-shadow: 0 0 0 3px var(--soft-accent);
      }
      .env-badge[data-env='prod'] {
        color: var(--danger);
        border-color: color-mix(in srgb, var(--danger) 40%, var(--border));
        background: var(--soft-danger);
      }
      .env-badge[data-env='prod'] .env-dot {
        background: var(--danger);
        box-shadow: 0 0 0 3px var(--soft-danger);
      }
      .env-badge[data-env='staging'] {
        color: var(--warn);
        border-color: color-mix(in srgb, var(--warn) 40%, var(--border));
        background: var(--soft-warn);
      }
      .env-badge[data-env='staging'] .env-dot {
        background: var(--warn);
        box-shadow: 0 0 0 3px var(--soft-warn);
      }
      @media (max-width: 640px) {
        .env-name {
          display: none;
        }
        .env-badge {
          padding: 0.34rem 0.5rem;
        }
      }
    `,
  ],
  template: `
    <div class="shell" [class.collapsed]="collapsed()" [class.mobile-open]="mobileOpen()">
      <aside class="sidebar">
        <div class="brand">
          <span class="logo"><app-icon name="server" [size]="18" /></span>
          <span class="brand-text">Cloud<b>Control</b></span>
        </div>

        <nav class="nav">
          @for (group of navGroups; track group.title) {
            <span class="nav-section nav-label">{{ group.title }}</span>
            @for (item of group.items; track item.path) {
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
            <span class="appbar-eyebrow">CONTROL PLANE</span>
            <h2>{{ title() }}</h2>
          </div>

          <div class="appbar-actions">
            <a class="env-badge" [attr.data-env]="activeEnv()" routerLink="/settings" title="Ambiente ativo — trocar em Configurações">
              <span class="env-dot"></span>
              <span class="env-name">{{ activeEnv() }}</span>
            </a>
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
  private readonly settings = inject(SettingsService);
  private readonly toast = inject(ToastService);
  readonly theme = inject(ThemeService);

  readonly navGroups = NAV_GROUPS;
  readonly activeEnv = this.settings.activeEnv;
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
