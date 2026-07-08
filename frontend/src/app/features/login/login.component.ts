import {
  ChangeDetectionStrategy,
  Component,
  inject,
  signal,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { HttpErrorResponse } from '@angular/common/http';
import { Router } from '@angular/router';
import { AuthService } from '../../core/auth.service';
import { ThemeService } from '../../core/theme.service';
import { IconComponent } from '../../shared/icon.component';
import { ToastService } from '../../shared/toast.service';

/** Error envelope returned by the BFF on a failed login. */
interface ErrorEnvelope {
  code: string;
  message: string;
  requestId?: string;
}

/** Branded, centered sign-in screen (theme-aware). */
@Component({
  selector: 'app-login',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [FormsModule, IconComponent],
  template: `
    <div class="auth">
      <button
        type="button"
        class="theme-toggle"
        (click)="theme.toggle()"
        [title]="theme.theme() === 'dark' ? 'Tema claro' : 'Tema escuro'"
      >
        <app-icon [name]="theme.theme() === 'dark' ? 'sun' : 'moon'" [size]="18" />
      </button>

      <form class="auth-card" (ngSubmit)="submit()">
        <div class="brand">
          <span class="logo"><app-icon name="bolt" [size]="22" /></span>
          <div class="brand-text">Actions<b>Console</b></div>
        </div>
        <p class="subtitle">Console de operações e análise dos produtos AWS</p>

        <label class="field">
          <span class="field-label">Usuário</span>
          <span class="control">
            <app-icon name="user" [size]="17" class="lead" />
            <input
              type="text"
              name="username"
              autocomplete="username"
              placeholder="seu usuário"
              [(ngModel)]="username"
            />
          </span>
        </label>

        <label class="field">
          <span class="field-label">Senha</span>
          <span class="control">
            <app-icon name="lock" [size]="17" class="lead" />
            <input
              [type]="showPassword() ? 'text' : 'password'"
              name="password"
              autocomplete="current-password"
              placeholder="••••••"
              [(ngModel)]="password"
            />
            <button
              type="button"
              class="reveal"
              (click)="showPassword.set(!showPassword())"
              [title]="showPassword() ? 'Ocultar senha' : 'Mostrar senha'"
            >
              <app-icon [name]="showPassword() ? 'eye-off' : 'eye'" [size]="16" />
            </button>
          </span>
        </label>

        @if (error(); as msg) {
          <div class="auth-error">{{ msg }}</div>
        }

        <button
          type="submit"
          class="submit"
          [disabled]="submitting() || !username() || !password()"
        >
          {{ submitting() ? 'Entrando…' : 'Entrar' }}
        </button>

        <p class="test-hint">
          <app-icon name="user" [size]="13" />
          Login de teste: <strong>admin</strong> / <strong>admin</strong>
        </p>
      </form>

      <p class="auth-foot muted">Ambiente local · dados de demonstração</p>
    </div>
  `,
  styles: [
    `
      :host {
        display: block;
      }
      .auth {
        position: relative;
        min-height: 100vh;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        gap: 1rem;
        padding: 1.5rem;
        background:
          radial-gradient(60rem 40rem at 15% -10%, color-mix(in srgb, var(--accent) 16%, transparent), transparent 60%),
          radial-gradient(50rem 40rem at 100% 110%, color-mix(in srgb, var(--accent-2) 14%, transparent), transparent 60%),
          var(--bg);
      }
      .theme-toggle {
        position: absolute;
        top: 1.25rem;
        right: 1.25rem;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 38px;
        height: 38px;
        padding: 0;
        border-radius: 10px;
        border: 1px solid var(--border);
        background: color-mix(in srgb, var(--panel) 70%, transparent);
        color: var(--muted);
      }
      .theme-toggle:hover {
        color: var(--text);
        border-color: var(--accent);
      }
      .auth-card {
        width: 100%;
        max-width: 400px;
        background: var(--panel);
        border: 1px solid var(--border);
        border-radius: 16px;
        padding: 2rem 1.9rem 1.75rem;
        box-shadow: var(--shadow-lg, 0 24px 60px rgba(0, 0, 0, 0.4));
        animation: rise 0.5s ease-out both;
      }
      @keyframes rise {
        from {
          opacity: 0;
          transform: translateY(12px);
        }
      }
      .brand {
        display: flex;
        align-items: center;
        gap: 0.65rem;
      }
      .logo {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 40px;
        height: 40px;
        border-radius: 11px;
        color: #fff;
        background: linear-gradient(135deg, var(--accent), var(--accent-2));
        box-shadow: 0 6px 18px color-mix(in srgb, var(--accent) 45%, transparent);
      }
      .brand-text {
        font-size: 1.3rem;
        font-weight: 700;
        letter-spacing: 0.01em;
      }
      .brand-text b {
        color: var(--accent);
        font-weight: 700;
      }
      .subtitle {
        margin: 0.85rem 0 1.4rem;
        color: var(--muted);
        font-size: 0.9rem;
      }
      .field {
        display: block;
        margin-bottom: 0.95rem;
      }
      .field-label {
        display: block;
        font-size: 0.8rem;
        font-weight: 600;
        color: var(--muted);
        margin-bottom: 0.4rem;
      }
      .control {
        position: relative;
        display: flex;
        align-items: center;
      }
      .control .lead {
        position: absolute;
        left: 0.7rem;
        color: var(--muted);
        pointer-events: none;
      }
      .control input {
        width: 100%;
        padding: 0.7rem 0.75rem 0.7rem 2.35rem;
        background: var(--panel-2);
        border: 1px solid var(--border);
        border-radius: 10px;
        color: var(--text);
        font-size: 0.95rem;
        transition: border-color 0.15s, box-shadow 0.15s;
      }
      .control input:focus {
        outline: none;
        border-color: var(--accent);
        box-shadow: 0 0 0 3px color-mix(in srgb, var(--accent) 22%, transparent);
      }
      .reveal {
        position: absolute;
        right: 0.35rem;
        width: 32px;
        height: 32px;
        padding: 0;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        border: none;
        background: transparent;
        color: var(--muted);
        border-radius: 8px;
      }
      .reveal:hover {
        color: var(--text);
      }
      .auth-error {
        margin: 0.2rem 0 0.9rem;
        padding: 0.6rem 0.75rem;
        font-size: 0.85rem;
        color: var(--danger);
        background: color-mix(in srgb, var(--danger) 12%, transparent);
        border: 1px solid color-mix(in srgb, var(--danger) 35%, transparent);
        border-radius: 9px;
      }
      .submit {
        width: 100%;
        margin-top: 0.35rem;
        padding: 0.7rem 1rem;
        font-size: 0.95rem;
        font-weight: 700;
        color: #fff;
        border: none;
        border-radius: 10px;
        background: linear-gradient(135deg, var(--accent), var(--accent-2));
        box-shadow: 0 8px 20px color-mix(in srgb, var(--accent) 40%, transparent);
        transition: filter 0.15s, transform 0.05s;
      }
      .submit:hover:not(:disabled) {
        filter: brightness(1.07);
      }
      .submit:active:not(:disabled) {
        transform: translateY(1px);
      }
      .submit:disabled {
        opacity: 0.55;
        cursor: not-allowed;
      }
      .test-hint {
        display: flex;
        align-items: center;
        gap: 0.4rem;
        justify-content: center;
        margin: 1.1rem 0 0;
        padding: 0.5rem;
        font-size: 0.8rem;
        color: var(--muted);
        background: var(--panel-2);
        border: 1px dashed var(--border);
        border-radius: 9px;
      }
      .test-hint strong {
        color: var(--text);
      }
      .auth-foot {
        font-size: 0.78rem;
      }
      @media (prefers-reduced-motion: reduce) {
        .auth-card {
          animation: none;
        }
      }
    `,
  ],
})
export class LoginComponent {
  private readonly auth = inject(AuthService);
  private readonly router = inject(Router);
  private readonly toast = inject(ToastService);
  readonly theme = inject(ThemeService);

  readonly username = signal('admin');
  readonly password = signal('admin');
  readonly showPassword = signal(false);
  readonly submitting = signal(false);
  readonly error = signal<string | null>(null);

  async submit(): Promise<void> {
    if (this.submitting()) {
      return;
    }
    this.submitting.set(true);
    this.error.set(null);
    try {
      await this.auth.login(this.username(), this.password());
      this.toast.success('Bem-vindo, ' + this.username(), 'Login realizado com sucesso.');
      await this.router.navigateByUrl('/');
    } catch (err) {
      this.error.set(this.toMessage(err));
    } finally {
      this.submitting.set(false);
    }
  }

  private toMessage(err: unknown): string {
    if (err instanceof HttpErrorResponse) {
      const envelope = err.error as ErrorEnvelope | null;
      if (envelope?.message) {
        return envelope.message;
      }
    } else if (err instanceof Error && err.message) {
      return err.message;
    }
    return 'Não foi possível entrar. Tente novamente.';
  }
}
