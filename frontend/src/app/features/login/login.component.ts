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

/** Error envelope returned by the BFF on a failed login. */
interface ErrorEnvelope {
  code: string;
  message: string;
  requestId?: string;
}

/** Username/password form that authenticates via the BFF and lands on home. */
@Component({
  selector: 'app-login',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [FormsModule],
  template: `
    <div class="container">
      <h1>Entrar</h1>

      <form class="card" (ngSubmit)="submit()">
        <div class="field">
          <label>Usuário</label>
          <input
            type="text"
            name="username"
            autocomplete="username"
            [(ngModel)]="username"
          />
        </div>
        <div class="field">
          <label>Senha</label>
          <input
            type="password"
            name="password"
            autocomplete="current-password"
            [(ngModel)]="password"
          />
        </div>

        @if (error(); as msg) {
          <div class="note err">{{ msg }}</div>
        }

        <div class="toolbar">
          <button
            type="submit"
            class="primary"
            [disabled]="submitting() || !username() || !password()"
          >
            {{ submitting() ? 'Entrando…' : 'Entrar' }}
          </button>
        </div>

        <p class="hint">Login de teste: <strong>admin</strong> / <strong>admin</strong></p>
      </form>
    </div>
  `,
})
export class LoginComponent {
  private readonly auth = inject(AuthService);
  private readonly router = inject(Router);

  readonly username = signal('admin');
  readonly password = signal('admin');
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
