import { ChangeDetectionStrategy, Component, inject, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { SettingsService } from '../core/settings.service';

/** Lets the operator configure the API Gateway base URL (persisted to localStorage). */
@Component({
  selector: 'app-settings',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [FormsModule],
  template: `
    <div class="container">
      <h1>Settings</h1>

      <div class="card">
        <div class="field">
          <label>Base URL do API Gateway</label>
          <input
            type="url"
            [(ngModel)]="draft"
            placeholder="https://api.example.com/v1"
          />
          <div class="hint">
            As ações são enviadas para <code>{{ draft() }}{{ '{path}' }}</code>.
          </div>
        </div>
        <div class="toolbar">
          <button class="primary" (click)="save()">Salvar</button>
          @if (saved()) {
            <span class="badge ok">Salvo</span>
          }
        </div>
      </div>

      <div class="note">
        Autenticação (Active Directory / Cognito) será adicionada depois. Um
        <code>authInterceptor</code> placeholder já está conectado para receber o
        token quando o login estiver disponível.
      </div>
    </div>
  `,
})
export class SettingsComponent {
  private readonly settings = inject(SettingsService);

  readonly draft = signal<string>(this.settings.baseUrl());
  readonly saved = signal(false);

  save(): void {
    this.settings.setBaseUrl(this.draft());
    this.draft.set(this.settings.baseUrl());
    this.saved.set(true);
    setTimeout(() => this.saved.set(false), 1500);
  }
}
