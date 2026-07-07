import {
  ChangeDetectionStrategy,
  Component,
  computed,
  effect,
  inject,
  signal,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { AwsProfile, Env, SettingsService } from '../core/settings.service';

const ACCOUNT_RE = /^\d{12}$/;
const ROLE_ARN_RE = /^arn:aws:iam::\d{12}:role\/.+$/;

const ENV_OPTIONS: readonly Env[] = ['dev', 'homol', 'staging', 'prod'];

/**
 * Lets the operator configure the BFF base URL and the AWS connection profile
 * for each environment. Profiles prefill the action forms, FinOps and Recursos
 * features; blank values fall back to demo data locally. All persisted via
 * SettingsService (localStorage).
 */
@Component({
  selector: 'app-settings',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [FormsModule],
  template: `
    <div class="container">
      <h1>Settings</h1>

      <div class="card">
        <h2>Conexão</h2>
        <div class="field">
          <label>Base URL do BFF</label>
          <input type="text" [(ngModel)]="baseUrlDraft" placeholder="/bff" />
          <div class="hint">
            As ações são enviadas para
            <code>{{ baseUrlDraft() }}/api{{ '{path}' }}</code>.
          </div>
        </div>
        <div class="toolbar">
          <button class="primary" (click)="saveBaseUrl()">Salvar</button>
          @if (baseUrlSaved()) {
            <span class="badge ok">Salvo</span>
          }
        </div>
      </div>

      <div class="card">
        <h2>Ambiente ativo</h2>
        <div class="field">
          <div class="toolbar">
            @for (env of envOptions; track env) {
              <button
                type="button"
                [class.primary]="env === activeEnv()"
                (click)="selectEnv(env)"
              >
                {{ env }}
              </button>
            }
          </div>
          <div class="hint">
            Define qual perfil AWS será usado por padrão nas demais telas.
          </div>
        </div>
      </div>

      <div class="card">
        <h2>Perfil AWS por ambiente</h2>
        <div class="field">
          <label>Ambiente</label>
          <span class="badge">{{ activeEnv() }}</span>
        </div>

        <div class="field">
          <label>Conta AWS</label>
          <input
            type="text"
            inputmode="numeric"
            [(ngModel)]="accountDraft"
            placeholder="000000000000"
          />
          @if (accountDraft() && !accountValid()) {
            <div class="error">A conta deve ter exatamente 12 dígitos.</div>
          } @else {
            <div class="hint">12 dígitos numéricos.</div>
          }
        </div>

        <div class="field">
          <label>Região</label>
          <input
            type="text"
            [(ngModel)]="regionDraft"
            placeholder="sa-east-1"
          />
          <div class="hint">Ex.: <code>sa-east-1</code>.</div>
        </div>

        <div class="field">
          <label>Role ARN</label>
          <input
            type="text"
            [(ngModel)]="roleArnDraft"
            placeholder="arn:aws:iam::000000000000:role/insights-demo"
          />
          @if (roleArnDraft() && !roleArnValid()) {
            <div class="error">
              Formato esperado:
              <code>arn:aws:iam::&lt;12 dígitos&gt;:role/&lt;nome&gt;</code>.
            </div>
          } @else {
            <div class="hint">ARN de uma role IAM assumível.</div>
          }
        </div>

        <div class="toolbar">
          <button
            class="primary"
            [disabled]="!profileValid()"
            (click)="saveProfile()"
          >
            Salvar perfil
          </button>
          @if (profileSaved()) {
            <span class="badge ok">Salvo</span>
          }
        </div>

        <div class="hint">
          Estes valores pré-preenchem os formulários de ações, FinOps e Recursos.
          Deixe em branco para usar dados de demonstração localmente.
        </div>
      </div>

      <div class="note">
        O BFF é o único backend acessado pelo frontend e cuida da autenticação:
        emite o JWT e o guarda num cookie httpOnly. As chamadas vão com
        <code>withCredentials</code>; o navegador nunca vê o token.
      </div>
    </div>
  `,
})
export class SettingsComponent {
  private readonly settings = inject(SettingsService);

  readonly envOptions = ENV_OPTIONS;

  readonly activeEnv = this.settings.activeEnv;

  readonly baseUrlDraft = signal<string>(this.settings.baseUrl());
  readonly baseUrlSaved = signal(false);

  readonly accountDraft = signal<string>('');
  readonly regionDraft = signal<string>('');
  readonly roleArnDraft = signal<string>('');
  readonly profileSaved = signal(false);

  readonly accountValid = computed(() => ACCOUNT_RE.test(this.accountDraft().trim()));
  readonly roleArnValid = computed(() => ROLE_ARN_RE.test(this.roleArnDraft().trim()));
  readonly profileValid = computed(() => this.accountValid() && this.roleArnValid());

  constructor() {
    // Keep the profile draft in sync with the active environment's stored profile.
    effect(() => {
      const profile = this.settings.activeProfile();
      this.accountDraft.set(profile.account);
      this.regionDraft.set(profile.region);
      this.roleArnDraft.set(profile.roleArn);
    });
  }

  saveBaseUrl(): void {
    this.settings.setBaseUrl(this.baseUrlDraft());
    this.baseUrlDraft.set(this.settings.baseUrl());
    this.baseUrlSaved.set(true);
    setTimeout(() => this.baseUrlSaved.set(false), 1500);
  }

  selectEnv(env: Env): void {
    this.settings.setActiveEnv(env);
  }

  saveProfile(): void {
    if (!this.profileValid()) return;
    const draft: AwsProfile = {
      account: this.accountDraft().trim(),
      region: this.regionDraft().trim(),
      roleArn: this.roleArnDraft().trim(),
    };
    this.settings.setAwsProfile(this.activeEnv(), draft);
    this.profileSaved.set(true);
    setTimeout(() => this.profileSaved.set(false), 1500);
  }
}
