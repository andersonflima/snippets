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
import { IconComponent } from '../shared/icon.component';
import { ToastService } from '../shared/toast.service';

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
  imports: [FormsModule, IconComponent],
  template: `
    <div class="page-head anim-in">
        <div>
          <h1>Configurações</h1>
          <p class="muted">
            Endpoint do BFF e perfil AWS por ambiente ·
            ativo <strong>{{ activeEnv() }}</strong>
          </p>
        </div>
      </div>

      <section class="card anim-in" style="animation-delay: 40ms">
        <div class="settings-head">
          <app-icon name="server" [size]="18" class="muted" />
          <h2>Conexão</h2>
        </div>
        <div class="field">
          <label for="set-baseurl">Base URL do BFF</label>
          <input
            id="set-baseurl"
            type="text"
            [(ngModel)]="baseUrlDraft"
            placeholder="/bff"
            autocomplete="off"
            spellcheck="false"
          />
          <div class="hint">
            As ações são enviadas para
            <code>{{ baseUrlDraft() }}/api{{ '{path}' }}</code>.
          </div>
        </div>
        <div class="toolbar">
          <button class="primary" type="button" (click)="saveBaseUrl()">
            <app-icon name="check" [size]="15" /> Salvar
          </button>
          @if (baseUrlSaved()) {
            <span class="badge ok save-flash">Salvo</span>
          }
        </div>
      </section>

      <section class="card anim-in" style="animation-delay: 90ms">
        <div class="settings-head">
          <app-icon name="layers" [size]="18" class="muted" />
          <h2>Ambiente ativo</h2>
        </div>
        <div class="field">
          <div
            class="segmented"
            role="radiogroup"
            aria-label="Selecionar ambiente ativo"
          >
            @for (env of envOptions; track env) {
              <button
                type="button"
                role="radio"
                [attr.aria-checked]="env === activeEnv()"
                [class.active]="env === activeEnv()"
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
      </section>

      <section class="card anim-in" style="animation-delay: 140ms">
        <div class="settings-head">
          <app-icon name="shield" [size]="18" class="muted" />
          <h2>Perfil AWS · <span class="badge">{{ activeEnv() }}</span></h2>
        </div>

        <div class="field">
          <label for="set-account">Conta AWS <span class="req">*</span></label>
          <input
            id="set-account"
            type="text"
            inputmode="numeric"
            [(ngModel)]="accountDraft"
            placeholder="000000000000"
            autocomplete="off"
            [attr.aria-invalid]="accountDraft() && !accountValid() ? 'true' : null"
          />
          @if (accountDraft() && !accountValid()) {
            <div class="error" role="alert">A conta deve ter exatamente 12 dígitos.</div>
          } @else {
            <div class="hint">12 dígitos numéricos.</div>
          }
        </div>

        <div class="field">
          <label for="set-region">Região</label>
          <input
            id="set-region"
            type="text"
            [(ngModel)]="regionDraft"
            placeholder="sa-east-1"
            autocomplete="off"
            spellcheck="false"
          />
          <div class="hint">Ex.: <code>sa-east-1</code>.</div>
        </div>

        <div class="field">
          <label for="set-arn">Role ARN <span class="req">*</span></label>
          <input
            id="set-arn"
            type="text"
            [(ngModel)]="roleArnDraft"
            placeholder="arn:aws:iam::000000000000:role/insights-demo"
            autocomplete="off"
            spellcheck="false"
            [attr.aria-invalid]="roleArnDraft() && !roleArnValid() ? 'true' : null"
          />
          @if (roleArnDraft() && !roleArnValid()) {
            <div class="error" role="alert">
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
            type="button"
            [disabled]="!profileValid()"
            (click)="saveProfile()"
          >
            <app-icon name="check" [size]="15" /> Salvar perfil
          </button>
          @if (profileSaved()) {
            <span class="badge ok save-flash">Salvo</span>
          }
        </div>

        <div class="hint">
          Estes valores pré-preenchem os formulários de ações, FinOps e Recursos.
          Deixe em branco para usar dados de demonstração localmente.
        </div>
      </section>

      <div class="note anim-in" style="animation-delay: 190ms">
        O BFF é o único backend acessado pelo frontend e cuida da autenticação:
        emite o JWT e o guarda num cookie httpOnly. As chamadas vão com
        <code>withCredentials</code>; o navegador nunca vê o token.
      </div>
  `,
  styles: [
    `
      .settings-head {
        display: flex;
        align-items: center;
        gap: 0.55rem;
        margin-bottom: 0.9rem;
      }
      .settings-head h2 {
        margin: 0;
        font-size: 1.05rem;
      }
      .primary app-icon {
        vertical-align: -2px;
      }
      .save-flash {
        animation: save-flash 0.4s ease-out;
      }
      @keyframes save-flash {
        from {
          opacity: 0;
          transform: scale(0.9);
        }
        to {
          opacity: 1;
          transform: scale(1);
        }
      }
      @media (prefers-reduced-motion: reduce) {
        .save-flash {
          animation: none;
        }
      }
    `,
  ],
})
export class SettingsComponent {
  private readonly settings = inject(SettingsService);
  private readonly toast = inject(ToastService);

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
    this.toast.success('Base URL salva', `${this.settings.baseUrl()}/api`);
    setTimeout(() => this.baseUrlSaved.set(false), 1500);
  }

  selectEnv(env: Env): void {
    this.settings.setActiveEnv(env);
    this.toast.info('Ambiente ativo', `Perfil padrão agora é ${env}.`);
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
    this.toast.success('Perfil salvo', `Conta ${draft.account} · ${this.activeEnv()}`);
    setTimeout(() => this.profileSaved.set(false), 1500);
  }
}
