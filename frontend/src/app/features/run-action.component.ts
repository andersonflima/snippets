import {
  ChangeDetectionStrategy,
  Component,
  computed,
  effect,
  inject,
  signal,
} from '@angular/core';
import { FormGroup, ReactiveFormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { toSignal } from '@angular/core/rxjs-interop';
import { ActivatedRoute } from '@angular/router';
import { map } from 'rxjs';
import { RegistryService } from '../core/registry.service';
import { ApiClientService, ActionResult } from '../core/api-client.service';
import { Integration } from '../core/openapi-parser';
import { buildFormGroup } from '../dynamic-form/form-schema';
import { buildPayload } from '../core/payload-builder';
import { DynamicFormComponent } from '../dynamic-form/dynamic-form.component';
import { SettingsService } from '../core/settings.service';
import { IconComponent } from '../shared/icon.component';

/** Renders one integration's dynamic form, a dry-run toggle, submit and response. */
@Component({
  selector: 'app-run-action',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [ReactiveFormsModule, RouterLink, DynamicFormComponent, IconComponent],
  template: `
    @if (integration(); as it) {
        <div class="page-head anim-in">
          <div>
            <a routerLink="/" class="back-link">
              <app-icon name="close" [size]="14" /> Integrações
            </a>
            <h1>{{ it.name }}</h1>
            <div class="method-line">
              <span class="method" [attr.data-verb]="it.method">{{ it.method }}</span>
              <span class="path">{{ it.path }}</span>
            </div>
            @if (it.summary) {
              <p class="muted" style="margin: 0.35rem 0 0">{{ it.summary }}</p>
            }
          </div>
        </div>

        <div class="note anim-in" style="animation-delay: 40ms">
          <app-icon name="shield" [size]="15" class="muted" />
          Dry run valida a ação sem executá-la (envia <code>dryRun: true</code>).
          Confirme antes de desligar o dry run em produção.
        </div>

        <form [formGroup]="form()" (ngSubmit)="submit()">
          <div class="card anim-in" style="animation-delay: 90ms">
            <app-dynamic-form [schema]="it.requestSchema" [group]="form()" />
          </div>

          <div class="card toolbar anim-in" style="animation-delay: 140ms">
            <label class="toggle">
              <input
                type="checkbox"
                [checked]="dryRun()"
                (change)="toggleDryRun($event)"
              />
              Dry run
            </label>
            <button
              type="submit"
              class="primary"
              [disabled]="form().invalid || submitting()"
            >
              @if (submitting()) {
                <span class="spin" aria-hidden="true"></span> Enviando…
              } @else {
                <app-icon name="bolt" [size]="15" /> Submeter
              }
            </button>
            <span class="muted mono-hint">POST {{ baseUrl() }}{{ it.path }}</span>
          </div>
        </form>

        @if (result(); as r) {
          <div class="card anim-in" aria-live="polite">
            <h3>
              Resposta
              <span class="badge" [class.ok]="r.ok" [class.err]="!r.ok">
                {{ r.status }} {{ r.statusText }}
              </span>
            </h3>
            <pre class="response">{{ pretty(r.body) }}</pre>
          </div>
        }
      } @else {
        <div class="empty-state anim-in" role="status">
          <app-icon name="close" [size]="28" class="muted" />
          <h2>Integração não encontrada</h2>
          <p class="muted">O contrato ou operação não existe no registro atual.</p>
          <a routerLink="/" class="back-link">
            <app-icon name="close" [size]="14" /> Voltar para Integrações
          </a>
        </div>
      }
  `,
  styles: [
    `
      .back-link {
        display: inline-flex;
        align-items: center;
        gap: 0.3rem;
        font-size: 0.82rem;
        color: var(--muted);
        margin-bottom: 0.4rem;
      }
      .back-link:hover {
        color: var(--accent);
        text-decoration: none;
      }
      .back-link app-icon {
        transform: rotate(45deg);
      }
      .method-line {
        display: flex;
        align-items: center;
        gap: 0.55rem;
        margin-top: 0.35rem;
      }
      .note app-icon {
        vertical-align: -2px;
        margin-right: 0.2rem;
      }
      button app-icon {
        vertical-align: -2px;
      }
      .mono-hint {
        font-family: var(--font-mono);
        font-size: 0.78rem;
      }
      .spin {
        display: inline-block;
        width: 13px;
        height: 13px;
        border: 2px solid color-mix(in srgb, var(--on-accent) 40%, transparent);
        border-top-color: var(--on-accent);
        border-radius: 50%;
        animation: ra-spin 0.7s linear infinite;
        vertical-align: -1px;
      }
      @keyframes ra-spin {
        to {
          transform: rotate(360deg);
        }
      }
      .empty-state {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 0.5rem;
        padding: 3rem 1rem;
        text-align: center;
      }
      .empty-state h2 {
        margin: 0.3rem 0 0;
      }
      .empty-state p {
        margin: 0;
      }
      @media (prefers-reduced-motion: reduce) {
        .spin {
          animation: none;
        }
      }
    `,
  ],
})
export class RunActionComponent {
  private readonly route = inject(ActivatedRoute);
  private readonly registry = inject(RegistryService);
  private readonly api = inject(ApiClientService);
  private readonly settings = inject(SettingsService);

  readonly baseUrl = this.settings.baseUrl;

  private readonly params = toSignal(
    this.route.paramMap.pipe(
      map((p) => ({
        contractId: p.get('contractId') ?? '',
        opId: p.get('opId') ?? '',
      })),
    ),
    { initialValue: { contractId: '', opId: '' } },
  );

  /** Prefill hints passed by the resource-detail quick actions (optional). */
  private readonly prefillParams = toSignal(
    this.route.queryParamMap.pipe(
      map((q) => ({
        resource: q.get('resource') ?? '',
        region: q.get('region') ?? '',
      })),
    ),
    { initialValue: { resource: '', region: '' } },
  );

  readonly integration = computed<Integration | undefined>(() => {
    const { contractId, opId } = this.params();
    return this.registry.findIntegration(contractId, opId);
  });

  /** Form group rebuilt whenever the integration changes. */
  readonly form = computed<FormGroup>(() => {
    const it = this.integration();
    return it ? buildFormGroup(it.requestSchema) : new FormGroup({});
  });

  readonly dryRun = computed<boolean>(() => {
    const control = this.form().get('dryRun');
    return control ? Boolean(control.value) : false;
  });

  readonly submitting = signal(false);
  readonly result = signal<ActionResult | null>(null);

  constructor() {
    // Prefill each time the form is (re)created for a route, without clobbering
    // values the operator already typed. Reads `form()` and the query params so
    // it re-runs when the integration or the prefill hints change.
    effect(() => {
      const form = this.form();
      const hints = this.prefillParams();
      this.prefill(form, hints);
    });
  }

  /**
   * Prefill the AWS envelope + resource fields when the matching controls exist
   * and are still empty. Region prefers the query param, then the settings
   * profile. Standalone navigation (no query params) keeps working.
   */
  private prefill(form: FormGroup, hints: { resource: string; region: string }): void {
    const envelope = this.settings.awsEnvelope();
    this.patchIfEmpty(form, 'account', envelope.account);
    this.patchIfEmpty(form, 'roleArn', envelope.roleArn);
    this.patchIfEmpty(form, 'environment', envelope.environment);
    this.patchIfEmpty(form, 'region', hints.region || envelope.region);
    this.patchIfEmpty(form, 'resource', hints.resource);
  }

  /** Patch a control only when it exists, is empty, and a value is available. */
  private patchIfEmpty(form: FormGroup, name: string, value: string): void {
    if (!value) {
      return;
    }
    const control = form.get(name);
    if (!control) {
      return;
    }
    const current = control.value;
    const isEmpty =
      current === null || current === undefined || String(current).trim() === '';
    if (isEmpty) {
      control.patchValue(value);
    }
  }

  toggleDryRun(event: Event): void {
    const checked = (event.target as HTMLInputElement).checked;
    const control = this.form().get('dryRun');
    control?.setValue(checked);
  }

  async submit(): Promise<void> {
    const it = this.integration();
    const form = this.form();
    if (!it || form.invalid) {
      form.markAllAsTouched();
      return;
    }
    this.submitting.set(true);
    this.result.set(null);
    try {
      const payload = buildPayload(it.requestSchema, form.getRawValue());
      const res = await this.api.runAction(it.path, payload);
      this.result.set(res);
    } finally {
      this.submitting.set(false);
    }
  }

  pretty(body: unknown): string {
    if (typeof body === 'string') {
      return body;
    }
    try {
      return JSON.stringify(body, null, 2);
    } catch {
      return String(body);
    }
  }
}
