import {
  ChangeDetectionStrategy,
  Component,
  computed,
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

/** Renders one integration's dynamic form, a dry-run toggle, submit and response. */
@Component({
  selector: 'app-run-action',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [ReactiveFormsModule, RouterLink, DynamicFormComponent],
  template: `
    <div class="container">
      @if (integration(); as it) {
        <p><a routerLink="/">← Integrações</a></p>
        <h1>{{ it.name }}</h1>
        <div class="toolbar" style="margin-bottom: 0.5rem">
          <span class="method">{{ it.method }}</span>
          <span class="path">{{ it.path }}</span>
        </div>
        @if (it.summary) {
          <p class="muted">{{ it.summary }}</p>
        }

        <div class="note">
          Dry run valida a ação sem executá-la (envia <code>dryRun: true</code>).
          Confirme antes de desligar o dry run em produção.
        </div>

        <form [formGroup]="form()" (ngSubmit)="submit()">
          <div class="card">
            <app-dynamic-form [schema]="it.requestSchema" [group]="form()" />
          </div>

          <div class="card toolbar">
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
              {{ submitting() ? 'Enviando…' : 'Submeter' }}
            </button>
            <span class="muted">POST {{ baseUrl() }}{{ it.path }}</span>
          </div>
        </form>

        @if (result(); as r) {
          <div class="card">
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
        <div class="container">
          <p>Integração não encontrada.</p>
          <p><a routerLink="/">← Voltar para Integrações</a></p>
        </div>
      }
    </div>
  `,
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
