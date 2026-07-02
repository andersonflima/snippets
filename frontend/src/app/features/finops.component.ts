import {
  ChangeDetectionStrategy,
  Component,
  computed,
  inject,
  signal,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActionResult } from '../core/api-client.service';
import { SettingsService } from '../core/settings.service';
import {
  FINOPS_REGION,
  FinopsResult,
  FinopsScope,
  FinopsService,
} from './finops.service';

interface FinopsDetailKeyValue {
  key: string;
  value: number;
}

/** Scans a client AWS account for cost-saving opportunities via the BFF. */
@Component({
  selector: 'app-finops',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [FormsModule],
  template: `
    <div class="container">
      <h1>FinOps</h1>
      <p class="muted">
        Analisa uma conta AWS cliente em busca de oportunidades de economia
        (somente leitura).
      </p>

      <div class="card">
        <div class="field">
          <label>Conta AWS (12 dígitos)</label>
          <input
            type="text"
            inputmode="numeric"
            maxlength="12"
            placeholder="123456789012"
            [(ngModel)]="account"
          />
        </div>

        <div class="field">
          <label>Role ARN</label>
          <input
            type="text"
            placeholder="arn:aws:iam::123456789012:role/finops-readonly"
            [(ngModel)]="roleArn"
          />
        </div>

        <div class="field">
          <label>Escopo</label>
          <select [(ngModel)]="scope">
            @for (opt of scopes; track opt) {
              <option [value]="opt">{{ opt }}</option>
            }
          </select>
        </div>

        <div class="field">
          <label>Lookback (dias)</label>
          <input type="number" min="1" [(ngModel)]="lookbackDays" />
        </div>

        <div class="field">
          <label>Região</label>
          <span class="badge">{{ region }}</span>
        </div>

        <div class="toolbar">
          <button
            class="primary"
            [disabled]="!canSubmit() || loading()"
            (click)="analyze()"
          >
            {{ loading() ? 'Analisando…' : 'Analisar' }}
          </button>
          <span class="muted">POST {{ baseUrl() }}/api/finops/execute</span>
        </div>
      </div>

      @if (loading()) {
        <div class="card muted">Analisando a conta…</div>
      }

      @if (errorResult(); as err) {
        <div class="card">
          <h3>
            Falha na análise
            <span class="badge err">{{ err.status }} {{ err.statusText }}</span>
          </h3>
          <pre class="response">{{ pretty(err.body) }}</pre>
        </div>
      }

      @if (detail(); as d) {
        <div class="card">
          <div class="row">
            <strong>Economia mensal estimada</strong>
            <span class="badge ok">
              {{ d.summary.estimatedMonthlySavings }} {{ d.summary.currency }}
            </span>
          </div>
          <div class="row">
            <span>Oportunidades encontradas</span>
            <span>{{ d.summary.findingsCount }}</span>
          </div>
          @for (entry of byResourceType(); track entry.key) {
            <div class="row">
              <span class="muted">{{ entry.key }}</span>
              <span>{{ entry.value }}</span>
            </div>
          }
        </div>

        @if (d.findings.length === 0) {
          <div class="card muted">Nenhuma oportunidade encontrada.</div>
        }

        @for (f of d.findings; track f.resourceId) {
          <div class="card">
            <div class="row">
              <div>
                <span class="method">{{ f.resourceType }}</span>
                <span class="path">{{ f.resourceId }}</span>
              </div>
              <span class="badge" [class]="severityClass(f.severity)">
                {{ f.severity }}
              </span>
            </div>
            <p class="muted" style="margin: 0.4rem 0 0.2rem">{{ f.issue }}</p>
            <p style="margin: 0.2rem 0">{{ f.recommendation }}</p>
            <div class="row">
              <span class="muted">Economia mensal estimada</span>
              <span class="badge ok">
                {{ f.estimatedMonthlySavings }} {{ d.summary.currency }}
              </span>
            </div>
          </div>
        }

        @if (d.notes.length > 0) {
          <div class="card">
            <h3>Notas</h3>
            @for (note of d.notes; track note) {
              <p class="muted" style="margin: 0.2rem 0">{{ note }}</p>
            }
          </div>
        }
      }
    </div>
  `,
})
export class FinOpsComponent {
  private readonly finops = inject(FinopsService);
  private readonly settings = inject(SettingsService);

  readonly baseUrl = this.settings.baseUrl;
  readonly region = FINOPS_REGION;
  readonly scopes: FinopsScope[] = [
    'all',
    'rds',
    'ec2',
    'ebs',
    'eip',
    'elb',
    'snapshots',
  ];

  readonly account = signal('');
  readonly roleArn = signal('');
  readonly scope = signal<FinopsScope>('all');
  readonly lookbackDays = signal(14);

  readonly loading = signal(false);
  private readonly result = signal<ActionResult | null>(null);

  readonly canSubmit = computed(
    () => /^\d{12}$/.test(this.account().trim()) && this.roleArn().trim() !== '',
  );

  readonly errorResult = computed<ActionResult | null>(() => {
    const r = this.result();
    return r && !r.ok ? r : null;
  });

  readonly detail = computed(() => {
    const r = this.result();
    if (!r || !r.ok) {
      return null;
    }
    return (r.body as FinopsResult | null)?.detail ?? null;
  });

  readonly byResourceType = computed<FinopsDetailKeyValue[]>(() => {
    const map = this.detail()?.summary.byResourceType ?? {};
    return Object.entries(map).map(([key, value]) => ({ key, value }));
  });

  async analyze(): Promise<void> {
    if (!this.canSubmit() || this.loading()) {
      return;
    }
    this.loading.set(true);
    this.result.set(null);
    try {
      const res = await this.finops.analyze({
        account: this.account().trim(),
        roleArn: this.roleArn().trim(),
        scope: this.scope(),
        lookbackDays: this.lookbackDays(),
      });
      this.result.set(res);
    } finally {
      this.loading.set(false);
    }
  }

  severityClass(severity: string): string {
    if (severity === 'high') {
      return 'err';
    }
    if (severity === 'low') {
      return 'ok';
    }
    return '';
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
