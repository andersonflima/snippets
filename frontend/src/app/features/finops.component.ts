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
import { ThemeService } from '../core/theme.service';
import { EChartComponent } from '../shared/echart.component';
import { IconComponent } from '../shared/icon.component';
import { palette } from '../dashboard/chart-options';
import {
  savingsByTypeOptions,
  savingsTrendOptions,
  utilizationGaugeOptions,
  utilizationOptions,
} from './finops-charts';
import {
  FINOPS_REGION,
  FinopsAnalytics,
  FinopsResult,
  FinopsScope,
  FinopsService,
  FinopsUtilizationRow,
  FinopsVerdict,
} from './finops.service';

interface FinopsDetailKeyValue {
  key: string;
  value: number;
}

/** Selectable analysis windows for the rightsizing view. */
const PERIODS = [
  { d: 30, label: '30 dias' },
  { d: 90, label: '90 dias' },
  { d: 180, label: '180 dias' },
];

/**
 * Analytical FinOps screen focused on cost savings / rightsizing: KPIs, savings
 * charts and a "usage vs. provisioned" opportunities table. The legacy
 * findings-scan flow stays available under a toggle so nothing is lost.
 */
@Component({
  selector: 'app-finops',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [FormsModule, EChartComponent, IconComponent],
  template: `
    <div class="page-head anim-in">
      <div>
        <h1>FinOps</h1>
        <p class="muted">
          Rightsizing e economia — uso real de cada recurso vs. o potencial
          provisionado.
        </p>
      </div>
      <div class="segmented">
        @for (per of periods; track per.d) {
          <button
            type="button"
            [class.active]="lookbackDays() === per.d"
            [disabled]="analyticsLoading()"
            (click)="setPeriod(per.d)"
          >
            {{ per.label }}
          </button>
        }
      </div>
    </div>

    @if (analyticsError(); as err) {
      <div class="card fin-error anim-in">
        <app-icon name="bolt" [size]="18" />
        <div>
          <strong>Não foi possível carregar a análise</strong>
          <p class="muted">{{ err }}</p>
        </div>
        <button type="button" (click)="reload()">Tentar novamente</button>
      </div>
    }

    @if (analyticsLoading() && !analytics()) {
      <section class="kpi-grid">
        @for (s of [1, 2, 3, 4]; track s) {
          <article class="kpi skeleton"><span class="sk-line"></span></article>
        }
      </section>
      <div class="card muted">Analisando utilização dos recursos…</div>
    }

    @if (analytics(); as a) {
      <section class="kpi-grid">
        <article class="kpi anim-in">
          <div class="kpi-top">
            <span class="kpi-ic" data-tone="ok"><app-icon name="coin" /></span>
          </div>
          <div class="kpi-val">{{ money(a.summary.estimatedMonthlySavings, a.summary.currency) }}</div>
          <div class="kpi-label">Economia mensal estimada</div>
        </article>
        <article class="kpi anim-in" [style.animation-delay.ms]="60">
          <div class="kpi-top">
            <span class="kpi-ic" data-tone="danger"><app-icon name="leaf" /></span>
          </div>
          <div class="kpi-val">{{ a.summary.idleCount }}</div>
          <div class="kpi-label">Recursos ociosos</div>
        </article>
        <article class="kpi anim-in" [style.animation-delay.ms]="120">
          <div class="kpi-top">
            <span class="kpi-ic" data-tone="warn"><app-icon name="layers" /></span>
          </div>
          <div class="kpi-val">{{ a.summary.oversizedCount }}</div>
          <div class="kpi-label">Superdimensionados</div>
        </article>
        <article class="kpi anim-in" [style.animation-delay.ms]="180">
          <div class="kpi-top">
            <span class="kpi-ic" data-tone="accent"><app-icon name="server" /></span>
          </div>
          <div class="kpi-val">{{ a.summary.analyzedCount }}</div>
          <div class="kpi-label">Recursos analisados</div>
        </article>
      </section>

      <section class="chart-grid">
        <article class="card panel span-2 anim-in">
          <header class="panel-head">
            <h3>Economia ao longo do tempo</h3>
            <span class="muted">custo vs. economia (US$)</span>
          </header>
          @if (a.savingsTrend.length) {
            <app-echart [options]="trendOpts()" height="300px" />
          } @else {
            <p class="muted fin-empty">Sem histórico disponível.</p>
          }
        </article>

        <article class="card panel anim-in">
          <header class="panel-head"><h3>Utilização média</h3></header>
          <app-echart [options]="gaugeOpts()" height="300px" />
        </article>

        <article class="card panel anim-in">
          <header class="panel-head">
            <h3>Economia por tipo</h3>
            <span class="muted">US$/mês</span>
          </header>
          @if (a.savingsByType.length) {
            <app-echart [options]="byTypeOpts()" height="320px" />
          } @else {
            <p class="muted fin-empty">Sem dados por tipo.</p>
          }
        </article>

        <article class="card panel span-2 anim-in">
          <header class="panel-head">
            <h3>Uso vs. provisionado</h3>
            <span class="muted">utilização % por recurso</span>
          </header>
          @if (a.utilization.length) {
            <app-echart [options]="utilOpts()" height="320px" />
          } @else {
            <p class="muted fin-empty">Nenhum recurso analisado.</p>
          }
        </article>
      </section>

      <article class="card panel span-3 anim-in">
        <header class="panel-head">
          <h3>Oportunidades de economia</h3>
          <span class="muted">
            {{ opportunities().length }} recursos · ordenar por
            <button type="button" class="link-btn" (click)="toggleSort()">
              {{ sortLabel() }}
            </button>
          </span>
        </header>
        @if (opportunities().length) {
          <div class="fin-table-wrap">
            <table class="fin-table">
              <thead>
                <tr>
                  <th>Recurso</th>
                  <th>Produto</th>
                  <th class="num">Utilização</th>
                  <th>Verdito</th>
                  <th class="num">Economia/mês</th>
                  <th>Recomendação</th>
                </tr>
              </thead>
              <tbody>
                @for (r of opportunities(); track r.resourceId) {
                  <tr>
                    <td>
                      <div class="fin-res">
                        <strong>{{ r.name || r.resourceId }}</strong>
                        <span class="muted fin-res-id">{{ r.resourceId }}</span>
                      </div>
                    </td>
                    <td><span class="badge">{{ r.product }}</span></td>
                    <td class="num">
                      <div class="fin-util">
                        <span
                          class="fin-util-bar"
                          [attr.data-verdict]="r.verdict"
                          [style.width.%]="clampPct(r.utilizationPct)"
                        ></span>
                        <span>{{ r.utilizationPct.toFixed(0) }}%</span>
                      </div>
                    </td>
                    <td>
                      <span class="badge" [class]="verdictClass(r.verdict)">
                        {{ verdictLabel(r.verdict) }}
                      </span>
                    </td>
                    <td class="num">
                      <span class="fin-savings">{{ money(r.monthlySavings, a.summary.currency) }}</span>
                    </td>
                    <td class="muted">{{ r.recommendation }}</td>
                  </tr>
                }
              </tbody>
            </table>
          </div>
        } @else {
          <p class="muted fin-empty">
            Nenhuma oportunidade de economia identificada neste período.
          </p>
        }
      </article>
    }

    <article class="card panel span-3 anim-in">
      <header class="panel-head">
        <h3>Escaneamento de findings</h3>
        <button type="button" class="link-btn" (click)="showScan.set(!showScan())">
          {{ showScan() ? 'Ocultar' : 'Mostrar' }}
        </button>
      </header>

      @if (showScan()) {
        <p class="muted">
          Analisa uma conta AWS cliente em busca de oportunidades de economia
          (somente leitura).
        </p>

        <div class="scan-form">
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
            <input type="number" min="1" [(ngModel)]="scanLookbackDays" />
          </div>

          <div class="field">
            <label>Região</label>
            <span class="badge">{{ region }}</span>
          </div>
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
      }
    </article>
  `,
  styles: [
    `
      .fin-error {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        border-left: 3px solid var(--danger);
        margin-bottom: 1.25rem;
      }
      .fin-error > div {
        flex: 1;
      }
      .fin-error strong {
        display: block;
      }
      .fin-error button,
      .link-btn {
        background: none;
        border: none;
        color: var(--accent);
        cursor: pointer;
        font: inherit;
        padding: 0;
      }
      .link-btn {
        font-weight: 700;
      }
      .fin-empty {
        margin: auto 0;
        padding: 2rem 0;
        text-align: center;
      }
      .kpi-val {
        font-size: 1.6rem;
        font-weight: 700;
      }
      .kpi.skeleton {
        min-height: 118px;
      }
      .sk-line {
        display: block;
        height: 1rem;
        width: 60%;
        border-radius: 4px;
        background: var(--border);
        opacity: 0.6;
      }
      .scan-form {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 0.75rem 1rem;
        margin: 0.5rem 0 0.75rem;
      }
      .fin-table-wrap {
        overflow-x: auto;
      }
      .fin-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.85rem;
      }
      .fin-table th,
      .fin-table td {
        padding: 0.55rem 0.7rem;
        text-align: left;
        border-bottom: 1px solid var(--border);
        vertical-align: middle;
      }
      .fin-table th {
        color: var(--muted);
        font-weight: 600;
        font-size: 0.78rem;
      }
      .fin-table td.num,
      .fin-table th.num {
        text-align: right;
      }
      .fin-table tbody tr:hover {
        background: color-mix(in srgb, var(--accent) 6%, transparent);
      }
      .fin-res {
        display: flex;
        flex-direction: column;
      }
      .fin-res-id {
        font-size: 0.72rem;
      }
      .fin-util {
        position: relative;
        display: inline-flex;
        align-items: center;
        justify-content: flex-end;
        gap: 0.5rem;
        min-width: 120px;
      }
      .fin-util-bar {
        position: absolute;
        left: 0;
        height: 6px;
        border-radius: 3px;
        opacity: 0.85;
      }
      .fin-util-bar[data-verdict='idle'] {
        background: var(--danger);
      }
      .fin-util-bar[data-verdict='oversized'] {
        background: var(--warn);
      }
      .fin-util-bar[data-verdict='ok'] {
        background: var(--ok);
      }
      .fin-util > span:last-child {
        position: relative;
      }
      .fin-savings {
        font-weight: 700;
        color: var(--ok);
      }
      .badge.warn {
        background: var(--soft-warn);
        color: var(--warn);
      }
    `,
  ],
})
export class FinOpsComponent {
  private readonly finops = inject(FinopsService);
  private readonly settings = inject(SettingsService);
  private readonly themeSvc = inject(ThemeService);

  readonly baseUrl = this.settings.baseUrl;
  readonly region = FINOPS_REGION;
  readonly periods = PERIODS;
  readonly scopes: FinopsScope[] = [
    'all',
    'rds',
    'ec2',
    'ebs',
    'eip',
    'elb',
    'snapshots',
  ];

  // ── Rightsizing analytics ────────────────────────────────────────────────
  readonly lookbackDays = signal(90);
  readonly analytics = signal<FinopsAnalytics | null>(null);
  readonly analyticsLoading = signal(false);
  readonly analyticsError = signal<string | null>(null);
  readonly sortDesc = signal(true);

  private readonly pal = computed(() => palette(this.themeSvc.theme()));

  readonly trendOpts = computed(() =>
    savingsTrendOptions(this.analytics()?.savingsTrend ?? [], this.pal()),
  );
  readonly byTypeOpts = computed(() =>
    savingsByTypeOptions(this.analytics()?.savingsByType ?? [], this.pal()),
  );
  readonly utilOpts = computed(() =>
    utilizationOptions(this.analytics()?.utilization ?? [], this.pal()),
  );
  readonly gaugeOpts = computed(() =>
    utilizationGaugeOptions(this.avgUtilization(), this.pal()),
  );

  private readonly avgUtilization = computed(() => {
    const rows = this.analytics()?.utilization ?? [];
    if (rows.length === 0) {
      return 0;
    }
    const total = rows.reduce((sum, r) => sum + r.utilizationPct, 0);
    return total / rows.length;
  });

  readonly opportunities = computed<FinopsUtilizationRow[]>(() => {
    const rows = [...(this.analytics()?.utilization ?? [])];
    const dir = this.sortDesc() ? -1 : 1;
    return rows.sort((a, b) => dir * (a.monthlySavings - b.monthlySavings));
  });

  readonly sortLabel = computed(() =>
    this.sortDesc() ? 'maior economia' : 'menor economia',
  );

  // ── Legacy findings scan (preserved) ─────────────────────────────────────
  readonly showScan = signal(false);
  readonly account = signal('');
  readonly roleArn = signal('');
  readonly scope = signal<FinopsScope>('all');
  readonly scanLookbackDays = signal(14);
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

  constructor() {
    void this.loadAnalytics();
  }

  setPeriod(days: number): void {
    if (this.lookbackDays() === days) {
      return;
    }
    this.lookbackDays.set(days);
    void this.loadAnalytics();
  }

  reload(): void {
    void this.loadAnalytics();
  }

  toggleSort(): void {
    this.sortDesc.set(!this.sortDesc());
  }

  private async loadAnalytics(): Promise<void> {
    this.analyticsLoading.set(true);
    this.analyticsError.set(null);
    try {
      const data = await this.finops.analytics({
        product: 'all',
        lookbackDays: this.lookbackDays(),
      });
      this.analytics.set(data);
    } catch (err) {
      this.analyticsError.set(
        err instanceof Error ? err.message : 'Erro inesperado ao carregar a análise.',
      );
    } finally {
      this.analyticsLoading.set(false);
    }
  }

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
        lookbackDays: this.scanLookbackDays(),
      });
      this.result.set(res);
    } finally {
      this.loading.set(false);
    }
  }

  money(value: number, currency: string): string {
    try {
      return new Intl.NumberFormat('pt-BR', {
        style: 'currency',
        currency: currency || 'USD',
        maximumFractionDigits: 0,
      }).format(value);
    } catch {
      return `${value} ${currency}`;
    }
  }

  clampPct(pct: number): number {
    return Math.max(0, Math.min(100, pct));
  }

  verdictClass(verdict: FinopsVerdict): string {
    if (verdict === 'idle') {
      return 'err';
    }
    if (verdict === 'oversized') {
      return 'warn';
    }
    return 'ok';
  }

  verdictLabel(verdict: FinopsVerdict): string {
    if (verdict === 'idle') {
      return 'ocioso';
    }
    if (verdict === 'oversized') {
      return 'superdimensionado';
    }
    return 'ok';
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
