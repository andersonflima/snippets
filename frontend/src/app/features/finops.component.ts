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
import { ModalComponent } from '../shared/modal.component';
import { SkeletonComponent } from '../shared/skeleton.component';
import { ToastService } from '../shared/toast.service';
import { palette } from '../dashboard/chart-options';
import {
  savingsByTypeOptions,
  savingsTrendOptions,
  usageRadarOptions,
  utilizationGaugeOptions,
  utilizationOptions,
} from './finops-charts';
import { demoFinopsAnalytics } from './finops-demo';
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
  imports: [
    FormsModule,
    EChartComponent,
    IconComponent,
    ModalComponent,
    SkeletonComponent,
  ],
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
          <article class="kpi"><app-skeleton height="86px" /></article>
        }
      </section>
      <section class="chart-grid">
        <div class="card panel span-2"><app-skeleton height="300px" /></div>
        <div class="card panel"><app-skeleton height="300px" /></div>
      </section>
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
                  <tr
                    class="fin-row"
                    role="button"
                    tabindex="0"
                    [attr.aria-label]="'Ver detalhes: ' + (r.name || r.resourceId)"
                    (click)="openRow(r)"
                    (keydown.enter)="openRow(r)"
                    (keydown.space)="openRow(r); $event.preventDefault()"
                  >
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

      @if (selectedRow(); as r) {
        <app-modal
          [open]="true"
          [title]="r.name || r.resourceId"
          [subtitle]="r.resourceId"
          (close)="selectedRow.set(null)"
        >
          <div class="fin-detail">
            <div class="fin-detail-head">
              <span class="badge">{{ r.product }}</span>
              <span class="badge" [class]="verdictClass(r.verdict)">
                {{ verdictLabel(r.verdict) }}
              </span>
              <span class="badge">{{ region }}</span>
              <span class="badge">janela {{ lookbackDays() }}d</span>
            </div>

            <div class="kd-stats fin-highlights">
              <div class="kd-stat">
                <span class="kd-stat-val">{{ money(r.monthlySavings, a.summary.currency) }}</span>
                <span class="muted">Economia / mês</span>
              </div>
              <div class="kd-stat">
                <span class="kd-stat-val">{{ money(annualSavings(r), a.summary.currency) }}</span>
                <span class="muted">Projeção / ano</span>
              </div>
              <div class="kd-stat">
                <span class="kd-stat-val">{{ r.utilizationPct.toFixed(0) }}%</span>
                <span class="muted">Utilização</span>
              </div>
              <div class="kd-stat">
                <span class="kd-stat-val">{{ savingsShare(r) }}%</span>
                <span class="muted">do total da conta</span>
              </div>
            </div>

            <div class="fin-detail-charts">
              <div class="fin-chart-card">
                <div class="panel-head">
                  <h3>Utilização</h3>
                  <span class="muted">uso vs. provisionado</span>
                </div>
                <app-echart [options]="rowGaugeOpts()" height="230px" />
              </div>
              @if (usedMetrics(r).length >= 3) {
                <div class="fin-chart-card">
                  <div class="panel-head">
                    <h3>Perfil de uso</h3>
                    <span class="muted">por dimensão</span>
                  </div>
                  <app-echart [options]="rowRadarOpts()" height="230px" />
                </div>
              }
            </div>

            <div class="fin-detail-block">
              <div class="panel-head"><h3>Uso por dimensão</h3></div>
              <div class="fin-metrics">
                @for (u of usedMetrics(r); track u.key) {
                  <div class="fin-metric">
                    <div class="fin-metric-top">
                      <span>{{ u.key }}</span>
                      <span class="mono">{{ u.value.toFixed(0) }}%</span>
                    </div>
                    <div class="fin-metric-track">
                      <span
                        class="fin-metric-fill"
                        [attr.data-verdict]="r.verdict"
                        [style.width.%]="clampPct(u.value)"
                      ></span>
                    </div>
                  </div>
                }
              </div>
            </div>

            @if (provisionedMetrics(r).length) {
              <div class="fin-detail-block">
                <div class="panel-head"><h3>Capacidade provisionada</h3></div>
                <div class="kd-stats">
                  @for (pm of provisionedMetrics(r); track pm.key) {
                    <div class="kd-stat">
                      <span class="kd-stat-val mono">{{ pm.value.toLocaleString('pt-BR') }}</span>
                      <span class="muted">{{ pm.key }}</span>
                    </div>
                  }
                </div>
              </div>
            }

            <div class="fin-detail-rec" [attr.data-verdict]="r.verdict">
              <span class="muted">Recomendação</span>
              <p>{{ r.recommendation }}</p>
            </div>
          </div>
        </app-modal>
      }
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
      .fin-table tbody tr.fin-row {
        cursor: pointer;
      }
      .fin-table tbody tr.fin-row:focus-visible {
        outline: 2px solid var(--accent);
        outline-offset: -2px;
      }
      .fin-detail {
        display: flex;
        flex-direction: column;
        gap: 1rem;
      }
      .fin-detail-head {
        display: flex;
        gap: 0.5rem;
        flex-wrap: wrap;
      }
      .fin-detail-util {
        display: flex;
        align-items: center;
        gap: 0.75rem;
      }
      .fin-util-lg {
        min-width: 180px;
      }
      .fin-util-lg .fin-util-bar {
        height: 8px;
      }
      .fin-detail-rec p {
        margin: 0.35rem 0 0;
        line-height: 1.5;
      }
      .fin-highlights .kd-stat-val {
        color: var(--accent);
      }
      .fin-detail-charts {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 0.8rem;
        margin-top: 0.4rem;
      }
      .fin-chart-card {
        border: 1px solid var(--border);
        border-radius: var(--radius);
        background: var(--panel-2);
        padding: 0.7rem 0.8rem 0.3rem;
      }
      .fin-detail-block {
        margin-top: 0.2rem;
      }
      .fin-detail-block .panel-head {
        margin-bottom: 0.5rem;
      }
      .fin-metrics {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 0.7rem 1rem;
      }
      .fin-metric-top {
        display: flex;
        justify-content: space-between;
        font-size: 0.82rem;
        margin-bottom: 0.3rem;
      }
      .fin-metric-track {
        height: 8px;
        border-radius: 999px;
        background: var(--panel-3);
        overflow: hidden;
      }
      .fin-metric-fill {
        display: block;
        height: 100%;
        border-radius: 999px;
        background: var(--accent);
        transition: width 0.5s ease;
      }
      .fin-metric-fill[data-verdict='idle'] {
        background: var(--danger);
      }
      .fin-metric-fill[data-verdict='oversized'] {
        background: var(--warn);
      }
      .fin-metric-fill[data-verdict='ok'] {
        background: var(--ok);
      }
      .mono {
        font-family: var(--font-mono);
        font-variant-numeric: tabular-nums;
      }
      .fin-detail-rec {
        border-left: 3px solid var(--border-strong);
        padding: 0.1rem 0 0.1rem 0.8rem;
        margin-top: 0.3rem;
      }
      .fin-detail-rec[data-verdict='idle'] {
        border-left-color: var(--danger);
      }
      .fin-detail-rec[data-verdict='oversized'] {
        border-left-color: var(--warn);
      }
      .fin-detail-rec[data-verdict='ok'] {
        border-left-color: var(--ok);
      }
      .kd-stats {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
        gap: 0.6rem;
      }
      .kd-stat {
        display: flex;
        flex-direction: column;
        gap: 0.15rem;
        padding: 0.65rem 0.8rem;
        background: var(--panel-2);
        border: 1px solid var(--border);
        border-radius: 10px;
        font-size: 0.8rem;
      }
      .kd-stat-val {
        font-size: 1.15rem;
        font-weight: 700;
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
  private readonly toast = inject(ToastService);

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
  readonly selectedRow = signal<FinopsUtilizationRow | null>(null);

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

  /** Per-resource drill-down charts (recomputed when the selected row/theme change). */
  readonly rowGaugeOpts = computed(() => {
    const r = this.selectedRow();
    return r ? utilizationGaugeOptions(r.utilizationPct, this.pal(), 'utilização') : {};
  });
  readonly rowRadarOpts = computed(() => {
    const r = this.selectedRow();
    return r ? usageRadarOptions(r.used, this.pal()) : {};
  });
  readonly demoMode = signal(false);

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

  openRow(row: FinopsUtilizationRow): void {
    this.selectedRow.set(row);
  }

  /** Measured usage percentages present on the row, ready for display. */
  usedMetrics(row: FinopsUtilizationRow): FinopsDetailKeyValue[] {
    const labels: Record<keyof FinopsUtilizationRow['used'], string> = {
      cpuPct: 'CPU',
      memoryPct: 'Memória',
      storagePct: 'Armazenamento',
      iopsPct: 'IOPS',
    };
    return (Object.entries(row.used) as [keyof FinopsUtilizationRow['used'], number | undefined][])
      .filter(([, value]) => typeof value === 'number')
      .map(([key, value]) => ({ key: labels[key], value: value as number }));
  }

  /** Provisioned capacity numbers present on the row, ready for display. */
  provisionedMetrics(row: FinopsUtilizationRow): FinopsDetailKeyValue[] {
    return Object.entries(row.provisioned).map(([key, value]) => ({ key, value }));
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
      this.demoMode.set(false);
      this.toast.success(
        'Análise atualizada',
        `Economia estimada: ${this.money(data.summary.estimatedMonthlySavings, data.summary.currency)}/mês.`,
      );
    } catch (err) {
      // Demo mode (no AWS profile configured) falls back to deterministic sample
      // data so the page and its drill-down stay explorable without a live BFF.
      // With a real profile set, a failure keeps the honest error state.
      if (this.isDemoMode()) {
        const demo = demoFinopsAnalytics(this.lookbackDays());
        this.analytics.set(demo);
        this.demoMode.set(true);
        this.toast.info(
          'Modo demonstração',
          'Sem perfil AWS configurado — exibindo dados de exemplo.',
        );
      } else {
        const message =
          err instanceof Error ? err.message : 'Erro inesperado ao carregar a análise.';
        this.analyticsError.set(message);
        this.toast.error('Falha na análise', message);
      }
    } finally {
      this.analyticsLoading.set(false);
    }
  }

  /** Demo mode = no real AWS account configured for the active environment. */
  private isDemoMode(): boolean {
    return this.settings.activeProfile().account.trim() === '';
  }

  /** Projected annual savings for a resource (monthly × 12). */
  annualSavings(row: FinopsUtilizationRow): number {
    return row.monthlySavings * 12;
  }

  /** Share of the account's total monthly savings this resource represents. */
  savingsShare(row: FinopsUtilizationRow): number {
    const total = this.analytics()?.summary.estimatedMonthlySavings ?? 0;
    return total > 0 ? Math.round((row.monthlySavings / total) * 100) : 0;
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
      if (res.ok) {
        this.toast.success('Análise concluída', 'Findings de economia carregados.');
      } else {
        this.toast.error('Falha na análise', `${res.status} ${res.statusText}`);
      }
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
