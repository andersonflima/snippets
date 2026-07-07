import {
  ChangeDetectionStrategy,
  Component,
  computed,
  effect,
  inject,
  signal,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { ActivatedRoute, RouterLink } from '@angular/router';
import { toSignal } from '@angular/core/rxjs-interop';
import { map } from 'rxjs';
import type { EChartsOption } from 'echarts';
import { EChartComponent } from '../../shared/echart.component';
import { IconComponent } from '../../shared/icon.component';
import { ThemeService } from '../../core/theme.service';
import { Palette, palette } from '../../dashboard/chart-options';
import {
  LogEntry,
  MetricSeries,
  ProductScope,
  ResourceItem,
  ResourcesService,
} from './resources.service';

type Tab = 'overview' | 'metrics' | 'logs' | 'actions';

interface LookbackOption {
  minutes: number;
  label: string;
}

const LOOKBACKS: LookbackOption[] = [
  { minutes: 60, label: '1h' },
  { minutes: 360, label: '6h' },
  { minutes: 1440, label: '24h' },
  { minutes: 10080, label: '7d' },
];

const LOG_LEVELS = ['all', 'error', 'warn', 'info', 'debug'];

/** Line-chart options for a single metric series, dark/light aware. */
function metricOptions(series: MetricSeries, p: Palette): EChartsOption {
  return {
    grid: { left: 8, right: 16, top: 16, bottom: 8, containLabel: true },
    tooltip: {
      trigger: 'axis',
      backgroundColor: p.tooltipBg,
      borderColor: p.border,
      textStyle: { color: p.text, fontSize: 12 },
      valueFormatter: (v) => `${Number(v).toLocaleString('pt-BR')} ${series.unit}`,
    },
    xAxis: {
      type: 'time',
      axisLine: { lineStyle: { color: p.border } },
      axisLabel: { color: p.muted, fontSize: 11 },
    },
    yAxis: {
      type: 'value',
      scale: true,
      splitLine: { lineStyle: { color: p.grid } },
      axisLabel: { color: p.muted, fontSize: 11 },
    },
    series: [
      {
        name: series.metric,
        type: 'line',
        smooth: true,
        symbol: 'none',
        lineStyle: { width: 2, color: p.accent },
        areaStyle: {
          color: {
            type: 'linear',
            x: 0,
            y: 0,
            x2: 0,
            y2: 1,
            colorStops: [
              { offset: 0, color: rgba(p.accent, 0.28) },
              { offset: 1, color: rgba(p.accent, 0.02) },
            ],
          } as unknown as string,
        },
        data: series.points.map((pt) => [pt.t, pt.value]),
      },
    ],
    animationDuration: 700,
  };
}

function rgba(hex: string, a: number): string {
  const m = hex.replace('#', '');
  const r = parseInt(m.substring(0, 2), 16);
  const g = parseInt(m.substring(2, 4), 16);
  const b = parseInt(m.substring(4, 6), 16);
  return `rgba(${r},${g},${b},${a})`;
}

/** Per-resource detail: overview, metrics charts, logs and quick actions. */
@Component({
  selector: 'app-resource-detail',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [FormsModule, RouterLink, EChartComponent, IconComponent],
  template: `
    <div class="detail-head anim-in">
      <a routerLink="/resources" class="back">
        <app-icon name="layers" [size]="16" /> Recursos
      </a>

      @if (resource(); as r) {
        <div class="head-main">
          <div>
            <h1>{{ r.name }}</h1>
            <span class="path">{{ r.id }}</span>
          </div>
          <div class="head-badges">
            <span class="prod-badge">{{ r.product }}</span>
            <span class="badge">{{ r.type }}</span>
            <span class="badge">{{ r.env }}</span>
            <span class="badge">{{ r.region }}</span>
            <span class="badge" [attr.data-status]="statusTone(r.status)">{{ r.status }}</span>
            <span class="badge ok">{{ money(r.monthlyCost) }}/mês</span>
          </div>
        </div>
      } @else if (!loadingResource()) {
        <h1>{{ id() }}</h1>
      }
    </div>

    @if (resourceError(); as msg) {
      <div class="card">
        <h3>Falha ao carregar recurso <span class="badge err">erro</span></h3>
        <p class="muted" style="margin:0">{{ msg }}</p>
      </div>
    }

    <div class="tabs anim-in">
      @for (t of tabs; track t.key) {
        <button
          type="button"
          class="tab"
          [class.active]="tab() === t.key"
          (click)="tab.set(t.key)"
        >
          {{ t.label }}
        </button>
      }
    </div>

    <!-- Visão geral -->
    @if (tab() === 'overview') {
      @if (resource(); as r) {
        <div class="card">
          <h3>Metadados</h3>
          <div class="row"><span class="muted">Nome</span><span>{{ r.name }}</span></div>
          <div class="row"><span class="muted">Identificador</span><span class="path">{{ r.id }}</span></div>
          <div class="row"><span class="muted">Produto</span><span>{{ r.product }}</span></div>
          <div class="row"><span class="muted">Tipo</span><span>{{ r.type }}</span></div>
          <div class="row"><span class="muted">Tamanho</span><span>{{ r.size }}</span></div>
          <div class="row"><span class="muted">Ambiente</span><span>{{ r.env }}</span></div>
          <div class="row"><span class="muted">Região</span><span>{{ r.region }}</span></div>
          <div class="row"><span class="muted">Status</span><span class="badge" [attr.data-status]="statusTone(r.status)">{{ r.status }}</span></div>
          <div class="row"><span class="muted">Custo mensal</span><span class="badge ok">{{ money(r.monthlyCost) }}</span></div>
          <div class="row"><span class="muted">Utilização</span><span>{{ r.utilizationPct }}%</span></div>
          <div class="row"><span class="muted">Criado em</span><span>{{ date(r.createdAt) }}</span></div>
        </div>

        <div class="card">
          <h3>Tags</h3>
          @if (tagEntries(r).length === 0) {
            <p class="muted" style="margin:0">Sem tags.</p>
          } @else {
            @for (tag of tagEntries(r); track tag.key) {
              <div class="row">
                <span class="muted">{{ tag.key }}</span>
                <span>{{ tag.value }}</span>
              </div>
            }
          }
        </div>
      } @else {
        <div class="card muted">Carregando metadados…</div>
      }
    }

    <!-- Métricas -->
    @if (tab() === 'metrics') {
      <div class="card toolbar filters">
        <span class="muted">Janela</span>
        <div class="segmented">
          @for (l of lookbacks; track l.minutes) {
            <button type="button" [class.active]="lookback() === l.minutes" (click)="lookback.set(l.minutes)">
              {{ l.label }}
            </button>
          }
        </div>
      </div>

      @if (loadingMetrics()) {
        <div class="card muted">Carregando métricas…</div>
      } @else if (metricsError(); as msg) {
        <div class="card"><p class="muted" style="margin:0">{{ msg }}</p></div>
      } @else if (charts().length === 0) {
        <div class="card muted">Sem métricas para esta janela.</div>
      } @else {
        <section class="metric-grid">
          @for (c of charts(); track c.series.metric) {
            <article class="card panel">
              <header class="panel-head">
                <h3>{{ c.series.metric }}</h3>
                <span class="muted">{{ c.series.unit }}</span>
              </header>
              <div class="stats">
                <span>méd <b>{{ num(c.series.stats.avg) }}</b></span>
                <span>máx <b>{{ num(c.series.stats.max) }}</b></span>
                <span>p95 <b>{{ num(c.series.stats.p95) }}</b></span>
              </div>
              <app-echart [options]="c.options" height="240px" />
            </article>
          }
        </section>
      }
    }

    <!-- Logs -->
    @if (tab() === 'logs') {
      <div class="card toolbar filters">
        <div class="chips">
          @for (lv of logLevels; track lv) {
            <button type="button" class="chip" [class.active]="level() === lv" (click)="level.set(lv)">
              {{ lv === 'all' ? 'Todos' : lv }}
            </button>
          }
        </div>
        <div class="search-box">
          <app-icon name="search" [size]="16" class="muted" />
          <input type="search" placeholder="Filtrar mensagens…" [(ngModel)]="logSearch" aria-label="Filtrar logs" />
        </div>
      </div>

      @if (loadingLogs()) {
        <div class="card muted">Carregando logs…</div>
      } @else if (logsError(); as msg) {
        <div class="card"><p class="muted" style="margin:0">{{ msg }}</p></div>
      } @else if (filteredLogs().length === 0) {
        <div class="card muted">Nenhum log corresponde aos filtros.</div>
      } @else {
        <div class="card logs">
          @for (e of filteredLogs(); track $index) {
            <div class="log-row">
              <span class="log-ts">{{ time(e.ts) }}</span>
              <span class="log-level" [attr.data-level]="levelTone(e.level)">{{ e.level }}</span>
              <span class="log-msg">{{ e.message }}</span>
              <span class="log-src muted">{{ e.source }}</span>
            </div>
          }
        </div>
      }
    }

    <!-- Ações -->
    @if (tab() === 'actions') {
      <div class="card">
        <h3>Ações rápidas</h3>
        <p class="muted">
          Dispare operações action-driven para este recurso. As ações abrem o
          catálogo de integrações — a execução direcionada por recurso chega em breve.
        </p>
        <div class="action-grid">
          @for (a of quickActions(); track a.label) {
            <a class="action" routerLink="/integrations">
              <app-icon [name]="a.icon" [size]="18" />
              <div>
                <strong>{{ a.label }}</strong>
                <span class="muted">{{ a.hint }}</span>
              </div>
            </a>
          }
        </div>
      </div>
    }
  `,
  styles: [
    `
      .detail-head {
        margin-bottom: 1rem;
      }
      .back {
        display: inline-flex;
        align-items: center;
        gap: 0.35rem;
        font-size: 0.85rem;
        color: var(--muted);
        margin-bottom: 0.6rem;
      }
      .back:hover {
        color: var(--accent);
        text-decoration: none;
      }
      .head-main {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 1rem;
        flex-wrap: wrap;
      }
      .head-main h1 {
        font-size: 1.4rem;
        margin: 0 0 0.2rem;
      }
      .head-badges {
        display: flex;
        flex-wrap: wrap;
        gap: 0.4rem;
        align-items: center;
      }
      .prod-badge {
        font-family: ui-monospace, "SF Mono", Menlo, monospace;
        font-size: 0.68rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.03em;
        padding: 0.15rem 0.45rem;
        border-radius: 5px;
        background: var(--soft-accent);
        color: var(--accent);
      }

      .tabs {
        display: flex;
        gap: 0.15rem;
        border-bottom: 1px solid var(--border);
        margin-bottom: 1.1rem;
      }
      .tab {
        border: none;
        background: transparent;
        color: var(--muted);
        padding: 0.55rem 0.95rem;
        border-radius: 0;
        border-bottom: 2px solid transparent;
        margin-bottom: -1px;
      }
      .tab:hover:not(:disabled) {
        color: var(--text);
        border-color: var(--border);
      }
      .tab.active {
        color: var(--accent);
        border-bottom-color: var(--accent);
        font-weight: 600;
      }

      .filters {
        align-items: center;
        gap: 0.85rem;
      }
      .chips {
        display: flex;
        flex-wrap: wrap;
        gap: 0.35rem;
      }
      .chip {
        padding: 0.3rem 0.7rem;
        border-radius: 999px;
        font-size: 0.78rem;
        color: var(--muted);
      }
      .chip.active {
        background: var(--accent-2);
        border-color: var(--accent-2);
        color: #fff;
      }
      .search-box {
        display: flex;
        align-items: center;
        gap: 0.45rem;
        flex: 1 1 200px;
        padding: 0 0.6rem;
        background: var(--panel-2);
        border: 1px solid var(--border);
        border-radius: 6px;
      }
      .search-box input {
        border: none;
        background: transparent;
        padding-left: 0;
      }
      .search-box input:focus {
        box-shadow: none;
      }

      .metric-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
        gap: 1rem;
      }
      .stats {
        display: flex;
        gap: 1rem;
        font-size: 0.8rem;
        color: var(--muted);
        margin-bottom: 0.4rem;
      }
      .stats b {
        color: var(--text);
      }

      .logs {
        display: flex;
        flex-direction: column;
        padding: 0.3rem 0.6rem;
      }
      .log-row {
        display: grid;
        grid-template-columns: 92px 68px 1fr auto;
        gap: 0.6rem;
        align-items: baseline;
        padding: 0.45rem 0.2rem;
        border-bottom: 1px solid var(--border);
        font-size: 0.84rem;
      }
      .log-row:last-child {
        border-bottom: none;
      }
      .log-ts {
        font-family: ui-monospace, "SF Mono", Menlo, monospace;
        font-size: 0.75rem;
        color: var(--muted);
      }
      .log-level {
        font-size: 0.68rem;
        font-weight: 700;
        text-transform: uppercase;
        text-align: center;
        padding: 0.1rem 0.35rem;
        border-radius: 4px;
        background: var(--panel-2);
        color: var(--muted);
      }
      .log-level[data-level="ok"] {
        background: var(--soft-accent);
        color: var(--accent);
      }
      .log-level[data-level="warn"] {
        background: var(--soft-warn);
        color: var(--warn);
      }
      .log-level[data-level="danger"] {
        background: var(--soft-danger);
        color: var(--danger);
      }
      .log-msg {
        min-width: 0;
        word-break: break-word;
      }
      .log-src {
        font-family: ui-monospace, "SF Mono", Menlo, monospace;
        font-size: 0.74rem;
        white-space: nowrap;
      }

      .action-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
        gap: 0.75rem;
        margin-top: 0.5rem;
      }
      .action {
        display: flex;
        align-items: center;
        gap: 0.6rem;
        padding: 0.75rem 0.9rem;
        border: 1px solid var(--border);
        border-radius: var(--radius);
        background: var(--panel-2);
        color: inherit;
      }
      .action:hover {
        text-decoration: none;
        border-color: var(--accent);
        background: var(--hover);
      }
      .action strong {
        display: block;
        font-size: 0.88rem;
      }
      .action span {
        font-size: 0.76rem;
      }

      .badge[data-status="ok"] {
        background: var(--soft-ok);
        color: var(--ok);
      }
      .badge[data-status="warn"] {
        background: var(--soft-warn);
        color: var(--warn);
      }
      .badge[data-status="danger"] {
        background: var(--soft-danger);
        color: var(--danger);
      }
      .badge[data-status="muted"] {
        background: var(--panel-2);
        color: var(--muted);
      }

      @media (max-width: 640px) {
        .log-row {
          grid-template-columns: 1fr;
          gap: 0.15rem;
        }
        .metric-grid {
          grid-template-columns: 1fr;
        }
      }
    `,
  ],
})
export class ResourceDetailComponent {
  private readonly route = inject(ActivatedRoute);
  private readonly resources = inject(ResourcesService);
  private readonly themeSvc = inject(ThemeService);

  private readonly params = toSignal(
    this.route.paramMap.pipe(
      map((p) => ({
        product: (p.get('product') ?? 'all') as ProductScope,
        id: p.get('id') ?? '',
      })),
    ),
    { initialValue: { product: 'all' as ProductScope, id: '' } },
  );
  readonly product = computed(() => this.params().product);
  readonly id = computed(() => this.params().id);

  readonly tabs: { key: Tab; label: string }[] = [
    { key: 'overview', label: 'Visão geral' },
    { key: 'metrics', label: 'Métricas' },
    { key: 'logs', label: 'Logs' },
    { key: 'actions', label: 'Ações' },
  ];
  readonly tab = signal<Tab>('overview');
  readonly lookback = signal(360);
  readonly level = signal('all');
  readonly logSearch = signal('');

  readonly lookbacks = LOOKBACKS;
  readonly logLevels = LOG_LEVELS;

  readonly resource = signal<ResourceItem | null>(null);
  readonly loadingResource = signal(false);
  readonly resourceError = signal<string | null>(null);

  private readonly metricSeries = signal<MetricSeries[]>([]);
  readonly loadingMetrics = signal(false);
  readonly metricsError = signal<string | null>(null);

  private readonly logEntries = signal<LogEntry[]>([]);
  readonly loadingLogs = signal(false);
  readonly logsError = signal<string | null>(null);

  private readonly pal = computed(() => palette(this.themeSvc.theme()));

  readonly charts = computed(() => {
    const p = this.pal();
    return this.metricSeries().map((series) => ({
      series,
      options: metricOptions(series, p),
    }));
  });

  readonly filteredLogs = computed(() => {
    const level = this.level();
    const term = this.logSearch().trim().toLowerCase();
    return this.logEntries().filter((e) => {
      const levelMatch = level === 'all' || e.level.toLowerCase() === level;
      const textMatch =
        !term ||
        e.message.toLowerCase().includes(term) ||
        e.source.toLowerCase().includes(term);
      return levelMatch && textMatch;
    });
  });

  readonly quickActions = computed(() => this.actionsFor(this.product()));

  private resToken = 0;
  private metricToken = 0;
  private logToken = 0;

  constructor() {
    effect(() => {
      const product = this.product();
      const id = this.id();
      if (!id) {
        return;
      }
      void this.loadResource(product, id);
      void this.loadLogs(product, id);
    });

    effect(() => {
      const product = this.product();
      const id = this.id();
      const lookback = this.lookback();
      if (!id) {
        return;
      }
      void this.loadMetrics(product, id, lookback);
    });
  }

  private async loadResource(product: ProductScope, id: string): Promise<void> {
    const current = ++this.resToken;
    this.loadingResource.set(true);
    this.resourceError.set(null);
    try {
      const detail = await this.resources.list(product, { search: id });
      if (current !== this.resToken) {
        return;
      }
      const match = detail.items.find((i) => i.id === id) ?? detail.items[0] ?? null;
      this.resource.set(match);
    } catch (err) {
      if (current !== this.resToken) {
        return;
      }
      this.resource.set(null);
      this.resourceError.set(err instanceof Error ? err.message : String(err));
    } finally {
      if (current === this.resToken) {
        this.loadingResource.set(false);
      }
    }
  }

  private async loadMetrics(
    product: ProductScope,
    id: string,
    lookback: number,
  ): Promise<void> {
    const current = ++this.metricToken;
    this.loadingMetrics.set(true);
    this.metricsError.set(null);
    try {
      const detail = await this.resources.metrics(id, product, lookback);
      if (current !== this.metricToken) {
        return;
      }
      this.metricSeries.set(detail.series);
    } catch (err) {
      if (current !== this.metricToken) {
        return;
      }
      this.metricSeries.set([]);
      this.metricsError.set(err instanceof Error ? err.message : String(err));
    } finally {
      if (current === this.metricToken) {
        this.loadingMetrics.set(false);
      }
    }
  }

  private async loadLogs(product: ProductScope, id: string): Promise<void> {
    const current = ++this.logToken;
    this.loadingLogs.set(true);
    this.logsError.set(null);
    try {
      const detail = await this.resources.logs(id, product, { limit: 200 });
      if (current !== this.logToken) {
        return;
      }
      this.logEntries.set(detail.entries);
    } catch (err) {
      if (current !== this.logToken) {
        return;
      }
      this.logEntries.set([]);
      this.logsError.set(err instanceof Error ? err.message : String(err));
    } finally {
      if (current === this.logToken) {
        this.loadingLogs.set(false);
      }
    }
  }

  tagEntries(r: ResourceItem): { key: string; value: string }[] {
    return Object.entries(r.tags ?? {}).map(([key, value]) => ({ key, value }));
  }

  private actionsFor(product: ProductScope): { label: string; hint: string; icon: string }[] {
    const common = [
      { label: 'Modificar', hint: 'alterar configuração', icon: 'gear' },
      { label: 'Tags', hint: 'gerenciar etiquetas', icon: 'layers' },
    ];
    if (product === 'rds' || product === 'ec2') {
      return [
        { label: 'Start / Stop', hint: 'ligar ou desligar', icon: 'bolt' },
        { label: 'Armazenamento', hint: 'ajustar volume', icon: 'server' },
        ...common,
      ];
    }
    if (product === 'ebs' || product === 'snapshot') {
      return [
        { label: 'Snapshot', hint: 'criar backup', icon: 'shield' },
        ...common,
      ];
    }
    return common;
  }

  statusTone(status: string): 'ok' | 'warn' | 'danger' | 'muted' {
    const s = status.toLowerCase();
    if (['available', 'running', 'in-use', 'active', 'completed'].includes(s)) {
      return 'ok';
    }
    if (['pending', 'modifying', 'creating', 'rebooting'].includes(s)) {
      return 'warn';
    }
    if (['error', 'failed', 'terminated', 'deleting'].includes(s)) {
      return 'danger';
    }
    return 'muted';
  }

  levelTone(level: string): 'ok' | 'warn' | 'danger' | 'muted' {
    const l = level.toLowerCase();
    if (l === 'error' || l === 'fatal' || l === 'critical') {
      return 'danger';
    }
    if (l === 'warn' || l === 'warning') {
      return 'warn';
    }
    if (l === 'info') {
      return 'ok';
    }
    return 'muted';
  }

  money(value: number): string {
    return value.toLocaleString('pt-BR', {
      style: 'currency',
      currency: 'USD',
      maximumFractionDigits: 0,
    });
  }

  num(value: number): string {
    return value.toLocaleString('pt-BR', { maximumFractionDigits: 2 });
  }

  date(iso: string): string {
    const d = new Date(iso);
    return Number.isNaN(d.getTime()) ? iso : d.toLocaleString('pt-BR');
  }

  time(iso: string): string {
    const d = new Date(iso);
    return Number.isNaN(d.getTime()) ? iso : d.toLocaleTimeString('pt-BR');
  }
}
