import {
  ChangeDetectionStrategy,
  Component,
  computed,
  inject,
  signal,
} from '@angular/core';
import type { EChartsOption } from 'echarts';
import { EChartComponent } from '../../shared/echart.component';
import { IconComponent } from '../../shared/icon.component';
import { ThemeService } from '../../core/theme.service';
import { palette, Palette } from '../../dashboard/chart-options';
import {
  DbInstance,
  DbMetadata,
  DbPerformanceService,
  DbRecommendation,
  DbTable,
} from './db-performance.service';

/** How many tables/indexes to show in the size-ranked charts. */
const TOP_N = 12;

type SortKey = 'size' | 'rows' | 'indexes';

/** Database performance analysis from RDS metadata (route: `/db-performance`). */
@Component({
  selector: 'app-db-performance',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [EChartComponent, IconComponent],
  template: `
    <div class="page-head anim-in">
      <div>
        <h1>Performance de BD</h1>
        <p class="muted">
          Análise de performance de banco de dados a partir de metadados (RDS ·
          somente leitura)
        </p>
      </div>
      <button
        type="button"
        class="ghost-btn"
        [disabled]="loadingInstances()"
        (click)="reloadInstances()"
      >
        <app-icon name="server" [size]="15" />
        {{ loadingInstances() ? 'Carregando…' : 'Atualizar instâncias' }}
      </button>
    </div>

    <section class="card panel anim-in">
      <header class="panel-head">
        <h3>Instância</h3>
        <span class="muted">selecione uma instância RDS para analisar</span>
      </header>

      @if (loadingInstances()) {
        <p class="muted">Carregando instâncias…</p>
      } @else if (instancesError(); as err) {
        <p class="note">{{ err }}</p>
      } @else if (instances().length === 0) {
        <p class="muted">Nenhuma instância RDS disponível.</p>
      } @else {
        <div class="chips">
          @for (inst of instances(); track inst.id) {
            <button
              type="button"
              class="chip"
              [class.active]="selectedId() === inst.id"
              (click)="select(inst.id)"
            >
              <span class="chip-dot" [attr.data-status]="statusTone(inst.status)"></span>
              <span class="chip-name">{{ inst.name }}</span>
              <span class="chip-meta muted">{{ inst.env }} · {{ inst.size }}</span>
            </button>
          }
        </div>
      }
    </section>

    @if (loadingMeta()) {
      <div class="card muted anim-in">Analisando metadados da instância…</div>
    }

    @if (metaError(); as err) {
      <div class="card anim-in"><p class="note">{{ err }}</p></div>
    }

    @if (!loadingMeta() && !metaError() && selectedId() && !meta()) {
      <div class="card muted anim-in">Sem metadados para esta instância.</div>
    }

    @if (meta(); as m) {
      <section class="kpi-grid">
        <article class="kpi anim-in">
          <div class="kpi-top">
            <span class="kpi-ic" data-tone="accent"><app-icon name="server" /></span>
          </div>
          <div class="kpi-val small">{{ m.engine }}</div>
          <div class="kpi-label">{{ m.engineVersion }}</div>
        </article>

        <article class="kpi anim-in" [style.animation-delay.ms]="60">
          <div class="kpi-top">
            <span class="kpi-ic" data-tone="accent"><app-icon name="layers" /></span>
          </div>
          <div class="kpi-val small">{{ m.instanceClass }}</div>
          <div class="kpi-label">Classe da instância</div>
        </article>

        <article class="kpi anim-in" [style.animation-delay.ms]="120">
          <div class="kpi-top">
            <span class="kpi-ic" [attr.data-tone]="storageTone()"><app-icon name="coin" /></span>
            <span class="kpi-delta" [attr.data-dir]="storagePct() >= 85 ? 'bad' : 'good'">
              {{ storagePct() }}%
            </span>
          </div>
          <div class="kpi-val">{{ m.storage.usedGb }} <small>/ {{ m.storage.allocatedGb }} GB</small></div>
          <div class="kpi-label">Storage usada / alocada</div>
          <div class="bar"><span class="bar-fill" [attr.data-tone]="storageTone()" [style.width.%]="storagePct()"></span></div>
        </article>

        <article class="kpi anim-in" [style.animation-delay.ms]="180">
          <div class="kpi-top">
            <span class="kpi-ic" [attr.data-tone]="connTone()"><app-icon name="activity" /></span>
            <span class="kpi-delta" [attr.data-dir]="connPct() >= 85 ? 'bad' : 'good'">
              {{ connPct() }}%
            </span>
          </div>
          <div class="kpi-val">{{ m.connections.current }} <small>/ {{ m.connections.max }}</small></div>
          <div class="kpi-label">Conexões atuais / máx</div>
          <div class="bar"><span class="bar-fill" [attr.data-tone]="connTone()" [style.width.%]="connPct()"></span></div>
        </article>

        <article class="kpi anim-in" [style.animation-delay.ms]="240">
          <div class="kpi-top">
            <span class="kpi-ic" data-tone="accent"><app-icon name="dashboard" /></span>
          </div>
          <div class="kpi-val">{{ m.tables.length }}</div>
          <div class="kpi-label">Tabelas</div>
        </article>

        <article class="kpi anim-in" [style.animation-delay.ms]="300">
          <div class="kpi-top">
            <span class="kpi-ic" [attr.data-tone]="m.unusedIndexes.length ? 'warn' : 'ok'"><app-icon name="bolt" /></span>
          </div>
          <div class="kpi-val">{{ m.unusedIndexes.length }}</div>
          <div class="kpi-label">Índices não usados</div>
        </article>
      </section>

      <section class="chart-grid">
        <article class="card panel span-2 anim-in">
          <header class="panel-head">
            <h3>Maiores tabelas</h3>
            <span class="muted">por tamanho (MB) · particionadas destacadas</span>
          </header>
          <app-echart [options]="topTablesOpts()" height="320px" />
        </article>

        <article class="card panel anim-in">
          <header class="panel-head">
            <h3>Storage usado vs alocado</h3>
            <span class="muted">{{ m.storage.type }}</span>
          </header>
          <app-echart [options]="storageOpts()" height="320px" />
        </article>

        <article class="card panel span-3 anim-in">
          <header class="panel-head">
            <h3>Uso de índices</h3>
            <span class="muted">scans por índice · não utilizados em vermelho</span>
          </header>
          <app-echart [options]="indexUsageOpts()" height="300px" />
        </article>
      </section>

      <section class="card panel anim-in">
        <header class="panel-head">
          <h3>Tabelas</h3>
          <div class="segmented small">
            <button type="button" [class.active]="sortKey() === 'size'" (click)="sortKey.set('size')">Tamanho</button>
            <button type="button" [class.active]="sortKey() === 'rows'" (click)="sortKey.set('rows')">Linhas</button>
            <button type="button" [class.active]="sortKey() === 'indexes'" (click)="sortKey.set('indexes')">Índices</button>
          </div>
        </header>
        <div class="table-wrap">
          <table class="grid">
            <thead>
              <tr>
                <th>Tabela</th>
                <th class="num">Linhas</th>
                <th class="num">Tamanho</th>
                <th>Particionada</th>
                <th class="num">Índices</th>
              </tr>
            </thead>
            <tbody>
              @for (t of sortedTables(); track t.schema + '.' + t.name) {
                <tr>
                  <td><span class="mono">{{ t.schema }}.{{ t.name }}</span></td>
                  <td class="num">{{ formatInt(t.rows) }}</td>
                  <td class="num">{{ formatSize(t.sizeMb) }}</td>
                  <td>
                    @if (t.partitioned) {
                      <span class="badge ok">sim · {{ t.partitions }}</span>
                    } @else {
                      <span class="badge">não</span>
                    }
                  </td>
                  <td class="num">{{ t.indexes.length }}</td>
                </tr>
              }
            </tbody>
          </table>
        </div>
      </section>

      <section class="card panel anim-in">
        <header class="panel-head">
          <h3>Índices não utilizados</h3>
          <span class="muted">candidatos a remoção</span>
        </header>
        @if (m.unusedIndexes.length === 0) {
          <p class="muted">Nenhum índice não utilizado detectado.</p>
        } @else {
          <div class="table-wrap">
            <table class="grid">
              <thead>
                <tr>
                  <th>Índice</th>
                  <th>Tabela</th>
                  <th class="num">Tamanho</th>
                </tr>
              </thead>
              <tbody>
                @for (ix of m.unusedIndexes; track ix.table + '.' + ix.index) {
                  <tr>
                    <td><span class="mono">{{ ix.index }}</span></td>
                    <td><span class="mono muted">{{ ix.table }}</span></td>
                    <td class="num"><span class="badge err">{{ formatSize(ix.sizeMb) }}</span></td>
                  </tr>
                }
              </tbody>
            </table>
          </div>
        }
      </section>

      <section class="card panel anim-in">
        <header class="panel-head">
          <h3>Queries lentas</h3>
          <span class="muted">tempo médio por execução</span>
        </header>
        @if (m.slowQueries.length === 0) {
          <p class="muted">Nenhuma query lenta registrada.</p>
        } @else {
          <div class="table-wrap">
            <table class="grid">
              <thead>
                <tr>
                  <th>Query</th>
                  <th class="num">Média (ms)</th>
                  <th class="num">Chamadas</th>
                  <th class="num">Linhas méd.</th>
                </tr>
              </thead>
              <tbody>
                @for (q of m.slowQueries; track q.query) {
                  <tr>
                    <td><code class="query" [title]="q.query">{{ truncate(q.query) }}</code></td>
                    <td class="num"><span class="badge" [class]="latencyClass(q.meanMs)">{{ formatMs(q.meanMs) }}</span></td>
                    <td class="num">{{ formatInt(q.calls) }}</td>
                    <td class="num">{{ formatInt(q.rowsAvg) }}</td>
                  </tr>
                }
              </tbody>
            </table>
          </div>
        }
      </section>

      <section class="anim-in">
        <header class="panel-head section-head">
          <h3>Recomendações</h3>
          <span class="muted">insights práticos de performance</span>
        </header>
        @if (m.recommendations.length === 0) {
          <div class="card muted">Nenhuma recomendação no momento.</div>
        } @else {
          <div class="rec-grid">
            @for (r of sortedRecs(); track r.title) {
              <article class="card rec" [attr.data-sev]="r.severity">
                <div class="rec-top">
                  <span class="dot"></span>
                  <strong>{{ r.title }}</strong>
                  <span class="badge" [class]="severityClass(r.severity)">{{ severityLabel(r.severity) }}</span>
                </div>
                <p class="muted">{{ r.detail }}</p>
              </article>
            }
          </div>
        }
      </section>
    }
  `,
  styles: [
    `
      :host {
        display: block;
      }
      .ghost-btn {
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
        background: var(--panel);
        border: 1px solid var(--border);
        color: var(--text);
        border-radius: var(--radius);
        padding: 0.45rem 0.75rem;
        font-size: 0.82rem;
        cursor: pointer;
        transition:
          border-color 0.18s ease,
          transform 0.18s ease;
      }
      .ghost-btn:hover:not(:disabled) {
        border-color: var(--accent);
        transform: translateY(-1px);
      }
      .ghost-btn:disabled {
        opacity: 0.6;
        cursor: default;
      }
      .chips {
        display: flex;
        flex-wrap: wrap;
        gap: 0.55rem;
      }
      .chip {
        display: inline-flex;
        align-items: center;
        gap: 0.5rem;
        background: var(--panel-2);
        border: 1px solid var(--border);
        border-radius: 999px;
        padding: 0.4rem 0.85rem;
        cursor: pointer;
        color: var(--text);
        transition:
          border-color 0.18s ease,
          background 0.18s ease,
          transform 0.18s ease;
      }
      .chip:hover {
        transform: translateY(-1px);
        border-color: var(--accent);
      }
      .chip.active {
        border-color: var(--accent);
        background: var(--soft-accent);
      }
      .chip-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: var(--muted);
      }
      .chip-dot[data-status='ok'] {
        background: var(--ok);
      }
      .chip-dot[data-status='warn'] {
        background: var(--warn);
      }
      .chip-dot[data-status='danger'] {
        background: var(--danger);
      }
      .chip-name {
        font-weight: 600;
        font-size: 0.85rem;
      }
      .chip-meta {
        font-size: 0.75rem;
      }
      .kpi-val.small {
        font-size: 1.15rem;
        word-break: break-word;
      }
      .kpi-val small {
        font-size: 0.75rem;
        color: var(--muted);
        font-weight: 500;
      }
      .bar {
        margin-top: 0.6rem;
        height: 6px;
        border-radius: 999px;
        background: var(--panel-2);
        overflow: hidden;
      }
      .bar-fill {
        display: block;
        height: 100%;
        border-radius: 999px;
        background: var(--accent);
        transition: width 0.4s ease;
      }
      .bar-fill[data-tone='ok'] {
        background: var(--ok);
      }
      .bar-fill[data-tone='warn'] {
        background: var(--warn);
      }
      .bar-fill[data-tone='danger'] {
        background: var(--danger);
      }
      .segmented.small button {
        padding: 0.25rem 0.6rem;
        font-size: 0.75rem;
      }
      .section-head {
        margin-bottom: 0.75rem;
      }
      .table-wrap {
        overflow-x: auto;
      }
      table.grid {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.83rem;
      }
      table.grid th {
        text-align: left;
        color: var(--muted);
        font-weight: 600;
        padding: 0.5rem 0.6rem;
        border-bottom: 1px solid var(--border);
        white-space: nowrap;
      }
      table.grid td {
        padding: 0.5rem 0.6rem;
        border-bottom: 1px solid var(--border);
      }
      table.grid tbody tr:last-child td {
        border-bottom: none;
      }
      table.grid tbody tr:hover {
        background: var(--panel-2);
      }
      .num {
        text-align: right;
        font-variant-numeric: tabular-nums;
      }
      .mono {
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        font-size: 0.8rem;
      }
      .query {
        font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
        font-size: 0.78rem;
        color: var(--text);
      }
      .rec-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
        gap: 1rem;
      }
      .rec {
        margin-bottom: 0;
        border-left: 3px solid var(--border);
      }
      .rec[data-sev='high'] {
        border-left-color: var(--danger);
      }
      .rec[data-sev='medium'] {
        border-left-color: var(--warn);
      }
      .rec[data-sev='low'] {
        border-left-color: var(--ok);
      }
      .rec-top {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        margin-bottom: 0.4rem;
      }
      .rec-top strong {
        flex: 1;
      }
      .rec .dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: var(--muted);
        flex: 0 0 auto;
      }
      .rec[data-sev='high'] .dot {
        background: var(--danger);
      }
      .rec[data-sev='medium'] .dot {
        background: var(--warn);
      }
      .rec[data-sev='low'] .dot {
        background: var(--ok);
      }
      .badge.warn {
        background: var(--soft-warn);
        color: var(--warn);
      }
    `,
  ],
})
export class DbPerformanceComponent {
  private readonly svc = inject(DbPerformanceService);
  private readonly themeSvc = inject(ThemeService);

  private readonly pal = computed<Palette>(() => palette(this.themeSvc.theme()));

  readonly instances = signal<DbInstance[]>([]);
  readonly loadingInstances = signal(false);
  readonly instancesError = signal<string | null>(null);

  readonly selectedId = signal<string | null>(null);
  readonly meta = signal<DbMetadata | null>(null);
  readonly loadingMeta = signal(false);
  readonly metaError = signal<string | null>(null);

  readonly sortKey = signal<SortKey>('size');

  constructor() {
    void this.reloadInstances();
  }

  async reloadInstances(): Promise<void> {
    this.loadingInstances.set(true);
    this.instancesError.set(null);
    try {
      const items = await this.svc.listInstances();
      this.instances.set(items);
      if (items.length > 0 && !this.selectedId()) {
        await this.select(items[0].id);
      }
    } catch (err) {
      this.instancesError.set(this.message(err));
    } finally {
      this.loadingInstances.set(false);
    }
  }

  async select(id: string): Promise<void> {
    this.selectedId.set(id);
    this.loadingMeta.set(true);
    this.metaError.set(null);
    this.meta.set(null);
    try {
      this.meta.set(await this.svc.metadata(id));
    } catch (err) {
      this.metaError.set(this.message(err));
    } finally {
      this.loadingMeta.set(false);
    }
  }

  // ---- Derived KPIs ---------------------------------------------------------

  readonly storagePct = computed(() => {
    const s = this.meta()?.storage;
    if (!s || s.allocatedGb <= 0) {
      return 0;
    }
    return Math.round((s.usedGb / s.allocatedGb) * 100);
  });

  readonly connPct = computed(() => {
    const c = this.meta()?.connections;
    if (!c || c.max <= 0) {
      return 0;
    }
    return Math.round((c.current / c.max) * 100);
  });

  readonly storageTone = computed(() => this.usageTone(this.storagePct()));
  readonly connTone = computed(() => this.usageTone(this.connPct()));

  readonly sortedTables = computed<DbTable[]>(() => {
    const tables = this.meta()?.tables ?? [];
    const key = this.sortKey();
    return [...tables].sort((a, b) => {
      if (key === 'rows') {
        return b.rows - a.rows;
      }
      if (key === 'indexes') {
        return b.indexes.length - a.indexes.length;
      }
      return b.sizeMb - a.sizeMb;
    });
  });

  readonly sortedRecs = computed<DbRecommendation[]>(() => {
    const order: Record<DbRecommendation['severity'], number> = {
      high: 0,
      medium: 1,
      low: 2,
    };
    return [...(this.meta()?.recommendations ?? [])].sort(
      (a, b) => order[a.severity] - order[b.severity],
    );
  });

  // ---- Chart options (recompute on theme + data) ---------------------------

  readonly topTablesOpts = computed<EChartsOption>(() =>
    topTablesChart(this.meta()?.tables ?? [], this.pal()),
  );

  readonly indexUsageOpts = computed<EChartsOption>(() =>
    indexUsageChart(this.meta()?.tables ?? [], this.pal()),
  );

  readonly storageOpts = computed<EChartsOption>(() =>
    storageGaugeChart(this.storagePct(), this.pal()),
  );

  // ---- Formatting + label helpers ------------------------------------------

  formatInt(value: number): string {
    return Math.round(value).toLocaleString('pt-BR');
  }

  formatSize(mb: number): string {
    if (mb >= 1024) {
      return (mb / 1024).toLocaleString('pt-BR', { maximumFractionDigits: 1 }) + ' GB';
    }
    return mb.toLocaleString('pt-BR', { maximumFractionDigits: 1 }) + ' MB';
  }

  formatMs(ms: number): string {
    return ms.toLocaleString('pt-BR', { maximumFractionDigits: 1 });
  }

  truncate(query: string, max = 80): string {
    const flat = query.replace(/\s+/g, ' ').trim();
    return flat.length > max ? flat.slice(0, max) + '…' : flat;
  }

  statusTone(status: string): 'ok' | 'warn' | 'danger' {
    const s = status.toLowerCase();
    if (s === 'available' || s === 'active' || s === 'running') {
      return 'ok';
    }
    if (s === 'stopped' || s === 'failed') {
      return 'danger';
    }
    return 'warn';
  }

  latencyClass(ms: number): string {
    if (ms >= 500) {
      return 'err';
    }
    if (ms >= 100) {
      return 'warn';
    }
    return 'ok';
  }

  severityClass(severity: DbRecommendation['severity']): string {
    if (severity === 'high') {
      return 'err';
    }
    if (severity === 'medium') {
      return 'warn';
    }
    return 'ok';
  }

  severityLabel(severity: DbRecommendation['severity']): string {
    return severity === 'high' ? 'alta' : severity === 'medium' ? 'média' : 'baixa';
  }

  private usageTone(pct: number): 'ok' | 'warn' | 'danger' {
    if (pct >= 85) {
      return 'danger';
    }
    if (pct >= 70) {
      return 'warn';
    }
    return 'ok';
  }

  private message(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }
}

// ---- Standalone chart builders (theme-aware) --------------------------------

function tooltip(p: Palette) {
  return {
    backgroundColor: p.tooltipBg,
    borderColor: p.border,
    textStyle: { color: p.text, fontSize: 12 },
    extraCssText: 'border-radius:8px;box-shadow:0 8px 24px rgba(0,0,0,0.25);',
  };
}

function topTablesChart(tables: DbTable[], p: Palette): EChartsOption {
  const top = [...tables]
    .sort((a, b) => b.sizeMb - a.sizeMb)
    .slice(0, TOP_N)
    .reverse();
  return {
    grid: { left: 8, right: 24, top: 30, bottom: 8, containLabel: true },
    legend: {
      data: ['Particionada', 'Não particionada'],
      right: 0,
      top: 0,
      textStyle: { color: p.muted },
      icon: 'roundRect',
      itemWidth: 10,
      itemHeight: 10,
    },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      valueFormatter: (v) => Number(v).toLocaleString('pt-BR') + ' MB',
      ...tooltip(p),
    },
    xAxis: {
      type: 'value',
      splitLine: { lineStyle: { color: p.grid } },
      axisLabel: { color: p.muted, fontSize: 11 },
    },
    yAxis: {
      type: 'category',
      data: top.map((t) => `${t.schema}.${t.name}`),
      axisLine: { lineStyle: { color: p.border } },
      axisLabel: { color: p.muted, fontSize: 11 },
    },
    series: [
      {
        name: 'Particionada',
        type: 'bar',
        stack: 'total',
        barWidth: '58%',
        itemStyle: { color: p.accent, borderRadius: [0, 5, 5, 0] },
        data: top.map((t) => (t.partitioned ? t.sizeMb : 0)),
      },
      {
        name: 'Não particionada',
        type: 'bar',
        stack: 'total',
        barWidth: '58%',
        itemStyle: { color: p.accent2, borderRadius: [0, 5, 5, 0] },
        data: top.map((t) => (t.partitioned ? 0 : t.sizeMb)),
      },
    ],
    animationDuration: 800,
    animationDelay: (i: number) => i * 30,
  };
}

function indexUsageChart(tables: DbTable[], p: Palette): EChartsOption {
  const indexes = tables.flatMap((t) =>
    t.indexes.map((ix) => ({
      label: `${t.name}.${ix.name}`,
      scans: ix.scans,
      unused: ix.unused || ix.scans === 0,
    })),
  );
  const top = [...indexes].sort((a, b) => b.scans - a.scans).slice(0, TOP_N);
  return {
    grid: { left: 8, right: 16, top: 10, bottom: 70, containLabel: true },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      valueFormatter: (v) => Number(v).toLocaleString('pt-BR') + ' scans',
      ...tooltip(p),
    },
    xAxis: {
      type: 'category',
      data: top.map((ix) => ix.label),
      axisLine: { lineStyle: { color: p.border } },
      axisLabel: { color: p.muted, fontSize: 10, rotate: 40, interval: 0 },
    },
    yAxis: {
      type: 'value',
      splitLine: { lineStyle: { color: p.grid } },
      axisLabel: { color: p.muted, fontSize: 11 },
    },
    series: [
      {
        type: 'bar',
        barWidth: '55%',
        data: top.map((ix) => ({
          value: ix.scans,
          itemStyle: {
            color: ix.unused ? p.danger : p.accent,
            borderRadius: [4, 4, 0, 0],
          },
        })),
      },
    ],
    animationDuration: 800,
    animationDelay: (i: number) => i * 30,
  };
}

function storageGaugeChart(pct: number, p: Palette): EChartsOption {
  const color = pct >= 85 ? p.danger : pct >= 70 ? p.warn : p.ok;
  return {
    series: [
      {
        type: 'gauge',
        startAngle: 210,
        endAngle: -30,
        min: 0,
        max: 100,
        radius: '92%',
        center: ['50%', '56%'],
        progress: { show: true, width: 16, itemStyle: { color } },
        axisLine: { lineStyle: { width: 16, color: [[1, p.grid]] } },
        axisTick: { show: false },
        splitLine: { length: 10, lineStyle: { color: p.border, width: 2 } },
        axisLabel: { color: p.muted, fontSize: 10, distance: 14 },
        pointer: { show: false },
        anchor: { show: false },
        title: { show: true, offsetCenter: [0, '32%'], color: p.muted, fontSize: 12 },
        detail: {
          valueAnimation: true,
          offsetCenter: [0, '2%'],
          formatter: '{value}%',
          color: p.text,
          fontSize: 30,
          fontWeight: 700,
        },
        data: [{ value: pct, name: 'Storage usada' }],
      },
    ],
    animationDuration: 800,
  };
}
