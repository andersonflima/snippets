import { ChangeDetectionStrategy, Component, computed, inject, signal } from '@angular/core';
import { EChartComponent } from '../shared/echart.component';
import { CountUpComponent } from '../shared/count-up.component';
import { IconComponent } from '../shared/icon.component';
import { ThemeService } from '../core/theme.service';
import { AnalyticsDataService } from './analytics-data.service';
import { ActionStatus, Kpi } from './analytics.model';
import type { EChartsOption } from 'echarts';
import {
  areaOptions,
  byServiceOptions,
  costOptions,
  heatmapOptions,
  palette,
  sparkOptions,
  statusOptions,
} from './chart-options';

const PERIODS = [
  { d: 7, label: '7 dias' },
  { d: 30, label: '30 dias' },
  { d: 90, label: '90 dias' },
];

/** Analytics landing page: KPIs, robust ECharts panels, insights and activity. */
@Component({
  selector: 'app-dashboard',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [EChartComponent, CountUpComponent, IconComponent],
  template: `
    <div class="page-head anim-in">
      <div>
        <h1>Visão geral</h1>
        <p class="muted">Operações dos microserviços action-driven · dados de demonstração</p>
      </div>
      <div class="segmented">
        @for (p of periods; track p.d) {
          <button type="button" [class.active]="period() === p.d" (click)="period.set(p.d)">
            {{ p.label }}
          </button>
        }
      </div>
    </div>

    <section class="kpi-grid">
      @for (k of data().kpis; track k.key; let i = $index) {
        <article class="kpi anim-in" [style.animation-delay.ms]="i * 60">
          <div class="kpi-top">
            <span class="kpi-ic" [attr.data-tone]="k.tone"><app-icon [name]="k.icon" /></span>
            <span class="kpi-delta" [attr.data-dir]="deltaDir(k)">
              {{ k.deltaPct >= 0 ? '▲' : '▼' }} {{ absPct(k) }}
            </span>
          </div>
          <div class="kpi-val">
            <app-count-up
              [value]="k.value"
              [decimals]="k.decimals"
              [prefix]="k.prefix ?? ''"
              [suffix]="k.suffix ?? ''"
            />
          </div>
          <div class="kpi-label">{{ k.label }}</div>
          <div class="kpi-spark">
            <app-echart [options]="sparkFor(k)" height="38px" />
          </div>
        </article>
      }
    </section>

    <section class="chart-grid">
      <article class="card panel span-2 anim-in">
        <header class="panel-head">
          <h3>Ações ao longo do tempo</h3>
          <span class="muted">sucesso vs. falha</span>
        </header>
        <app-echart [options]="areaOpts()" height="300px" />
      </article>

      <article class="card panel anim-in">
        <header class="panel-head"><h3>Status</h3></header>
        <app-echart [options]="statusOpts()" height="300px" />
      </article>

      <article class="card panel anim-in">
        <header class="panel-head"><h3>Ações por serviço</h3></header>
        <app-echart [options]="byServiceOpts()" height="340px" />
      </article>

      <article class="card panel span-2 anim-in">
        <header class="panel-head">
          <h3>Custo & economia</h3>
          <span class="muted">12 meses (US$)</span>
        </header>
        <app-echart [options]="costOpts()" height="340px" />
      </article>

      <article class="card panel span-3 anim-in">
        <header class="panel-head">
          <h3>Intensidade por serviço × ambiente</h3>
          <span class="muted">volume de execuções</span>
        </header>
        <app-echart [options]="heatmapOpts()" height="260px" />
      </article>
    </section>

    <section class="bottom-grid">
      <div class="insight-grid">
        @for (ins of data().insights; track ins.title; let i = $index) {
          <article class="card insight anim-in" [attr.data-tone]="ins.tone" [style.animation-delay.ms]="i * 60">
            <span class="dot"></span>
            <div>
              <strong>{{ ins.title }}</strong>
              <p class="muted">{{ ins.detail }}</p>
            </div>
          </article>
        }
      </div>

      <article class="card panel anim-in">
        <header class="panel-head">
          <h3>Atividade recente</h3>
          <app-icon name="activity" [size]="16" class="muted" />
        </header>
        <ul class="activity">
          @for (a of data().activity; track a.id) {
            <li>
              <span class="act-ic" [attr.data-status]="a.status">
                <app-icon [name]="statusIcon(a.status)" [size]="14" />
              </span>
              <div class="act-main">
                <span class="act-title">{{ a.action }}</span>
                <span class="muted act-sub">{{ a.service }} · {{ a.env }} · {{ a.actor }}</span>
              </div>
              <div class="act-meta">
                <span class="badge" [attr.data-status]="a.status">{{ statusLabel(a.status) }}</span>
                <span class="muted act-time">{{ timeAgo(a.at) }}</span>
              </div>
            </li>
          }
        </ul>
      </article>
    </section>
  `,
})
export class DashboardComponent {
  private readonly analytics = inject(AnalyticsDataService);
  private readonly themeSvc = inject(ThemeService);

  readonly periods = PERIODS;
  readonly period = signal(30);

  readonly data = computed(() => this.analytics.snapshot(this.period()));
  private readonly pal = computed(() => palette(this.themeSvc.theme()));

  readonly areaOpts = computed(() => areaOptions(this.data().actionsOverTime, this.pal()));
  readonly byServiceOpts = computed(() => byServiceOptions(this.data().byService, this.pal()));
  readonly statusOpts = computed(() => statusOptions(this.data().statusBreakdown, this.pal()));
  readonly costOpts = computed(() => costOptions(this.data().costTrend, this.pal()));
  readonly heatmapOpts = computed(() => heatmapOptions(this.data().heatmap, this.pal()));

  // Memoized per KPI so the mini charts keep a stable options reference across
  // change-detection cycles (only rebuilt when data/theme actually change).
  private readonly sparkMap = computed(() => {
    const pal = this.pal();
    return new Map<string, EChartsOption>(
      this.data().kpis.map((k) => [
        k.key,
        sparkOptions(k.spark, { accent: pal.accent, ok: pal.ok, warn: pal.warn, danger: pal.danger }[k.tone]),
      ]),
    );
  });

  sparkFor(k: Kpi): EChartsOption {
    return this.sparkMap().get(k.key)!;
  }

  deltaDir(k: Kpi): 'good' | 'bad' {
    const positive = k.deltaPct >= 0;
    return positive === k.higherIsBetter ? 'good' : 'bad';
  }

  absPct(k: Kpi): string {
    return Math.abs(k.deltaPct).toFixed(1) + '%';
  }

  statusIcon(status: ActionStatus): string {
    return status === 'success' ? 'check' : status === 'failed' ? 'bolt' : 'clock';
  }

  statusLabel(status: ActionStatus): string {
    return status === 'success' ? 'sucesso' : status === 'failed' ? 'falha' : 'pendente';
  }

  timeAgo(iso: string): string {
    const diff = Date.now() - new Date(iso).getTime();
    const min = Math.round(diff / 60000);
    if (min < 60) {
      return `há ${min}min`;
    }
    const h = Math.floor(min / 60);
    return h < 24 ? `há ${h}h` : `há ${Math.floor(h / 24)}d`;
  }
}
