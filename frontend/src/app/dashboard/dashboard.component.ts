import {
  ChangeDetectionStrategy,
  Component,
  afterNextRender,
  computed,
  inject,
  signal,
} from '@angular/core';
import { EChartComponent } from '../shared/echart.component';
import { CountUpComponent } from '../shared/count-up.component';
import { IconComponent } from '../shared/icon.component';
import { ModalComponent } from '../shared/modal.component';
import { SkeletonComponent } from '../shared/skeleton.component';
import { ThemeService } from '../core/theme.service';
import { ToastService } from '../shared/toast.service';
import { AnalyticsDataService } from './analytics-data.service';
import { ActionStatus, ActivityItem, Insight, Kpi, ServiceCount } from './analytics.model';
import type { EChartsOption } from 'echarts';
import { RouterLink } from '@angular/router';
import {
  areaOptions,
  byServiceOptions,
  costOptions,
  envVolumeOptions,
  heatmapOptions,
  kpiDetailBreakdownOptions,
  kpiDetailPrimaryOptions,
  palette,
  resourceMixOptions,
  sparkOptions,
  statusOptions,
} from './chart-options';

interface QuickAction {
  label: string;
  hint: string;
  icon: string;
  link: string;
}

const QUICK_ACTIONS: QuickAction[] = [
  { label: 'Recursos', hint: 'inventário & drill-down', icon: 'server', link: '/resources' },
  { label: 'Nova ação', hint: 'executar integração', icon: 'bolt', link: '/integrations' },
  { label: 'FinOps', hint: 'custo & rightsizing', icon: 'chart', link: '/finops' },
  { label: 'Performance BD', hint: 'métricas & queries', icon: 'database', link: '/db-performance' },
  { label: 'GMUD', hint: 'mudanças & aprovações', icon: 'shield', link: '/gmud' },
  { label: 'Configurações', hint: 'ambiente & perfil AWS', icon: 'gear', link: '/settings' },
];

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
  imports: [EChartComponent, CountUpComponent, IconComponent, ModalComponent, SkeletonComponent, RouterLink],
  template: `
    @if (busy()) {
      <div class="dash-progress" aria-hidden="true"></div>
    }

    <header class="cmd-head anim-in">
      <div class="cmd-id">
        <span class="cmd-eyebrow"><span class="cmd-blip"></span> CONSOLE · OPS</span>
        <h1>Visão geral</h1>
        <p class="cmd-sub">Microserviços action-driven — telemetria de demonstração</p>
      </div>
      <div class="cmd-controls">
        <span class="live" [class.on]="!loading()" title="Atualização contínua">
          <span class="live-dot"></span> Ao vivo
        </span>
        <div class="segmented">
          @for (p of periods; track p.d) {
            <button type="button" [class.active]="period() === p.d" (click)="setPeriod(p.d)">
              {{ p.label }}
            </button>
          }
        </div>
        <button
          type="button"
          class="ghost icon-btn refresh-btn"
          [class.spinning]="loading()"
          [disabled]="loading()"
          (click)="refresh()"
          title="Atualizar dados"
          aria-label="Atualizar dados"
        >
          <app-icon name="refresh" [size]="18" />
        </button>
      </div>
    </header>

    @if (loading()) {
      <section class="kpi-strip">
        @for (c of skeletonCells; track c) {
          <div class="kpi-cell"><app-skeleton height="104px" radius="14px" /></div>
        }
      </section>
      <section class="bento">
        <div class="cell area-hero"><app-skeleton height="360px" /></div>
        <div class="cell area-status"><app-skeleton height="360px" /></div>
        <div class="cell area-cost"><app-skeleton height="320px" /></div>
        <div class="cell area-svc"><app-skeleton height="320px" /></div>
        <div class="cell area-heat"><app-skeleton height="260px" /></div>
        <div class="cell area-top"><app-skeleton height="300px" /></div>
        <div class="cell area-env"><app-skeleton height="300px" /></div>
        <div class="cell area-restype"><app-skeleton height="300px" /></div>
        <div class="cell area-feed"><app-skeleton height="300px" /></div>
        <div class="cell area-quick"><app-skeleton height="300px" /></div>
      </section>
    } @else {
      <!-- KPI command strip ------------------------------------------------ -->
      <section class="kpi-strip">
        @for (k of data().kpis; track k.key; let i = $index) {
          <article
            class="kpi-cell anim-in"
            [style.animation-delay.ms]="i * 55"
            role="button"
            tabindex="0"
            [attr.aria-label]="'Ver detalhes: ' + k.label"
            (click)="openKpi(k)"
            (keydown.enter)="openKpi(k)"
            (keydown.space)="openKpi(k)"
          >
            <div class="kpi-top">
              <span class="kpi-ic" [attr.data-tone]="k.tone"><app-icon [name]="k.icon" /></span>
              <span class="kpi-delta" [attr.data-dir]="deltaDir(k)">
                {{ k.deltaPct >= 0 ? '▲' : '▼' }} {{ absPct(k) }}
              </span>
            </div>
            <div class="kv">
              <app-count-up
                [value]="k.value"
                [decimals]="k.decimals"
                [prefix]="k.prefix ?? ''"
                [suffix]="k.suffix ?? ''"
              />
            </div>
            <div class="kl">
              {{ k.label }}
              <app-icon name="expand" [size]="12" class="kpi-expand" />
            </div>
            <div class="kpi-spark">
              <app-echart [options]="sparkFor(k)" height="34px" />
            </div>
          </article>
        }
      </section>

      <!-- Bento command grid ----------------------------------------------- -->
      <section class="bento">
        <article class="cell area-hero glow anim-in">
          <header class="cell-head">
            <span class="tag">01 · série principal</span>
            <div class="cell-title">
              <h3>Ações ao longo do tempo</h3>
              <span class="muted">sucesso vs. falha</span>
            </div>
          </header>
          <app-echart [options]="areaOpts()" height="360px" />
        </article>

        <article class="cell area-status anim-in">
          <header class="cell-head">
            <span class="tag">02 · status</span>
            <div class="cell-title"><h3>Distribuição</h3></div>
          </header>
          <app-echart [options]="statusOpts()" height="360px" />
        </article>

        <article class="cell area-cost anim-in">
          <header class="cell-head">
            <span class="tag">03 · finops</span>
            <div class="cell-title">
              <h3>Custo &amp; economia</h3>
              <span class="muted">12 meses (US$)</span>
            </div>
          </header>
          <app-echart [options]="costOpts()" height="300px" />
        </article>

        <article class="cell area-svc anim-in">
          <header class="cell-head">
            <span class="tag">04 · serviços</span>
            <div class="cell-title"><h3>Ações por serviço</h3></div>
          </header>
          <app-echart [options]="byServiceOpts()" height="300px" />
        </article>

        <article class="cell area-heat glow anim-in">
          <header class="cell-head">
            <span class="tag">05 · matriz de intensidade</span>
            <div class="cell-title">
              <h3>Serviço × ambiente</h3>
              <span class="muted">volume de execuções</span>
            </div>
          </header>
          <app-echart [options]="heatmapOpts()" height="260px" />
        </article>

        <article class="cell area-top anim-in">
          <header class="cell-head">
            <span class="tag">06 · ranking</span>
            <div class="cell-title">
              <h3>Top serviços por volume</h3>
              <span class="muted">clique para inspecionar</span>
            </div>
          </header>
          <ul class="svc-list">
            @for (s of topServices(); track s.service; let i = $index) {
              <li
                class="svc-row anim-in"
                [style.animation-delay.ms]="i * 50"
                role="button"
                tabindex="0"
                [attr.aria-label]="s.service + ': ' + s.count + ' execuções'"
                (click)="onServiceClick(s)"
                (keydown.enter)="onServiceClick(s)"
              >
                <span class="svc-rank">{{ i + 1 }}</span>
                <span class="svc-name">{{ s.service }}</span>
                <span class="svc-track">
                  <span class="svc-bar" [style.width.%]="s.pct"></span>
                </span>
                <span class="svc-count">{{ s.count }}</span>
              </li>
            }
          </ul>
        </article>

        <article class="cell area-env anim-in">
          <header class="cell-head">
            <span class="tag">07 · ambientes</span>
            <div class="cell-title">
              <h3>Volume por ambiente</h3>
              <span class="muted">execuções por env</span>
            </div>
          </header>
          <app-echart [options]="envOpts()" height="240px" />
        </article>

        <article class="cell area-restype anim-in">
          <header class="cell-head">
            <span class="tag">08 · inventário</span>
            <div class="cell-title">
              <h3>Tipo de recurso</h3>
              <span class="muted">recursos ativos por tipo</span>
            </div>
          </header>
          <app-echart [options]="resTypeOpts()" height="240px" />
        </article>

        <article class="cell area-feed anim-in">
          <header class="cell-head">
            <span class="tag">09 · stream</span>
            <div class="cell-title">
              <h3>Atividade recente</h3>
              <app-icon name="activity" [size]="15" class="muted" />
            </div>
          </header>
          <ul class="activity">
            @for (a of data().activity; track a.id) {
              <li
                class="clickable"
                role="button"
                tabindex="0"
                [attr.aria-label]="'Ver detalhe: ' + a.action + ' em ' + a.service"
                (click)="openActivity(a)"
                (keydown.enter)="openActivity(a)"
                (keydown.space)="openActivity(a)"
              >
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
                  <app-icon name="expand" [size]="13" class="act-expand" />
                </div>
              </li>
            }
          </ul>
        </article>

        <article class="cell area-quick anim-in">
          <header class="cell-head">
            <span class="tag">10 · atalhos</span>
            <div class="cell-title">
              <h3>Ações rápidas</h3>
              <span class="muted">ir direto ao ponto</span>
            </div>
          </header>
          <div class="quick-grid">
            @for (q of quickActions; track q.link) {
              <a class="quick-tile" [routerLink]="q.link">
                <span class="quick-ic"><app-icon [name]="q.icon" [size]="18" /></span>
                <span class="quick-text">
                  <strong>{{ q.label }}</strong>
                  <span class="muted">{{ q.hint }}</span>
                </span>
                <app-icon name="expand" [size]="13" class="quick-arrow" />
              </a>
            }
          </div>
        </article>

        <section class="cell area-insights bare anim-in">
          <div class="insight-rail">
            @for (ins of data().insights; track ins.title; let i = $index) {
              <article
                class="insight-card clickable"
                [attr.data-tone]="ins.tone"
                [style.animation-delay.ms]="i * 60"
                role="button"
                tabindex="0"
                [attr.aria-label]="'Ver detalhe: ' + ins.title"
                (click)="openInsight(ins)"
                (keydown.enter)="openInsight(ins)"
                (keydown.space)="openInsight(ins)"
              >
                <span class="dot"></span>
                <div class="insight-body">
                  <strong>{{ ins.title }}</strong>
                  <p class="muted">{{ ins.detail }}</p>
                </div>
                <app-icon name="expand" [size]="13" class="insight-expand" />
              </article>
            }
          </div>
        </section>
      </section>
    }

    <app-modal
      [open]="!!kpiDetail()"
      [title]="kpiDetail()?.title ?? ''"
      [subtitle]="'Detalhamento da métrica no período selecionado'"
      (close)="closeKpi()"
    >
      @if (kpiDetail(); as d) {
        <div class="kpi-detail">
          <div class="kd-headline">
            <span class="kd-value">{{ d.valueLabel }}</span>
            <span class="kpi-delta" [attr.data-dir]="d.deltaPct >= 0 === d.higherIsBetter ? 'good' : 'bad'">
              {{ d.deltaPct >= 0 ? '▲' : '▼' }} {{ absVal(d.deltaPct) }}% vs. período anterior
            </span>
          </div>

          <div class="kd-stats">
            @for (s of d.stats; track s.label) {
              <div class="kd-stat">
                <span class="kd-stat-val">{{ s.value }}</span>
                <span class="muted">{{ s.label }}</span>
              </div>
            }
          </div>

          <div class="kd-chart">
            <div class="panel-head"><h3>{{ d.primaryTitle }}</h3><span class="muted">passe o mouse / arraste para dar zoom</span></div>
            <app-echart [options]="kpiPrimaryOpts()" height="340px" />
          </div>

          @if (d.secondary && d.secondary.length) {
            <div class="kd-chart">
              <div class="panel-head"><h3>{{ d.secondaryTitle }}</h3></div>
              <app-echart [options]="kpiBreakdownOpts()" height="300px" />
            </div>
          }

          <p class="kd-desc muted">{{ d.description }}</p>
        </div>
      }
    </app-modal>

    <app-modal
      [open]="!!selectedActivity()"
      [title]="selectedActivity()?.action ?? ''"
      [subtitle]="'Detalhe da atividade recente'"
      (close)="selectedActivity.set(null)"
    >
      @if (selectedActivity(); as a) {
        <div class="act-detail">
          <div class="act-detail-head">
            <span class="act-ic" [attr.data-status]="a.status">
              <app-icon [name]="statusIcon(a.status)" [size]="16" />
            </span>
            <span class="badge" [attr.data-status]="a.status">{{ statusLabel(a.status) }}</span>
          </div>

          <div class="kd-stats">
            <div class="kd-stat">
              <span class="kd-stat-val">{{ a.service }}</span>
              <span class="muted">Serviço</span>
            </div>
            <div class="kd-stat">
              <span class="kd-stat-val">{{ a.env }}</span>
              <span class="muted">Ambiente</span>
            </div>
            <div class="kd-stat">
              <span class="kd-stat-val">{{ a.actor }}</span>
              <span class="muted">Executor</span>
            </div>
            <div class="kd-stat">
              <span class="kd-stat-val">{{ formatDuration(a.durationMs) }}</span>
              <span class="muted">Duração</span>
            </div>
            <div class="kd-stat">
              <span class="kd-stat-val">{{ timeAgo(a.at) }}</span>
              <span class="muted">{{ absoluteDate(a.at) }}</span>
            </div>
          </div>

          <p class="path act-detail-id">{{ a.id }}</p>
        </div>
      }
    </app-modal>

    <app-modal
      [open]="!!selectedInsight()"
      [title]="selectedInsight()?.title ?? ''"
      [subtitle]="'Insight operacional'"
      (close)="selectedInsight.set(null)"
    >
      @if (selectedInsight(); as ins) {
        <p class="insight-detail">{{ ins.detail }}</p>
      }
    </app-modal>
  `,
  styles: [
    `
      :host {
        display: block;
      }

      /* Command header ---------------------------------------------------- */
      .cmd-head {
        display: flex;
        align-items: flex-end;
        justify-content: space-between;
        gap: 1rem;
        flex-wrap: wrap;
        padding-bottom: 1.1rem;
        margin-bottom: 1.4rem;
        border-bottom: 1px solid var(--border);
      }
      .cmd-eyebrow {
        display: inline-flex;
        align-items: center;
        gap: 0.45rem;
        font-family: var(--font-mono);
        font-size: 0.7rem;
        font-weight: 600;
        letter-spacing: 0.22em;
        text-transform: uppercase;
        color: var(--accent);
      }
      .cmd-blip {
        width: 7px;
        height: 7px;
        border-radius: 50%;
        background: var(--accent);
        box-shadow: 0 0 0 0 color-mix(in srgb, var(--accent) 60%, transparent);
        animation: cmd-blip 1.8s ease-out infinite;
      }
      @keyframes cmd-blip {
        0% { box-shadow: 0 0 0 0 color-mix(in srgb, var(--accent) 55%, transparent); }
        70% { box-shadow: 0 0 0 8px color-mix(in srgb, var(--accent) 0%, transparent); }
        100% { box-shadow: 0 0 0 0 color-mix(in srgb, var(--accent) 0%, transparent); }
      }
      .cmd-id h1 {
        margin: 0.5rem 0 0.2rem;
        font-size: clamp(1.9rem, 3.6vw, 2.7rem);
        font-weight: 800;
        letter-spacing: -0.035em;
        line-height: 1;
      }
      .cmd-sub {
        margin: 0;
        color: var(--muted);
        font-size: 0.9rem;
      }
      .cmd-controls {
        display: flex;
        align-items: center;
        gap: 0.6rem;
        flex-wrap: wrap;
      }
      .refresh-btn.spinning app-icon {
        animation: dash-spin 0.8s linear infinite;
      }
      @keyframes dash-spin {
        to { transform: rotate(360deg); }
      }

      /* KPI command strip ------------------------------------------------- */
      .kpi-strip {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 0.9rem;
        margin-bottom: 1.4rem;
      }
      .kpi-cell {
        position: relative;
        display: flex;
        flex-direction: column;
        gap: 0.4rem;
        padding: 1.1rem 1.2rem 0.9rem;
        border: 1px solid var(--border);
        border-radius: 16px;
        background:
          linear-gradient(180deg, color-mix(in srgb, var(--panel-2) 55%, var(--panel)), var(--panel));
        cursor: pointer;
        overflow: hidden;
        transition: transform 0.18s ease, border-color 0.18s ease, box-shadow 0.18s ease;
      }
      .kpi-cell::after {
        content: '';
        position: absolute;
        inset: 0 auto auto 0;
        width: 42%;
        height: 2px;
        background: linear-gradient(90deg, var(--accent), transparent);
        opacity: 0.7;
      }
      .kpi-cell:hover,
      .kpi-cell:focus-visible {
        transform: translateY(-4px);
        border-color: color-mix(in srgb, var(--accent) 45%, var(--border));
        box-shadow: 0 16px 34px color-mix(in srgb, var(--accent) 12%, transparent);
        outline: none;
      }
      .kpi-top {
        display: flex;
        align-items: center;
        justify-content: space-between;
      }
      .kv {
        font-family: var(--font-mono);
        font-size: 2rem;
        font-weight: 700;
        line-height: 1;
        letter-spacing: -0.03em;
        font-variant-numeric: tabular-nums;
      }
      .kl {
        font-size: 0.82rem;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.06em;
      }
      .kpi-expand {
        opacity: 0;
        color: var(--muted);
        transition: opacity 0.15s;
        vertical-align: middle;
        margin-left: 0.2rem;
      }
      .kpi-cell:hover .kpi-expand,
      .kpi-cell:focus-visible .kpi-expand {
        opacity: 1;
      }
      .kpi-spark {
        margin-top: 0.1rem;
      }

      /* Bento command grid ------------------------------------------------ */
      .bento {
        display: grid;
        grid-template-columns: repeat(12, 1fr);
        grid-auto-flow: dense;
        gap: 1rem;
      }
      .cell {
        position: relative;
        display: flex;
        flex-direction: column;
        padding: 1.2rem 1.3rem 1.35rem;
        border: 1px solid var(--border);
        border-radius: 18px;
        background:
          linear-gradient(180deg, color-mix(in srgb, var(--panel-2) 45%, var(--panel)), var(--panel));
        box-shadow: var(--shadow-sm);
        overflow: hidden;
        transition: border-color 0.18s ease, box-shadow 0.18s ease, transform 0.18s ease;
      }
      /* Corner tick marks — command-console framing */
      .cell::before {
        content: '';
        position: absolute;
        top: 10px;
        right: 10px;
        width: 10px;
        height: 10px;
        border-top: 2px solid color-mix(in srgb, var(--accent) 55%, transparent);
        border-right: 2px solid color-mix(in srgb, var(--accent) 55%, transparent);
        border-radius: 0 4px 0 0;
        opacity: 0.5;
        transition: opacity 0.2s ease;
      }
      .cell:hover {
        border-color: var(--border-strong);
        box-shadow: var(--shadow-md);
      }
      .cell:hover::before {
        opacity: 1;
      }
      .cell.glow {
        border-color: color-mix(in srgb, var(--accent) 32%, var(--border));
        box-shadow: 0 20px 46px color-mix(in srgb, var(--accent) 12%, transparent);
      }
      .cell.glow::after {
        content: '';
        position: absolute;
        inset: -40% 40% 60% -10%;
        background: radial-gradient(60% 60% at 20% 10%, color-mix(in srgb, var(--accent) 14%, transparent), transparent 70%);
        pointer-events: none;
      }
      .cell.bare {
        border: none;
        background: none;
        box-shadow: none;
        padding: 0;
        overflow: visible;
      }
      .cell.bare::before { display: none; }

      .cell-head {
        display: flex;
        flex-direction: column;
        gap: 0.35rem;
        margin-bottom: 0.9rem;
      }
      .tag {
        font-family: var(--font-mono);
        font-size: 0.66rem;
        font-weight: 600;
        letter-spacing: 0.16em;
        text-transform: uppercase;
        color: var(--faint);
      }
      .cell-title {
        display: flex;
        align-items: baseline;
        justify-content: space-between;
        gap: 0.75rem;
      }
      .cell-title h3 {
        margin: 0;
        font-size: 1.05rem;
        font-weight: 700;
        letter-spacing: -0.01em;
      }
      .cell-title .muted {
        font-size: 0.8rem;
      }

      /* Grid placement */
      .area-hero { grid-column: span 8; }
      .area-status { grid-column: span 4; }
      .area-cost { grid-column: span 7; }
      .area-svc { grid-column: span 5; }
      .area-heat { grid-column: span 12; }
      .area-top { grid-column: span 4; }
      .area-env { grid-column: span 4; }
      .area-restype { grid-column: span 4; }
      .area-feed { grid-column: span 8; }
      .area-quick { grid-column: span 4; }
      .area-insights { grid-column: span 12; }

      /* Quick actions */
      .quick-grid {
        display: grid;
        grid-template-columns: repeat(2, 1fr);
        gap: 0.7rem;
      }
      .quick-tile {
        display: flex;
        align-items: center;
        gap: 0.7rem;
        padding: 0.8rem 0.85rem;
        border: 1px solid var(--border);
        border-radius: var(--radius);
        background: var(--panel-2);
        color: var(--text);
        transition:
          transform 0.16s ease,
          border-color 0.16s ease,
          background-color 0.16s ease;
      }
      .quick-tile:hover {
        text-decoration: none;
        transform: translateY(-2px);
        border-color: color-mix(in srgb, var(--accent) 45%, var(--border));
        background: var(--hover);
      }
      .quick-ic {
        display: grid;
        place-items: center;
        width: 34px;
        height: 34px;
        flex: none;
        border-radius: 9px;
        color: var(--accent);
        background: var(--soft-accent);
      }
      .quick-text {
        display: flex;
        flex-direction: column;
        gap: 0.1rem;
        min-width: 0;
        line-height: 1.2;
      }
      .quick-text strong {
        font-size: 0.9rem;
      }
      .quick-text .muted {
        font-size: 0.74rem;
      }
      .quick-arrow {
        margin-left: auto;
        color: var(--faint);
        transition: transform 0.16s ease;
      }
      .quick-tile:hover .quick-arrow {
        color: var(--accent);
        transform: translate(2px, -2px);
      }
      @media (max-width: 560px) {
        .quick-grid {
          grid-template-columns: 1fr;
        }
      }

      .insight-rail {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
        gap: 1rem;
      }
      .insight-card {
        position: relative;
        display: flex;
        align-items: flex-start;
        gap: 0.7rem;
        padding: 1rem 1.15rem;
        border: 1px solid var(--border);
        border-left-width: 3px;
        border-left-color: var(--accent);
        border-radius: 14px;
        background: var(--panel);
        cursor: pointer;
        transition: border-color 0.15s, background 0.15s, transform 0.15s;
      }
      .insight-card[data-tone='ok'] { border-left-color: var(--ok); }
      .insight-card[data-tone='warn'] { border-left-color: var(--warn); }
      .insight-card[data-tone='danger'] { border-left-color: var(--danger); }
      .insight-card[data-tone='accent'] { border-left-color: var(--accent); }
      .insight-card:hover,
      .insight-card:focus-visible {
        transform: translateY(-3px);
        border-color: var(--border-strong);
        background: var(--panel-2);
        outline: none;
      }
      .insight-card .dot {
        flex: 0 0 auto;
        width: 9px;
        height: 9px;
        margin-top: 0.35rem;
        border-radius: 50%;
        background: var(--accent);
      }
      .insight-card[data-tone='ok'] .dot { background: var(--ok); }
      .insight-card[data-tone='warn'] .dot { background: var(--warn); }
      .insight-card[data-tone='danger'] .dot { background: var(--danger); }
      .insight-body strong {
        display: block;
        margin-bottom: 0.15rem;
      }
      .insight-body p {
        margin: 0;
        font-size: 0.84rem;
      }
      .insight-expand {
        position: absolute;
        top: 0.7rem;
        right: 0.7rem;
        opacity: 0;
        color: var(--muted);
        transition: opacity 0.15s;
      }
      .insight-card:hover .insight-expand,
      .insight-card:focus-visible .insight-expand {
        opacity: 1;
      }

      /* Activity feed inside its cell */
      .activity {
        list-style: none;
        margin: 0;
        padding: 0;
        display: flex;
        flex-direction: column;
      }
      .activity li.clickable {
        cursor: pointer;
        border-radius: 10px;
        padding: 0.55rem 0.5rem;
        transition: background 0.15s;
      }
      .activity li.clickable:hover,
      .activity li.clickable:focus-visible {
        background: var(--hover);
        outline: none;
      }
      .act-expand {
        opacity: 0;
        color: var(--muted);
        transition: opacity 0.15s;
      }
      .activity li.clickable:hover .act-expand,
      .activity li.clickable:focus-visible .act-expand {
        opacity: 1;
      }

      /* Modal detail blocks */
      .kpi-detail,
      .act-detail {
        display: flex;
        flex-direction: column;
        gap: 1.1rem;
      }
      .act-detail-head {
        display: flex;
        align-items: center;
        gap: 0.6rem;
      }
      .act-detail-id {
        margin: 0;
        word-break: break-all;
      }
      .kd-chart .panel-head {
        display: flex;
        align-items: baseline;
        justify-content: space-between;
        margin-bottom: 0.35rem;
      }
      .kd-chart h3 {
        margin: 0;
        font-size: 0.95rem;
      }
      .insight-detail {
        margin: 0;
        font-size: 0.92rem;
        line-height: 1.6;
      }

      /* Responsive collapse ---------------------------------------------- */
      @media (max-width: 1100px) {
        .area-hero,
        .area-status,
        .area-cost,
        .area-svc,
        .area-top,
        .area-env,
        .area-restype,
        .area-feed,
        .area-quick {
          grid-column: span 12;
        }
      }
      @media (min-width: 1101px) and (max-width: 1360px) {
        .area-top,
        .area-env,
        .area-restype {
          grid-column: span 6;
        }
      }
      @media (prefers-reduced-motion: reduce) {
        .kpi-cell:hover,
        .cell:hover,
        .insight-card:hover {
          transform: none;
        }
        .cmd-blip { animation: none; }
      }
    `,
  ],
})
export class DashboardComponent {
  private readonly analytics = inject(AnalyticsDataService);
  private readonly themeSvc = inject(ThemeService);
  private readonly toast = inject(ToastService);

  readonly periods = PERIODS;
  readonly period = signal(30);

  /** Initial skeleton gate + on-demand reloads (also re-runs count-up/echarts). */
  readonly loading = signal(true);
  /** Lightweight top progress bar for period switches. */
  readonly busy = signal(false);
  readonly skeletonCells = [0, 1, 2, 3, 4, 5, 6, 7];

  readonly data = computed(() => this.analytics.snapshot(this.period()));
  private readonly pal = computed(() => palette(this.themeSvc.theme()));

  readonly areaOpts = computed(() => areaOptions(this.data().actionsOverTime, this.pal()));
  readonly byServiceOpts = computed(() => byServiceOptions(this.data().byService, this.pal()));
  readonly statusOpts = computed(() => statusOptions(this.data().statusBreakdown, this.pal()));
  readonly costOpts = computed(() => costOptions(this.data().costTrend, this.pal()));
  readonly heatmapOpts = computed(() => heatmapOptions(this.data().heatmap, this.pal()));
  readonly envOpts = computed(() => envVolumeOptions(this.data().heatmap, this.pal()));
  readonly resTypeOpts = computed(() =>
    resourceMixOptions(this.analytics.resourceMix(this.period()), this.pal()),
  );

  readonly quickActions = QUICK_ACTIONS;

  /** Top services with a bar width relative to the busiest one. */
  readonly topServices = computed(() => {
    const rows = this.data().byService.slice(0, 6);
    const top = rows.length ? Math.max(...rows.map((r) => r.count)) : 1;
    return rows.map((r) => ({ ...r, pct: Math.max(6, Math.round((r.count / top) * 100)) }));
  });

  constructor() {
    afterNextRender(() => {
      setTimeout(() => {
        this.loading.set(false);
        this.toast.info('Dashboard pronto', 'Métricas de demonstração carregadas.');
      }, 650);
    });
  }

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

  setPeriod(d: number): void {
    if (d === this.period() || this.loading()) {
      return;
    }
    this.period.set(d);
    this.busy.set(true);
    setTimeout(() => {
      this.busy.set(false);
      const label = this.periods.find((p) => p.d === d)?.label ?? `${d} dias`;
      this.toast.info('Período atualizado', `Exibindo os últimos ${label}.`);
    }, 480);
  }

  refresh(): void {
    if (this.loading()) {
      return;
    }
    this.loading.set(true);
    setTimeout(() => {
      this.loading.set(false);
      this.toast.success('Dados atualizados', 'Painéis e métricas recalculados.');
    }, 620);
  }

  onServiceClick(s: ServiceCount): void {
    this.toast.info(`Serviço “${s.service}”`, `${s.count.toLocaleString('pt-BR')} execuções no período.`);
  }

  // --- Activity & insight detail modals ---
  readonly selectedActivity = signal<ActivityItem | null>(null);
  readonly selectedInsight = signal<Insight | null>(null);

  openActivity(a: ActivityItem): void {
    this.selectedActivity.set(a);
  }

  openInsight(ins: Insight): void {
    this.selectedInsight.set(ins);
  }

  absoluteDate(iso: string): string {
    return new Date(iso).toLocaleString('pt-BR');
  }

  formatDuration(ms: number): string {
    if (ms < 1000) {
      return `${ms}ms`;
    }
    const s = ms / 1000;
    if (s < 60) {
      return `${s.toLocaleString('pt-BR', { maximumFractionDigits: 1 })}s`;
    }
    const min = Math.floor(s / 60);
    const rem = Math.round(s % 60);
    return `${min}min ${rem}s`;
  }

  // --- KPI drill-down modal ---
  readonly selectedKey = signal<string | null>(null);
  readonly kpiDetail = computed(() => {
    const key = this.selectedKey();
    return key ? this.analytics.kpiDetail(key, this.period()) : null;
  });
  readonly kpiPrimaryOpts = computed<EChartsOption>(() => {
    const d = this.kpiDetail();
    return d ? kpiDetailPrimaryOptions(d, this.pal()) : {};
  });
  readonly kpiBreakdownOpts = computed<EChartsOption>(() => {
    const d = this.kpiDetail();
    return d ? kpiDetailBreakdownOptions(d, this.pal()) : {};
  });

  openKpi(k: Kpi): void {
    this.selectedKey.set(k.key);
  }

  closeKpi(): void {
    this.selectedKey.set(null);
  }

  absVal(n: number): string {
    return Math.abs(n).toFixed(1);
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
