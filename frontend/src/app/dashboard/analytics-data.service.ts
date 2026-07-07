import { Injectable } from '@angular/core';
import {
  ActivityItem,
  CostPoint,
  DashboardData,
  DayPoint,
  Environment,
  ENVIRONMENTS,
  HeatCell,
  Insight,
  Kpi,
  ServiceCount,
  SERVICES,
  StatusSlice,
} from './analytics.model';

/** Deterministic PRNG (mulberry32) so the demo data is stable across reloads. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const ACTORS = ['ana.souza', 'bruno.lima', 'carla.dias', 'diego.rocha', 'admin'];
const ACTION_LABELS: Record<string, string> = {
  create: 'Provisionar RDS',
  destroy: 'Remover instância',
  modify: 'Alterar classe',
  'start-stop': 'Parar/Iniciar',
  storage: 'Ajustar storage',
  replicate: 'Criar réplica',
  restore: 'Restaurar snapshot',
  'db-password': 'Rotacionar senha',
  'vpc-link': 'Publicar VPC link',
  kms: 'Rotacionar chave',
  finops: 'Análise de custo',
  servicenow: 'Abrir GMUD',
  'rds-data': 'Query RDS Data',
};

/**
 * Mock analytics source. Produces a realistic {@link DashboardData} snapshot for
 * a period (days). Isolated behind this service so it can be swapped for a real
 * API/aggregation layer without touching the dashboard component.
 */
@Injectable({ providedIn: 'root' })
export class AnalyticsDataService {
  snapshot(days: number): DashboardData {
    const rng = mulberry32(0x51ac + days);
    const series = this.dailySeries(days, rng);

    const total = sum(series.map((d) => d.success + d.failed));
    const success = sum(series.map((d) => d.success));
    const failed = sum(series.map((d) => d.failed));

    // Previous equal-length period for deltas.
    const prevSeries = this.dailySeries(days, mulberry32(0x51ac + days + 97));
    const prevTotal = sum(prevSeries.map((d) => d.success + d.failed));
    const prevSuccess = sum(prevSeries.map((d) => d.success));

    const successRate = total ? (success / total) * 100 : 0;
    const prevRate = prevTotal ? (prevSuccess / prevTotal) * 100 : 0;

    const byService = this.byService(total, rng);
    const costTrend = this.costTrend(rng);
    const monthCost = costTrend[costTrend.length - 1].cost;
    const prevMonthCost = costTrend[costTrend.length - 2].cost;
    const monthSavings = costTrend[costTrend.length - 1].savings;

    const activeResources = 180 + Math.floor(rng() * 90);
    const avgDuration = 3.4 + rng() * 2.2;
    const pendingApprovals = 4 + Math.floor(rng() * 8);

    const kpis: Kpi[] = [
      {
        key: 'actions',
        label: 'Ações executadas',
        value: total,
        decimals: 0,
        deltaPct: pct(total, prevTotal),
        higherIsBetter: true,
        spark: series.map((d) => d.success + d.failed),
        tone: 'accent',
        icon: 'bolt',
      },
      {
        key: 'success',
        label: 'Taxa de sucesso',
        value: successRate,
        decimals: 1,
        suffix: '%',
        deltaPct: successRate - prevRate,
        higherIsBetter: true,
        spark: series.map((d) =>
          d.success + d.failed ? (d.success / (d.success + d.failed)) * 100 : 0,
        ),
        tone: 'ok',
        icon: 'check',
      },
      {
        key: 'cost',
        label: 'Custo do mês',
        value: monthCost,
        decimals: 0,
        prefix: 'US$ ',
        deltaPct: pct(monthCost, prevMonthCost),
        higherIsBetter: false,
        spark: costTrend.map((c) => c.cost),
        tone: 'warn',
        icon: 'coin',
      },
      {
        key: 'savings',
        label: 'Economia FinOps',
        value: monthSavings,
        decimals: 0,
        prefix: 'US$ ',
        deltaPct: pct(monthSavings, costTrend[costTrend.length - 2].savings),
        higherIsBetter: true,
        spark: costTrend.map((c) => c.savings),
        tone: 'ok',
        icon: 'leaf',
      },
      {
        key: 'resources',
        label: 'Recursos ativos',
        value: activeResources,
        decimals: 0,
        deltaPct: pct(activeResources, activeResources - 12),
        higherIsBetter: true,
        spark: series.map((_, i) => activeResources - (series.length - i) * 0.8),
        tone: 'accent',
        icon: 'server',
      },
      {
        key: 'latency',
        label: 'Duração média',
        value: avgDuration,
        decimals: 1,
        suffix: 's',
        deltaPct: -6.2,
        higherIsBetter: false,
        spark: series.map(() => 3 + rng() * 2),
        tone: 'accent',
        icon: 'clock',
      },
    ];

    const statusBreakdown: StatusSlice[] = [
      { name: 'Sucesso', value: success, tone: 'ok' },
      { name: 'Falha', value: failed, tone: 'danger' },
      { name: 'Pendente', value: pendingApprovals, tone: 'warn' },
    ];

    return {
      kpis,
      actionsOverTime: series,
      byService,
      statusBreakdown,
      costTrend,
      heatmap: this.heatmap(rng),
      activity: this.activity(rng),
      insights: this.insights({
        successRate,
        prevRate,
        monthCost,
        prevMonthCost,
        monthSavings,
        byService,
      }),
    };
  }

  private dailySeries(days: number, rng: () => number): DayPoint[] {
    const out: DayPoint[] = [];
    const today = new Date();
    for (let i = days - 1; i >= 0; i--) {
      const d = new Date(today);
      d.setDate(today.getDate() - i);
      const dow = d.getDay();
      const weekend = dow === 0 || dow === 6 ? 0.45 : 1;
      const base = 22 + Math.sin(i / 5) * 6;
      const success = Math.max(3, Math.round((base + rng() * 10) * weekend));
      const failed = Math.max(0, Math.round((rng() * 4 + 1) * weekend));
      out.push({ date: fmtDay(d), success, failed });
    }
    return out;
  }

  private byService(total: number, rng: () => number): ServiceCount[] {
    const weights = SERVICES.map(() => 0.4 + rng());
    const wsum = sum(weights);
    return SERVICES.map((service, i) => ({
      service,
      count: Math.max(1, Math.round((weights[i] / wsum) * total)),
    })).sort((a, b) => b.count - a.count);
  }

  private costTrend(rng: () => number): CostPoint[] {
    const months = ['Ago', 'Set', 'Out', 'Nov', 'Dez', 'Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul'];
    let cost = 42000;
    return months.map((month) => {
      cost = Math.round(cost * (0.97 + rng() * 0.05));
      const savings = Math.round(cost * (0.12 + rng() * 0.08));
      return { month, cost, savings };
    });
  }

  private heatmap(rng: () => number): HeatCell[] {
    const cells: HeatCell[] = [];
    for (const service of SERVICES) {
      for (const env of ENVIRONMENTS) {
        const envWeight: Record<Environment, number> = {
          dev: 1,
          homol: 0.7,
          staging: 0.5,
          prod: 0.85,
        };
        cells.push({
          service,
          env,
          value: Math.round(rng() * 40 * envWeight[env]),
        });
      }
    }
    return cells;
  }

  private activity(rng: () => number): ActivityItem[] {
    const items: ActivityItem[] = [];
    const now = Date.now();
    for (let i = 0; i < 12; i++) {
      const service = SERVICES[Math.floor(rng() * SERVICES.length)];
      const env = ENVIRONMENTS[Math.floor(rng() * ENVIRONMENTS.length)];
      const roll = rng();
      const status = roll > 0.82 ? 'failed' : roll > 0.72 ? 'pending' : 'success';
      items.push({
        id: `op-${(1000 + i).toString(36)}`,
        service,
        action: ACTION_LABELS[service] ?? service,
        actor: ACTORS[Math.floor(rng() * ACTORS.length)],
        env,
        status,
        at: new Date(now - i * (1000 * 60 * (11 + Math.floor(rng() * 40)))).toISOString(),
        durationMs: Math.round(800 + rng() * 6000),
      });
    }
    return items;
  }

  private insights(ctx: {
    successRate: number;
    prevRate: number;
    monthCost: number;
    prevMonthCost: number;
    monthSavings: number;
    byService: ServiceCount[];
  }): Insight[] {
    const out: Insight[] = [];
    const top = ctx.byService[0];
    const costDelta = pct(ctx.monthCost, ctx.prevMonthCost);

    out.push({
      title: costDelta <= 0 ? 'Custo em queda' : 'Custo em alta',
      detail:
        `Gasto do mês ${costDelta <= 0 ? 'caiu' : 'subiu'} ` +
        `${Math.abs(costDelta).toFixed(1)}% vs. o anterior, com US$ ` +
        `${ctx.monthSavings.toLocaleString('pt-BR')} economizados via FinOps.`,
      tone: costDelta <= 0 ? 'ok' : 'warn',
    });
    out.push({
      title: `“${top.service}” lidera as ações`,
      detail: `${top.service} concentra ${top.count} execuções no período — maior volume entre os 13 microserviços.`,
      tone: 'accent',
    });
    out.push({
      title:
        ctx.successRate >= ctx.prevRate ? 'Confiabilidade melhorou' : 'Atenção à confiabilidade',
      detail:
        `Taxa de sucesso em ${ctx.successRate.toFixed(1)}% ` +
        `(${(ctx.successRate - ctx.prevRate >= 0 ? '+' : '') + (ctx.successRate - ctx.prevRate).toFixed(1)} p.p.). ` +
        'Falhas concentradas em ambientes de teste.',
      tone: ctx.successRate >= ctx.prevRate ? 'ok' : 'danger',
    });
    out.push({
      title: 'Janela de manutenção',
      detail: 'Pico de start-stop entre 19h e 22h sugere agendar automações fora do horário comercial.',
      tone: 'accent',
    });
    return out;
  }
}

function sum(xs: number[]): number {
  return xs.reduce((a, b) => a + b, 0);
}

function pct(now: number, prev: number): number {
  if (!prev) {
    return 0;
  }
  return ((now - prev) / prev) * 100;
}

function fmtDay(d: Date): string {
  return `${String(d.getDate()).padStart(2, '0')}/${String(d.getMonth() + 1).padStart(2, '0')}`;
}
