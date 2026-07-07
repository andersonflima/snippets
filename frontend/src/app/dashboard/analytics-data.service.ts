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
  KpiBreakdownItem,
  KpiDetail,
  KpiSeriesPoint,
  KpiStat,
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

  /**
   * Rich per-KPI detail for the drill-down modal. Built from the SAME
   * {@link snapshot} so it stays consistent with the cards. Deterministic:
   * reuses snapshot data (and its seeded sparks) plus a locally seeded PRNG for
   * the breakdown splits that aren't already in the mock.
   */
  kpiDetail(key: string, days: number): KpiDetail {
    const data = this.snapshot(days);
    const kpi = data.kpis.find((k) => k.key === key) ?? data.kpis[0];
    const series = data.actionsOverTime;

    const base = {
      key: kpi.key,
      title: kpi.label,
      valueLabel: fmtKpiValue(kpi),
      deltaPct: kpi.deltaPct,
      higherIsBetter: kpi.higherIsBetter,
      tone: kpi.tone,
    };

    switch (kpi.key) {
      case 'success':
        return { ...base, ...this.successDetail(kpi, series, data.statusBreakdown) };
      case 'cost':
        return { ...base, ...this.costDetail(data.costTrend, days) };
      case 'savings':
        return { ...base, ...this.savingsDetail(data.costTrend, days) };
      case 'resources':
        return { ...base, ...this.resourcesDetail(kpi, series, data.byService) };
      case 'latency':
        return { ...base, ...this.latencyDetail(kpi, series) };
      case 'actions':
      default:
        return { ...base, ...this.actionsDetail(series, data.byService) };
    }
  }

  private actionsDetail(
    series: DayPoint[],
    byService: ServiceCount[],
  ): Omit<KpiDetail, keyof KpiDetailBase> {
    const totals = series.map((d) => d.success + d.failed);
    const total = sum(totals);
    const success = sum(series.map((d) => d.success));
    const successRate = total ? (success / total) * 100 : 0;
    const primary: KpiSeriesPoint[] = series.map((d) => ({
      t: d.date,
      value: d.success + d.failed,
    }));
    const secondary: KpiBreakdownItem[] = byService
      .slice(0, 8)
      .map((s) => ({ label: s.service, value: s.count }));
    return {
      description:
        `No período foram executadas ${n0(total)} ações, com média de ` +
        `${n0(total / Math.max(1, series.length))} por dia e pico de ${n0(max(totals))}. ` +
        `A taxa de sucesso agregada ficou em ${pctStr(successRate)}, indicando volume ` +
        `saudável e tendência estável de operação.`,
      stats: [
        stat('Total', n0(total)),
        stat('Média/dia', n0(total / Math.max(1, series.length))),
        stat('Pico', n0(max(totals))),
        stat('Sucesso', pctStr(successRate)),
      ],
      primaryTitle: 'Ações por dia',
      primaryUnit: '',
      primary,
      secondaryTitle: 'Volume por serviço',
      secondary,
    };
  }

  private successDetail(
    kpi: Kpi,
    series: DayPoint[],
    statusBreakdown: StatusSlice[],
  ): Omit<KpiDetail, keyof KpiDetailBase> {
    const rates = series.map((d) =>
      d.success + d.failed ? (d.success / (d.success + d.failed)) * 100 : 0,
    );
    const primary: KpiSeriesPoint[] = series.map((d, i) => ({ t: d.date, value: rates[i] }));
    const secondary: KpiBreakdownItem[] = statusBreakdown.map((s) => ({
      label: s.name,
      value: s.value,
    }));
    const avg = rates.length ? sum(rates) / rates.length : 0;
    return {
      description:
        `A taxa de sucesso está em ${pctStr(kpi.value)}, com média diária de ${pctStr(avg)}. ` +
        `O melhor dia atingiu ${pctStr(max(rates))} e o pior ${pctStr(min(rates))}. ` +
        `Falhas se concentram em ambientes de teste, sem impacto relevante em produção.`,
      stats: [
        stat('Atual', pctStr(kpi.value)),
        stat('Média', pctStr(avg)),
        stat('Melhor dia', pctStr(max(rates))),
        stat('Pior dia', pctStr(min(rates))),
      ],
      primaryTitle: 'Taxa de sucesso por dia',
      primaryUnit: '%',
      primary,
      secondaryTitle: 'Distribuição por status',
      secondary,
    };
  }

  private costDetail(costTrend: CostPoint[], days: number): Omit<KpiDetail, keyof KpiDetailBase> {
    const costs = costTrend.map((c) => c.cost);
    const monthCost = costs[costs.length - 1];
    const prevCost = costs[costs.length - 2];
    const avg = costs.length ? sum(costs) / costs.length : 0;
    const primary: KpiSeriesPoint[] = costTrend.map((c) => ({ t: c.month, value: c.cost }));
    const secondary = this.resourceSplit(monthCost, 0x0057 + days);
    return {
      description:
        `O custo do mês corrente é ${usd(monthCost)}, ${pct(monthCost, prevCost) <= 0 ? 'abaixo' : 'acima'} ` +
        `do mês anterior (${signedPct(pct(monthCost, prevCost))}). A média dos últimos 12 meses é ` +
        `${usd(avg)}, com máximo de ${usd(max(costs))}. Maior parte concentrada em banco de dados e computação.`,
      stats: [
        stat('Mês atual', usd(monthCost)),
        stat('Média 12m', usd(avg)),
        stat('Máx', usd(max(costs))),
        stat('Δ vs anterior', signedPct(pct(monthCost, prevCost))),
      ],
      primaryTitle: 'Custo por mês',
      primaryUnit: 'US$',
      primary,
      secondaryTitle: 'Custo por tipo de recurso',
      secondary,
    };
  }

  private savingsDetail(
    costTrend: CostPoint[],
    days: number,
  ): Omit<KpiDetail, keyof KpiDetailBase> {
    const savings = costTrend.map((c) => c.savings);
    const monthSavings = savings[savings.length - 1];
    const accrued = sum(savings);
    const avg = savings.length ? accrued / savings.length : 0;
    const primary: KpiSeriesPoint[] = costTrend.map((c) => ({ t: c.month, value: c.savings }));
    const secondary = this.resourceSplit(monthSavings, 0x00a5 + days);
    return {
      description:
        `A economia FinOps do mês é ${usd(monthSavings)}, acumulando ${usd(accrued)} em 12 meses ` +
        `(média de ${usd(avg)}/mês). O melhor mês economizou ${usd(max(savings))}. ` +
        `Ganhos vêm de rightsizing, agendamento de start-stop e ajustes de storage.`,
      stats: [
        stat('Mês atual', usd(monthSavings)),
        stat('Acumulado 12m', usd(accrued)),
        stat('Média', usd(avg)),
        stat('Melhor mês', usd(max(savings))),
      ],
      primaryTitle: 'Economia por mês',
      primaryUnit: 'US$',
      primary,
      secondaryTitle: 'Economia por produto',
      secondary,
    };
  }

  private resourcesDetail(
    kpi: Kpi,
    series: DayPoint[],
    byService: ServiceCount[],
  ): Omit<KpiDetail, keyof KpiDetailBase> {
    // Reuse the deterministic resources-over-time spark from the KPI.
    const values = kpi.spark.map((v) => Math.round(v));
    const primary: KpiSeriesPoint[] = series.map((d, i) => ({
      t: d.date,
      value: values[i] ?? Math.round(kpi.value),
    }));
    const active = Math.round(kpi.value);
    const newLast7 =
      values.length > 7 ? Math.max(0, values[values.length - 1] - values[values.length - 8]) : 0;
    const avg = values.length ? sum(values) / values.length : active;
    const secondary: KpiBreakdownItem[] = byService
      .slice(0, 8)
      .map((s) => ({ label: s.service, value: s.count }));
    return {
      description:
        `São ${n0(active)} recursos ativos, com ${n0(newLast7)} novos nos últimos 7 dias e ` +
        `média de ${n0(avg)} no período. O pico foi de ${n0(max(values))}. ` +
        `A distribuição por serviço mostra concentração nos fluxos de provisionamento.`,
      stats: [
        stat('Ativos', n0(active)),
        stat('Novos (7d)', n0(newLast7)),
        stat('Média', n0(avg)),
        stat('Pico', n0(max(values))),
      ],
      primaryTitle: 'Recursos ativos por dia',
      primaryUnit: '',
      primary,
      secondaryTitle: 'Distribuição por serviço',
      secondary,
    };
  }

  private latencyDetail(kpi: Kpi, series: DayPoint[]): Omit<KpiDetail, keyof KpiDetailBase> {
    // Reuse the deterministic per-day duration spark from the KPI.
    const values = kpi.spark.map((v) => v);
    const primary: KpiSeriesPoint[] = series.map((d, i) => ({
      t: d.date,
      value: values[i] ?? kpi.value,
    }));
    const avg = values.length ? sum(values) / values.length : kpi.value;
    return {
      description:
        `A duração média atual é ${secStr(kpi.value)}, com média de ${secStr(avg)} no período. ` +
        `O P95 fica em ${secStr(percentile(values, 95))} e o máximo em ${secStr(max(values))}. ` +
        `Tempos estáveis indicam boa saúde das automações.`,
      stats: [
        stat('Atual', secStr(kpi.value)),
        stat('Média', secStr(avg)),
        stat('P95', secStr(percentile(values, 95))),
        stat('Máx', secStr(max(values))),
      ],
      primaryTitle: 'Duração média por dia',
      primaryUnit: 's',
      primary,
    };
  }

  /** Deterministic split of a total across resource-type buckets. */
  private resourceSplit(totalValue: number, seed: number): KpiBreakdownItem[] {
    const rng = mulberry32(seed);
    const types = ['RDS', 'EC2', 'S3', 'Lambda', 'EBS', 'KMS', 'VPC'];
    const weights = types.map(() => 0.4 + rng());
    const wsum = sum(weights);
    return types
      .map((label, i) => ({ label, value: Math.round((weights[i] / wsum) * totalValue) }))
      .sort((a, b) => b.value - a.value);
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

/** Fields of {@link KpiDetail} filled generically from the source {@link Kpi}. */
type KpiDetailBase = Pick<
  KpiDetail,
  'key' | 'title' | 'valueLabel' | 'deltaPct' | 'higherIsBetter' | 'tone'
>;

function max(xs: number[]): number {
  return xs.length ? Math.max(...xs) : 0;
}

function min(xs: number[]): number {
  return xs.length ? Math.min(...xs) : 0;
}

function percentile(xs: number[], p: number): number {
  if (!xs.length) {
    return 0;
  }
  const sorted = [...xs].sort((a, b) => a - b);
  const idx = Math.min(sorted.length - 1, Math.floor((p / 100) * (sorted.length - 1)));
  return sorted[idx];
}

function stat(label: string, value: string): KpiStat {
  return { label, value };
}

function n0(x: number): string {
  return Math.round(x).toLocaleString('pt-BR');
}

function n1(x: number): string {
  return x.toLocaleString('pt-BR', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
}

function pctStr(x: number): string {
  return `${n1(x)}%`;
}

function signedPct(x: number): string {
  return `${x >= 0 ? '+' : ''}${n1(x)}%`;
}

function usd(x: number): string {
  return `US$ ${n0(x)}`;
}

function secStr(x: number): string {
  return `${n1(x)}s`;
}

/** Formats a {@link Kpi} value the same way the cards render it. */
function fmtKpiValue(k: Kpi): string {
  const num = k.value.toLocaleString('pt-BR', {
    minimumFractionDigits: k.decimals,
    maximumFractionDigits: k.decimals,
  });
  return `${k.prefix ?? ''}${num}${k.suffix ?? ''}`;
}
