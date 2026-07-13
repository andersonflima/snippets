import {
  FinopsAnalytics,
  FinopsUtilizationRow,
  FinopsVerdict,
} from './finops.service';

/**
 * Deterministic demo data for the analytical FinOps view, used as a local
 * fallback when no AWS profile is configured (demo mode) so the page — and its
 * per-resource drill-down — stays fully explorable without a live BFF. Mirrors
 * the shape the `insights/execute` action returns in production.
 */

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

const ENVS = ['prod', 'staging', 'homol', 'dev'];

interface ProductSpec {
  product: string;
  count: number;
  baseCost: [number, number];
  provisioned: (rng: () => number) => Record<string, number>;
  used: (rng: () => number) => FinopsUtilizationRow['used'];
}

const SPECS: ProductSpec[] = [
  {
    product: 'rds',
    count: 4,
    baseCost: [420, 1180],
    provisioned: (r) => ({
      vCPU: pick(r, [2, 4, 8, 16]),
      memGB: pick(r, [8, 16, 32, 64]),
      storageGB: pick(r, [100, 200, 500, 1000]),
      iops: pick(r, [3000, 6000, 12000]),
    }),
    used: (r) => ({
      cpuPct: rnd(r, 6, 78),
      memoryPct: rnd(r, 20, 82),
      storagePct: rnd(r, 15, 74),
      iopsPct: rnd(r, 8, 70),
    }),
  },
  {
    product: 'ec2',
    count: 4,
    baseCost: [180, 720],
    provisioned: (r) => ({ vCPU: pick(r, [2, 4, 8]), memGB: pick(r, [4, 8, 16, 32]) }),
    used: (r) => ({ cpuPct: rnd(r, 4, 76), memoryPct: rnd(r, 18, 80) }),
  },
  {
    product: 'ebs',
    count: 3,
    baseCost: [40, 190],
    provisioned: (r) => ({ storageGB: pick(r, [100, 250, 500, 1000]), iops: pick(r, [3000, 6000]) }),
    used: (r) => ({ storagePct: rnd(r, 10, 68), iopsPct: rnd(r, 5, 55) }),
  },
  {
    product: 'eip',
    count: 2,
    baseCost: [4, 8],
    provisioned: () => ({}),
    used: () => ({}),
  },
  {
    product: 'elb',
    count: 1,
    baseCost: [28, 96],
    provisioned: (r) => ({ listeners: pick(r, [1, 2, 3]) }),
    used: (r) => ({ cpuPct: rnd(r, 3, 40) }),
  },
];

const RECOMMENDATION: Record<FinopsVerdict, (p: string) => string> = {
  idle: (p) =>
    p === 'eip'
      ? 'Elastic IP sem associação há mais de 30 dias — libere para parar de pagar pela reserva.'
      : 'Recurso ocioso no período: consumo mínimo. Considere desligar, agendar start-stop ou remover.',
  oversized: () =>
    'Superdimensionado: use consistentemente abaixo de metade do provisionado. Reduza a classe/tamanho (rightsizing) para a próxima faixa.',
  ok: () => 'Dimensionamento adequado ao consumo observado — sem ação recomendada.',
};

function pick<T>(r: () => number, xs: T[]): T {
  return xs[Math.floor(r() * xs.length)];
}
function rnd(r: () => number, min: number, max: number): number {
  return Math.round(min + r() * (max - min));
}

function verdictFor(util: number): FinopsVerdict {
  if (util < 15) return 'idle';
  if (util < 45) return 'oversized';
  return 'ok';
}

function meanUsed(used: FinopsUtilizationRow['used']): number {
  const vals = Object.values(used).filter((v): v is number => typeof v === 'number');
  if (!vals.length) return 3;
  return vals.reduce((a, b) => a + b, 0) / vals.length;
}

export function demoFinopsAnalytics(days: number): FinopsAnalytics {
  const rng = mulberry32(0xf1a0 + days);
  const rows: FinopsUtilizationRow[] = [];

  for (const spec of SPECS) {
    for (let i = 0; i < spec.count; i++) {
      const provisioned = spec.provisioned(rng);
      const used = spec.used(rng);
      const utilizationPct = Math.round(meanUsed(used));
      const verdict = spec.product === 'eip' ? 'idle' : verdictFor(utilizationPct);
      const [lo, hi] = spec.baseCost;
      const baseCost = Math.round(lo + rng() * (hi - lo));
      const factor = verdict === 'idle' ? 0.7 + rng() * 0.25 : verdict === 'oversized' ? 0.25 + rng() * 0.25 : rng() < 0.25 ? 0.08 : 0;
      const monthlySavings = Math.round(baseCost * factor);
      const env = ENVS[i % ENVS.length];
      rows.push({
        resourceId: `${spec.product}-${env}-${String(i + 1).padStart(2, '0')}-${(0x1000 + i).toString(16)}`,
        name: `${spec.product.toUpperCase()} · ${env}-${String(i + 1).padStart(2, '0')}`,
        product: spec.product,
        provisioned,
        used,
        utilizationPct,
        verdict,
        monthlySavings,
        recommendation: RECOMMENDATION[verdict](spec.product),
      });
    }
  }

  const estimatedMonthlySavings = rows.reduce((a, r) => a + r.monthlySavings, 0);
  const idleCount = rows.filter((r) => r.verdict === 'idle').length;
  const oversizedCount = rows.filter((r) => r.verdict === 'oversized').length;

  const byType = new Map<string, number>();
  for (const r of rows) {
    byType.set(r.product, (byType.get(r.product) ?? 0) + r.monthlySavings);
  }
  const savingsByType = [...byType.entries()]
    .map(([type, savings]) => ({ type: type.toUpperCase(), savings }))
    .sort((a, b) => b.savings - a.savings);

  const months = ['Ago', 'Set', 'Out', 'Nov', 'Dez', 'Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul'];
  let cost = 44000;
  const savingsTrend = months.map((month) => {
    cost = Math.round(cost * (0.97 + rng() * 0.05));
    const savings = Math.round(estimatedMonthlySavings * (0.6 + rng() * 0.7));
    return { month, cost, savings };
  });

  return {
    summary: {
      estimatedMonthlySavings,
      currency: 'USD',
      idleCount,
      oversizedCount,
      analyzedCount: rows.length,
    },
    utilization: rows,
    savingsByType,
    savingsTrend,
  };
}
