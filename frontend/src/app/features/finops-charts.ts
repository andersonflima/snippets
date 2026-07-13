import type { EChartsOption } from 'echarts';
import type { Palette } from '../dashboard/chart-options';
import {
  FinopsSavingsByType,
  FinopsSavingsTrendPoint,
  FinopsUtilizationRow,
  FinopsVerdict,
} from './finops.service';

/**
 * ECharts option builders for the analytical FinOps page. Each builder is pure
 * (data + palette in, option object out) so the component can recompute them in
 * a `computed()` whenever the theme or the loaded analytics change.
 */

function rgba(hex: string, a: number): string {
  const m = hex.replace('#', '');
  const r = parseInt(m.substring(0, 2), 16);
  const g = parseInt(m.substring(2, 4), 16);
  const b = parseInt(m.substring(4, 6), 16);
  return `rgba(${r},${g},${b},${a})`;
}

function tooltipStyle(p: Palette) {
  return {
    backgroundColor: p.tooltipBg,
    borderColor: p.border,
    textStyle: { color: p.text, fontSize: 12 },
    extraCssText: 'border-radius:10px;box-shadow:var(--shadow-lg);',
  };
}

function fade(color: string, top = 0.22, bottom = 0.01): string {
  return {
    type: 'linear',
    x: 0,
    y: 0,
    x2: 0,
    y2: 1,
    colorStops: [
      { offset: 0, color: rgba(color, top) },
      { offset: 1, color: rgba(color, bottom) },
    ],
  } as unknown as string;
}

const usd = (v: unknown): string =>
  'US$ ' + Number(v).toLocaleString('pt-BR', { maximumFractionDigits: 0 });

function verdictColor(verdict: FinopsVerdict, p: Palette): string {
  if (verdict === 'idle') {
    return p.danger;
  }
  if (verdict === 'oversized') {
    return p.warn;
  }
  return p.ok;
}

/** Cost line + savings bars across the observed months. */
export function savingsTrendOptions(
  rows: FinopsSavingsTrendPoint[],
  p: Palette,
): EChartsOption {
  return {
    grid: { left: 8, right: 8, top: 30, bottom: 8, containLabel: true },
    legend: {
      data: ['Custo', 'Economia'],
      right: 0,
      top: 0,
      textStyle: { color: p.muted },
      icon: 'roundRect',
      itemWidth: 10,
      itemHeight: 10,
    },
    tooltip: { trigger: 'axis', valueFormatter: usd, ...tooltipStyle(p) },
    xAxis: {
      type: 'category',
      data: rows.map((r) => r.month),
      axisLine: { lineStyle: { color: p.border } },
      axisLabel: { color: p.muted, fontSize: 11 },
    },
    yAxis: {
      type: 'value',
      splitLine: { lineStyle: { color: p.grid } },
      axisLabel: {
        color: p.muted,
        fontSize: 11,
        formatter: (v: number) => (v >= 1000 ? v / 1000 + 'k' : String(v)),
      },
    },
    series: [
      {
        name: 'Economia',
        type: 'bar',
        data: rows.map((r) => r.savings),
        barWidth: '46%',
        itemStyle: { color: rgba(p.ok, 0.55), borderRadius: [4, 4, 0, 0] },
        emphasis: { itemStyle: { color: p.ok } },
      },
      {
        name: 'Custo',
        type: 'line',
        smooth: true,
        symbol: 'circle',
        symbolSize: 7,
        lineStyle: { width: 3, color: p.warn },
        itemStyle: { color: p.warn },
        areaStyle: { color: fade(p.warn, 0.16, 0.01) },
        data: rows.map((r) => r.cost),
      },
    ],
    animationDuration: 900,
  };
}

/** Potential savings grouped by resource type (horizontal bars, biggest on top). */
export function savingsByTypeOptions(
  rows: FinopsSavingsByType[],
  p: Palette,
): EChartsOption {
  const data = [...rows].sort((a, b) => a.savings - b.savings);
  return {
    grid: { left: 8, right: 24, top: 10, bottom: 8, containLabel: true },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      valueFormatter: usd,
      ...tooltipStyle(p),
    },
    xAxis: {
      type: 'value',
      splitLine: { lineStyle: { color: p.grid } },
      axisLabel: {
        color: p.muted,
        fontSize: 11,
        formatter: (v: number) => (v >= 1000 ? v / 1000 + 'k' : String(v)),
      },
    },
    yAxis: {
      type: 'category',
      data: data.map((d) => d.type),
      axisLine: { lineStyle: { color: p.border } },
      axisLabel: { color: p.muted, fontSize: 11 },
    },
    series: [
      {
        type: 'bar',
        data: data.map((d) => d.savings),
        barWidth: '58%',
        label: {
          show: true,
          position: 'right',
          color: p.muted,
          fontSize: 11,
          formatter: (o: { value?: unknown }) => usd(o.value),
        },
        itemStyle: {
          borderRadius: [0, 5, 5, 0],
          color: {
            type: 'linear',
            x: 0,
            y: 0,
            x2: 1,
            y2: 0,
            colorStops: [
              { offset: 0, color: rgba(p.ok, 0.5) },
              { offset: 1, color: p.accent },
            ],
          } as unknown as string,
        },
        emphasis: { itemStyle: { color: p.accent } },
      },
    ],
    animationDuration: 900,
    animationDelay: (i: number) => i * 40,
  };
}

/**
 * Usage vs. provisioned: one horizontal bar per resource showing utilization %,
 * colored by verdict (idle=red, oversized=warn, ok=green). Shows the worst
 * offenders (lowest utilization) first so opportunities stand out.
 */
export function utilizationOptions(
  rows: FinopsUtilizationRow[],
  p: Palette,
): EChartsOption {
  const data = [...rows]
    .sort((a, b) => b.utilizationPct - a.utilizationPct)
    .slice(-14);
  return {
    grid: { left: 8, right: 40, top: 10, bottom: 8, containLabel: true },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      valueFormatter: (v: unknown) => Number(v).toFixed(0) + '%',
      ...tooltipStyle(p),
    },
    xAxis: {
      type: 'value',
      max: 100,
      splitLine: { lineStyle: { color: p.grid } },
      axisLabel: { color: p.muted, fontSize: 11, formatter: '{value}%' },
    },
    yAxis: {
      type: 'category',
      data: data.map((d) => d.name || d.resourceId),
      axisLine: { lineStyle: { color: p.border } },
      axisLabel: { color: p.muted, fontSize: 11 },
    },
    series: [
      {
        type: 'bar',
        barWidth: '62%',
        label: {
          show: true,
          position: 'right',
          color: p.muted,
          fontSize: 11,
          formatter: (o: { value?: unknown }) => Number(o.value).toFixed(0) + '%',
        },
        data: data.map((d) => ({
          value: Math.round(d.utilizationPct),
          itemStyle: {
            borderRadius: [0, 5, 5, 0],
            color: verdictColor(d.verdict, p),
          },
        })),
      },
    ],
    animationDuration: 800,
    animationDelay: (i: number) => i * 40,
  };
}

/**
 * Radar of a single resource's usage profile across the dimensions present
 * (CPU / Memory / Storage / IOPS), each on a 0–100% axis. Best with ≥3 dims.
 */
export function usageRadarOptions(
  used: FinopsUtilizationRow['used'],
  p: Palette,
): EChartsOption {
  const dims: { key: keyof FinopsUtilizationRow['used']; name: string }[] = [
    { key: 'cpuPct', name: 'CPU' },
    { key: 'memoryPct', name: 'Memória' },
    { key: 'storagePct', name: 'Armazenamento' },
    { key: 'iopsPct', name: 'IOPS' },
  ];
  const present = dims.filter((d) => typeof used[d.key] === 'number');
  return {
    tooltip: { ...tooltipStyle(p) },
    radar: {
      indicator: present.map((d) => ({ name: d.name, max: 100 })),
      radius: '64%',
      center: ['50%', '54%'],
      splitNumber: 4,
      axisName: { color: p.muted, fontSize: 11 },
      splitLine: { lineStyle: { color: p.grid } },
      splitArea: { areaStyle: { color: ['transparent', 'transparent'] } },
      axisLine: { lineStyle: { color: p.border } },
    },
    series: [
      {
        type: 'radar',
        data: [
          {
            value: present.map((d) => Math.round(used[d.key] as number)),
            name: 'uso %',
            symbolSize: 5,
            areaStyle: { color: rgba(p.accent, 0.22) },
            lineStyle: { color: p.accent, width: 2 },
            itemStyle: { color: p.accent },
          },
        ],
      },
    ],
    animationDuration: 800,
  };
}

/** Gauge of the overall (average) utilization across analyzed resources. */
export function utilizationGaugeOptions(
  pct: number,
  p: Palette,
  title = 'utilização média',
): EChartsOption {
  const value = Math.max(0, Math.min(100, Math.round(pct)));
  return {
    series: [
      {
        type: 'gauge',
        startAngle: 210,
        endAngle: -30,
        min: 0,
        max: 100,
        radius: '92%',
        center: ['50%', '58%'],
        progress: {
          show: true,
          width: 14,
          roundCap: true,
          itemStyle: { color: p.accent },
        },
        pointer: { show: false },
        axisLine: {
          lineStyle: {
            width: 14,
            color: [
              [0.35, p.danger],
              [0.7, p.warn],
              [1, p.ok],
            ],
          },
        },
        axisTick: { show: false },
        splitLine: { show: false },
        axisLabel: { show: false },
        anchor: { show: false },
        title: { show: true, offsetCenter: [0, '32%'], color: p.muted, fontSize: 12 },
        detail: {
          valueAnimation: true,
          offsetCenter: [0, '-8%'],
          fontSize: 30,
          fontWeight: 700,
          color: p.text,
          formatter: '{value}%',
        },
        data: [{ value, name: title }],
      },
    ],
    animationDuration: 900,
  };
}
