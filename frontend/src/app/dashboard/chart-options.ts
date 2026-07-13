import type { EChartsOption } from 'echarts';
import { ThemeMode } from '../core/theme.service';
import {
  CostPoint,
  DayPoint,
  ENVIRONMENTS,
  HeatCell,
  KpiDetail,
  ServiceCount,
  SERVICES,
  StatusSlice,
  Tone,
} from './analytics.model';

export interface Palette {
  text: string;
  muted: string;
  border: string;
  accent: string;
  accent2: string;
  ok: string;
  warn: string;
  danger: string;
  grid: string;
  tooltipBg: string;
}

export function palette(theme: ThemeMode): Palette {
  if (theme === 'light') {
    return {
      text: '#0f1a26',
      muted: '#566579',
      border: '#e1e8f1',
      accent: '#0d9488',
      accent2: '#0284c7',
      ok: '#16a34a',
      warn: '#d97706',
      danger: '#dc2626',
      grid: 'rgba(15,26,38,0.07)',
      tooltipBg: '#ffffff',
    };
  }
  return {
    text: '#e8eef7',
    muted: '#8b9cb3',
    border: '#1f2d3d',
    accent: '#2dd4bf',
    accent2: '#38bdf8',
    ok: '#4ade80',
    warn: '#fbbf24',
    danger: '#f87171',
    grid: 'rgba(255,255,255,0.06)',
    tooltipBg: '#0e1620',
  };
}

function tooltipStyle(p: Palette) {
  return {
    backgroundColor: p.tooltipBg,
    borderColor: p.border,
    textStyle: { color: p.text, fontSize: 12 },
    extraCssText: 'border-radius:10px;box-shadow:0 16px 40px rgba(1,4,9,0.5);',
  };
}

function fade(color: string, top = 0.35, bottom = 0.02): EChartsOption['color'] {
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

function rgba(hex: string, a: number): string {
  const m = hex.replace('#', '');
  const r = parseInt(m.substring(0, 2), 16);
  const g = parseInt(m.substring(2, 4), 16);
  const b = parseInt(m.substring(4, 6), 16);
  return `rgba(${r},${g},${b},${a})`;
}

export function areaOptions(series: DayPoint[], p: Palette): EChartsOption {
  return {
    grid: { left: 8, right: 16, top: 30, bottom: 8, containLabel: true },
    legend: {
      data: ['Sucesso', 'Falha'],
      right: 0,
      top: 0,
      textStyle: { color: p.muted },
      icon: 'roundRect',
      itemWidth: 10,
      itemHeight: 10,
    },
    tooltip: { trigger: 'axis', ...tooltipStyle(p) },
    xAxis: {
      type: 'category',
      data: series.map((d) => d.date),
      boundaryGap: false,
      axisLine: { lineStyle: { color: p.border } },
      axisLabel: { color: p.muted, fontSize: 11 },
    },
    yAxis: {
      type: 'value',
      splitLine: { lineStyle: { color: p.grid } },
      axisLabel: { color: p.muted, fontSize: 11 },
    },
    series: [
      {
        name: 'Sucesso',
        type: 'line',
        stack: 'total',
        smooth: true,
        symbol: 'none',
        color: p.accent,
        lineStyle: { width: 2, color: p.accent },
        itemStyle: { color: p.accent },
        areaStyle: { color: fade(p.accent) as unknown as string },
        emphasis: { focus: 'series' },
        data: series.map((d) => d.success),
      },
      {
        name: 'Falha',
        type: 'line',
        stack: 'total',
        smooth: true,
        symbol: 'none',
        color: p.danger,
        lineStyle: { width: 2, color: p.danger },
        itemStyle: { color: p.danger },
        areaStyle: { color: fade(p.danger) as unknown as string },
        emphasis: { focus: 'series' },
        data: series.map((d) => d.failed),
      },
    ],
    animationDuration: 900,
    animationEasing: 'cubicOut',
  };
}

export function byServiceOptions(rows: ServiceCount[], p: Palette): EChartsOption {
  const data = [...rows].reverse();
  return {
    grid: { left: 8, right: 24, top: 10, bottom: 8, containLabel: true },
    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, ...tooltipStyle(p) },
    xAxis: {
      type: 'value',
      splitLine: { lineStyle: { color: p.grid } },
      axisLabel: { color: p.muted, fontSize: 11 },
    },
    yAxis: {
      type: 'category',
      data: data.map((d) => d.service),
      axisLine: { lineStyle: { color: p.border } },
      axisLabel: { color: p.muted, fontSize: 11 },
    },
    series: [
      {
        type: 'bar',
        data: data.map((d) => d.count),
        barWidth: '58%',
        itemStyle: {
          borderRadius: [0, 5, 5, 0],
          color: {
            type: 'linear',
            x: 0,
            y: 0,
            x2: 1,
            y2: 0,
            colorStops: [
              { offset: 0, color: rgba(p.accent2, 0.55) },
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

export function statusOptions(slices: StatusSlice[], p: Palette): EChartsOption {
  const toneColor: Record<Tone, string> = {
    ok: p.ok,
    danger: p.danger,
    warn: p.warn,
    accent: p.accent,
  };
  return {
    tooltip: { trigger: 'item', ...tooltipStyle(p) },
    legend: {
      bottom: 0,
      textStyle: { color: p.muted },
      icon: 'circle',
      itemWidth: 9,
      itemHeight: 9,
    },
    series: [
      {
        type: 'pie',
        radius: ['58%', '82%'],
        center: ['50%', '44%'],
        avoidLabelOverlap: true,
        padAngle: 2,
        itemStyle: { borderRadius: 6 },
        label: { show: false },
        emphasis: {
          scale: true,
          scaleSize: 6,
          label: { show: true, fontSize: 15, fontWeight: 700, color: p.text, formatter: '{c}' },
        },
        data: slices.map((s) => ({
          name: s.name,
          value: s.value,
          itemStyle: { color: toneColor[s.tone] },
        })),
      },
    ],
    animationDuration: 900,
  };
}

export function costOptions(rows: CostPoint[], p: Palette): EChartsOption {
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
    tooltip: {
      trigger: 'axis',
      valueFormatter: (v) => 'US$ ' + Number(v).toLocaleString('pt-BR'),
      ...tooltipStyle(p),
    },
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
      },
      {
        name: 'Custo',
        type: 'line',
        smooth: true,
        symbol: 'circle',
        symbolSize: 7,
        lineStyle: { width: 3, color: p.warn },
        itemStyle: { color: p.warn },
        areaStyle: { color: fade(p.warn, 0.18, 0.01) as unknown as string },
        data: rows.map((r) => r.cost),
      },
    ],
    animationDuration: 900,
  };
}

export function heatmapOptions(cells: HeatCell[], p: Palette): EChartsOption {
  const data = cells.map((c) => [
    SERVICES.indexOf(c.service as (typeof SERVICES)[number]),
    ENVIRONMENTS.indexOf(c.env),
    c.value,
  ]);
  const max = Math.max(...cells.map((c) => c.value), 1);
  return {
    grid: { left: 8, right: 8, top: 8, bottom: 60, containLabel: true },
    tooltip: {
      position: 'top',
      formatter: (params: unknown) => {
        const value = (params as { data: [number, number, number] }).data;
        return `${SERVICES[value[0]]} · ${ENVIRONMENTS[value[1]]}: <b>${value[2]}</b>`;
      },
      ...tooltipStyle(p),
    },
    xAxis: {
      type: 'category',
      data: SERVICES as unknown as string[],
      splitArea: { show: true },
      axisLine: { lineStyle: { color: p.border } },
      axisLabel: { color: p.muted, fontSize: 10, rotate: 45 },
    },
    yAxis: {
      type: 'category',
      data: ENVIRONMENTS,
      splitArea: { show: true },
      axisLine: { lineStyle: { color: p.border } },
      axisLabel: { color: p.muted, fontSize: 11 },
    },
    visualMap: {
      min: 0,
      max,
      calculable: true,
      orient: 'horizontal',
      left: 'center',
      bottom: 0,
      textStyle: { color: p.muted },
      inRange: { color: [rgba(p.accent, 0.08), p.accent2, p.accent] },
    },
    series: [
      {
        type: 'heatmap',
        data,
        itemStyle: { borderColor: p.tooltipBg, borderWidth: 1, borderRadius: 3 },
        emphasis: { itemStyle: { shadowBlur: 8, shadowColor: rgba(p.accent, 0.5) } },
      },
    ],
    animationDuration: 800,
  };
}

function toneColor(p: Palette): Record<Tone, string> {
  return { ok: p.ok, danger: p.danger, warn: p.warn, accent: p.accent };
}

/** Formats a value with the metric unit for tooltips/labels. */
function fmtUnit(unit: string | undefined, v: number): string {
  if (unit === 'US$') {
    return 'US$ ' + Math.round(v).toLocaleString('pt-BR');
  }
  if (unit === '%') {
    return v.toLocaleString('pt-BR', { maximumFractionDigits: 1 }) + '%';
  }
  if (unit === 's') {
    return v.toLocaleString('pt-BR', { maximumFractionDigits: 1 }) + 's';
  }
  return Math.round(v).toLocaleString('pt-BR');
}

/** Compact axis-label formatter aware of the metric unit. */
function fmtAxis(unit: string | undefined, v: number): string {
  if (unit === 'US$') {
    return v >= 1000 ? v / 1000 + 'k' : String(v);
  }
  if (unit === '%') {
    return v + '%';
  }
  if (unit === 's') {
    return v + 's';
  }
  return v >= 1000 ? v / 1000 + 'k' : String(v);
}

/**
 * Polished large time-series chart for a KPI drill-down: smooth line + gradient
 * area in the tone color, unit-aware tooltip/axes, an average markLine and
 * inside+slider dataZoom for interactivity.
 */
export function kpiDetailPrimaryOptions(detail: KpiDetail, p: Palette): EChartsOption {
  const color = toneColor(p)[detail.tone];
  const unit = detail.primaryUnit ?? '';
  const values = detail.primary.map((pt) => pt.value);
  const avg = values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
  return {
    grid: { left: 8, right: 20, top: 34, bottom: 70, containLabel: true },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'line', lineStyle: { color: p.border } },
      formatter: (params: any) => {
        const arr = Array.isArray(params) ? params : [params];
        const first = arr[0] ?? {};
        const head = first.axisValueLabel ?? first.name ?? '';
        const val = fmtUnit(unit, Number(first.value ?? 0));
        return (
          `<div style="font-weight:600;margin-bottom:2px">${head}</div>` +
          `${first.marker ?? ''}${detail.primaryTitle}: <b>${val}</b>`
        );
      },
      ...tooltipStyle(p),
    },
    xAxis: {
      type: 'category',
      data: detail.primary.map((pt) => pt.t),
      boundaryGap: false,
      axisLine: { lineStyle: { color: p.border } },
      axisLabel: { color: p.muted, fontSize: 11 },
    },
    yAxis: {
      type: 'value',
      scale: unit !== '%',
      splitLine: { lineStyle: { color: p.grid } },
      axisLabel: {
        color: p.muted,
        fontSize: 11,
        formatter: (v: number) => fmtAxis(unit, v),
      },
    },
    dataZoom: [
      { type: 'inside', throttle: 50 },
      {
        type: 'slider',
        height: 18,
        bottom: 26,
        borderColor: p.border,
        fillerColor: rgba(color, 0.12),
        handleStyle: { color },
        textStyle: { color: p.muted, fontSize: 10 },
      },
    ],
    series: [
      {
        name: detail.primaryTitle,
        type: 'line',
        smooth: true,
        symbol: 'circle',
        symbolSize: 7,
        showSymbol: false,
        lineStyle: { width: 3, color },
        itemStyle: { color },
        areaStyle: { color: fade(color, 0.3, 0.02) as unknown as string },
        emphasis: { focus: 'series' },
        markLine: {
          silent: true,
          symbol: 'none',
          lineStyle: { color, type: 'dashed', width: 1.5, opacity: 0.7 },
          label: {
            color: p.muted,
            fontSize: 11,
            formatter: () => 'Média ' + fmtUnit(unit, avg),
          },
          data: [{ yAxis: avg }],
        },
        data: values,
      },
    ],
    animationDuration: 900,
    animationEasing: 'cubicOut',
  };
}

/**
 * Horizontal bar chart for a KPI drill-down breakdown (sorted, rounded bars,
 * value labels). Returns an empty option when there's no secondary data.
 */
export function kpiDetailBreakdownOptions(detail: KpiDetail, p: Palette): EChartsOption {
  const rows = detail.secondary;
  if (!rows || !rows.length) {
    return {};
  }
  const color = toneColor(p)[detail.tone];
  // Ascending so the largest bar renders on top.
  const data = [...rows].sort((a, b) => a.value - b.value);
  // Breakdowns are counts unless the metric is monetary.
  const unit = detail.primaryUnit === 'US$' ? 'US$' : '';
  return {
    grid: { left: 8, right: 52, top: 12, bottom: 8, containLabel: true },
    tooltip: {
      trigger: 'axis',
      axisPointer: { type: 'shadow' },
      valueFormatter: (v) => fmtUnit(unit, Number(v)),
      ...tooltipStyle(p),
    },
    xAxis: {
      type: 'value',
      splitLine: { lineStyle: { color: p.grid } },
      axisLabel: {
        color: p.muted,
        fontSize: 11,
        formatter: (v: number) => fmtAxis(unit, v),
      },
    },
    yAxis: {
      type: 'category',
      data: data.map((d) => d.label),
      axisLine: { lineStyle: { color: p.border } },
      axisLabel: { color: p.muted, fontSize: 11 },
    },
    series: [
      {
        type: 'bar',
        data: data.map((d) => d.value),
        barWidth: '58%',
        itemStyle: {
          borderRadius: [0, 5, 5, 0],
          color: {
            type: 'linear',
            x: 0,
            y: 0,
            x2: 1,
            y2: 0,
            colorStops: [
              { offset: 0, color: rgba(color, 0.45) },
              { offset: 1, color },
            ],
          } as unknown as string,
        },
        label: {
          show: true,
          position: 'right',
          color: p.muted,
          fontSize: 11,
          formatter: (o: any) => fmtUnit(unit, Number(o?.value ?? 0)),
        },
        emphasis: { itemStyle: { color } },
      },
    ],
    animationDuration: 900,
    animationDelay: (i: number) => i * 40,
  };
}

export function sparkOptions(values: number[], color: string): EChartsOption {
  return {
    grid: { left: 0, right: 0, top: 4, bottom: 0 },
    xAxis: { type: 'category', show: false, data: values.map((_, i) => i), boundaryGap: false },
    yAxis: { type: 'value', show: false, min: 'dataMin', max: 'dataMax' },
    tooltip: { show: false },
    series: [
      {
        type: 'line',
        data: values,
        smooth: true,
        symbol: 'none',
        lineStyle: { width: 2, color },
        areaStyle: { color: fade(color, 0.28, 0) as unknown as string },
      },
    ],
    animationDuration: 700,
  };
}
