import type { EChartsOption } from 'echarts';
import { ThemeMode } from '../core/theme.service';
import {
  CostPoint,
  DayPoint,
  ENVIRONMENTS,
  HeatCell,
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
      text: '#1a2230',
      muted: '#5c6b7f',
      border: '#e3e8f0',
      accent: '#2f7fe0',
      accent2: '#5aa0ff',
      ok: '#1f9d57',
      warn: '#d98a00',
      danger: '#e5484d',
      grid: 'rgba(20,30,45,0.07)',
      tooltipBg: '#ffffff',
    };
  }
  return {
    text: '#e3e8ee',
    muted: '#8a99a8',
    border: '#2d3a48',
    accent: '#4ea1ff',
    accent2: '#2f7fe0',
    ok: '#44d07b',
    warn: '#f5b942',
    danger: '#ff6b6b',
    grid: 'rgba(255,255,255,0.06)',
    tooltipBg: '#1a212b',
  };
}

function tooltipStyle(p: Palette) {
  return {
    backgroundColor: p.tooltipBg,
    borderColor: p.border,
    textStyle: { color: p.text, fontSize: 12 },
    extraCssText: 'border-radius:8px;box-shadow:0 8px 24px rgba(0,0,0,0.25);',
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
        lineStyle: { width: 2, color: p.accent },
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
        lineStyle: { width: 2, color: p.danger },
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
