import {
  ChangeDetectionStrategy,
  Component,
  ElementRef,
  OnDestroy,
  afterNextRender,
  effect,
  input,
  viewChild,
} from '@angular/core';
import type { ECharts, EChartsOption } from 'echarts';
import * as echarts from 'echarts';

/**
 * Thin, dependency-light wrapper around Apache ECharts. Owns the chart lifecycle
 * (init after render, re-apply options on change, resize with the container, and
 * dispose on destroy) so feature components only supply reactive `options`.
 *
 * Kept framework-free (no ngx-echarts) to avoid peer-dep friction with the
 * zoneless Angular 20 setup — ECharts renders outside Angular's change detection.
 */
@Component({
  selector: 'app-echart',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `<div class="echart-host" #host [style.height]="height()"></div>`,
  styles: [
    `
      :host {
        display: block;
        width: 100%;
      }
      .echart-host {
        width: 100%;
      }
    `,
  ],
})
export class EChartComponent implements OnDestroy {
  readonly options = input.required<EChartsOption>();
  readonly height = input<string>('320px');

  private readonly host = viewChild.required<ElementRef<HTMLDivElement>>('host');
  private chart: ECharts | null = null;
  private observer: ResizeObserver | null = null;

  constructor() {
    afterNextRender(() => {
      this.chart = echarts.init(this.host().nativeElement, undefined, {
        renderer: 'canvas',
      });
      this.chart.setOption(this.options());
      this.observer = new ResizeObserver(() => this.chart?.resize());
      this.observer.observe(this.host().nativeElement);
    });

    // Re-apply whenever the reactive options change (theme/period switches).
    effect(() => {
      const opts = this.options();
      this.chart?.setOption(opts, true);
    });
  }

  ngOnDestroy(): void {
    this.dispose();
  }

  private dispose(): void {
    this.observer?.disconnect();
    this.observer = null;
    this.chart?.dispose();
    this.chart = null;
  }
}
