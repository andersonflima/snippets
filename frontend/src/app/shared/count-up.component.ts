import {
  ChangeDetectionStrategy,
  Component,
  afterNextRender,
  input,
  signal,
} from '@angular/core';

/**
 * Animated number that eases from 0 to `value` on first render. Accepts optional
 * `prefix`/`suffix` and `decimals` for currency/percent formatting. Purely
 * cosmetic; falls back to the final value if animation cannot run.
 */
@Component({
  selector: 'app-count-up',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `{{ prefix() }}{{ display() }}{{ suffix() }}`,
})
export class CountUpComponent {
  readonly value = input.required<number>();
  readonly decimals = input<number>(0);
  readonly prefix = input<string>('');
  readonly suffix = input<string>('');
  readonly duration = input<number>(900);

  private readonly current = signal(0);
  readonly display = signal('0');

  constructor() {
    afterNextRender(() => this.animate());
  }

  private animate(): void {
    const target = this.value();
    const dur = this.duration();
    const start = performance.now();

    const step = (now: number) => {
      const t = Math.min(1, (now - start) / dur);
      // easeOutCubic for a lively but settled finish.
      const eased = 1 - Math.pow(1 - t, 3);
      this.current.set(target * eased);
      this.display.set(this.format(this.current()));
      if (t < 1) {
        requestAnimationFrame(step);
      } else {
        this.display.set(this.format(target));
      }
    };
    requestAnimationFrame(step);
  }

  private format(n: number): string {
    return n.toLocaleString('pt-BR', {
      minimumFractionDigits: this.decimals(),
      maximumFractionDigits: this.decimals(),
    });
  }
}
