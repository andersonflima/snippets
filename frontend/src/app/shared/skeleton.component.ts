import { ChangeDetectionStrategy, Component, input } from '@angular/core';

/**
 * Shimmering placeholder block used for loading states. Size it via inputs:
 *   <app-skeleton height="90px" />
 *   <app-skeleton height="1rem" width="60%" radius="6px" />
 */
@Component({
  selector: 'app-skeleton',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: '',
  host: {
    class: 'skeleton',
    '[style.height]': 'height()',
    '[style.width]': 'width()',
    '[style.borderRadius]': 'radius()',
  },
  styles: [
    `
      :host {
        display: block;
        position: relative;
        overflow: hidden;
        background: var(--panel-2);
      }
      :host::after {
        content: '';
        position: absolute;
        inset: 0;
        transform: translateX(-100%);
        background: linear-gradient(
          90deg,
          transparent,
          color-mix(in srgb, var(--text) 9%, transparent),
          transparent
        );
        animation: sk-shimmer 1.35s ease-in-out infinite;
      }
      @keyframes sk-shimmer {
        100% {
          transform: translateX(100%);
        }
      }
      @media (prefers-reduced-motion: reduce) {
        :host::after {
          animation: none;
        }
      }
    `,
  ],
})
export class SkeletonComponent {
  readonly height = input<string>('1rem');
  readonly width = input<string>('100%');
  readonly radius = input<string>('10px');
}
