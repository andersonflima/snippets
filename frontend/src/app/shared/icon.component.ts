import { ChangeDetectionStrategy, Component, computed, input } from '@angular/core';
import { ICONS } from './icons';

/** Inline stroke icon by name (see {@link ICONS}). Sizes to the given `size`. */
@Component({
  selector: 'app-icon',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <svg
      [attr.width]="size()"
      [attr.height]="size()"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      stroke-width="2"
      stroke-linecap="round"
      stroke-linejoin="round"
      aria-hidden="true"
    >
      <path [attr.d]="d()" />
    </svg>
  `,
  styles: [':host{display:inline-flex;align-items:center;justify-content:center;line-height:0}'],
})
export class IconComponent {
  readonly name = input.required<string>();
  readonly size = input<number>(18);
  readonly d = computed(() => ICONS[this.name()] ?? '');
}
