import { ChangeDetectionStrategy, Component, inject } from '@angular/core';
import { IconComponent } from './icon.component';
import { ToastKind, ToastService } from './toast.service';

/**
 * Fixed toast stack (bottom-right) driven by {@link ToastService}. Each toast
 * slides in, shows a tone icon and an auto-dismiss progress bar, and can be
 * closed manually. Rendered once, globally, from the app root.
 */
@Component({
  selector: 'app-toast-host',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [IconComponent],
  template: `
    <div class="toast-host" aria-live="polite" aria-atomic="true">
      @for (t of toasts(); track t.id) {
        <div class="toast" [attr.data-kind]="t.kind" role="status">
          <span class="toast-ic"><app-icon [name]="iconFor(t.kind)" [size]="18" /></span>
          <div class="toast-body">
            <strong class="toast-title">{{ t.title }}</strong>
            @if (t.message) {
              <span class="toast-msg">{{ t.message }}</span>
            }
          </div>
          <button
            type="button"
            class="toast-close"
            (click)="toast.dismiss(t.id)"
            aria-label="Fechar notificação"
          >
            <app-icon name="close" [size]="15" />
          </button>
          @if (t.duration > 0) {
            <span class="toast-bar" [style.animation-duration.ms]="t.duration"></span>
          }
        </div>
      }
    </div>
  `,
})
export class ToastHostComponent {
  readonly toast = inject(ToastService);
  readonly toasts = this.toast.toasts;

  iconFor(kind: ToastKind): string {
    switch (kind) {
      case 'success':
        return 'check';
      case 'error':
        return 'bolt';
      case 'warn':
        return 'bell';
      default:
        return 'activity';
    }
  }
}
