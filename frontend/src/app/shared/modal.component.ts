import { ChangeDetectionStrategy, Component, input, output } from '@angular/core';
import { IconComponent } from './icon.component';

/**
 * Theme-aware modal/dialog. Renders nothing while closed.
 *
 * Usage:
 *   <app-modal [open]="isOpen()" [title]="'Title'" (close)="isOpen.set(false)">
 *     projected content
 *   </app-modal>
 *
 * Closes on backdrop click, close-button click, or Escape.
 */
@Component({
  selector: 'app-modal',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [IconComponent],
  host: { '(document:keydown.escape)': 'onEsc()' },
  template: `
    @if (open()) {
      <div class="modal-backdrop" (click)="close.emit()">
        <div
          class="modal-dialog"
          role="dialog"
          aria-modal="true"
          [attr.aria-label]="title()"
          (click)="$event.stopPropagation()"
        >
          <header class="modal-header">
            <div class="modal-heading">
              <h3 class="modal-title">{{ title() }}</h3>
              @if (subtitle()) {
                <p class="modal-subtitle">{{ subtitle() }}</p>
              }
            </div>
            <button
              type="button"
              class="modal-close"
              aria-label="Close"
              (click)="close.emit()"
            >
              <app-icon name="close" [size]="18" />
            </button>
          </header>
          <div class="modal-body">
            <ng-content />
          </div>
        </div>
      </div>
    }
  `,
  styles: [
    `
      .modal-backdrop {
        position: fixed;
        inset: 0;
        z-index: 1000;
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 24px;
        background: color-mix(in srgb, var(--scrim-bg) 100%, transparent);
        backdrop-filter: blur(4px);
        -webkit-backdrop-filter: blur(4px);
        animation: modal-backdrop-in 0.18s ease-out;
      }

      .modal-dialog {
        display: flex;
        flex-direction: column;
        width: min(94vw, 760px);
        max-width: 760px;
        max-height: 88vh;
        background: var(--panel);
        color: var(--text);
        border: 1px solid var(--border);
        border-radius: var(--radius);
        box-shadow: var(--shadow-lg);
        overflow: hidden;
        animation: modal-dialog-in 0.18s ease-out;
      }

      .modal-header {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 16px;
        padding: 18px 20px;
        border-bottom: 1px solid var(--border);
      }

      .modal-heading {
        min-width: 0;
      }

      .modal-title {
        margin: 0;
        font-size: 1.05rem;
        font-weight: 600;
        line-height: 1.3;
        color: var(--text);
      }

      .modal-subtitle {
        margin: 4px 0 0;
        font-size: 0.85rem;
        line-height: 1.4;
        color: var(--muted);
      }

      .modal-close {
        flex: 0 0 auto;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 32px;
        height: 32px;
        padding: 0;
        color: var(--muted);
        background: transparent;
        border: 1px solid transparent;
        border-radius: var(--radius);
        cursor: pointer;
        transition: color 0.12s ease, background 0.12s ease, border-color 0.12s ease;
      }

      .modal-close:hover {
        color: var(--text);
        background: var(--hover);
        border-color: var(--border);
      }

      .modal-close:focus-visible {
        outline: 2px solid var(--accent);
        outline-offset: 2px;
      }

      .modal-body {
        flex: 1 1 auto;
        min-height: 0;
        padding: 20px;
        overflow-y: auto;
      }

      @keyframes modal-backdrop-in {
        from {
          opacity: 0;
        }
        to {
          opacity: 1;
        }
      }

      @keyframes modal-dialog-in {
        from {
          opacity: 0;
          transform: translateY(12px) scale(0.98);
        }
        to {
          opacity: 1;
          transform: translateY(0) scale(1);
        }
      }

      @media (prefers-reduced-motion: reduce) {
        .modal-backdrop,
        .modal-dialog {
          animation: none;
        }
      }
    `,
  ],
})
export class ModalComponent {
  /** Whether the modal is visible. */
  readonly open = input<boolean>(false);
  /** Header title, also used as the dialog aria-label. */
  readonly title = input<string>('');
  /** Optional muted subtitle below the title. */
  readonly subtitle = input<string>('');

  /** Emitted on backdrop click, close-button click, or Escape (while open). */
  readonly close = output<void>();

  onEsc(): void {
    if (this.open()) {
      this.close.emit();
    }
  }
}
