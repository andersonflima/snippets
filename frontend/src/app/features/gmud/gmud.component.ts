import {
  ChangeDetectionStrategy,
  Component,
  computed,
  inject,
  signal,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { IconComponent } from '../../shared/icon.component';
import { ModalComponent } from '../../shared/modal.component';
import { SkeletonComponent } from '../../shared/skeleton.component';
import { ToastService } from '../../shared/toast.service';
import { SettingsService } from '../../core/settings.service';
import { GmudCheck, GmudCheckKey, GmudDetail, GmudService } from './gmud.service';

/** A check ready to render: label, icon and the resolved value string. */
interface CheckRow {
  key: GmudCheckKey;
  label: string;
  icon: string;
  check: GmudCheck;
  valueLabel: string;
}

const CHECK_META: Record<GmudCheckKey, { label: string; icon: string }> = {
  state: { label: 'Status de implementação', icon: 'activity' },
  window: { label: 'Janela planejada', icon: 'clock' },
  approval: { label: 'Aprovações', icon: 'check' },
  conflict: { label: 'Conflitos', icon: 'shield' },
  tasks: { label: 'Tarefas registradas', icon: 'layers' },
};

const CHECK_ORDER: GmudCheckKey[] = ['state', 'window', 'approval', 'conflict', 'tasks'];

/** Monitors a ServiceNow change (GMUD): runs the validation checks and renders verdicts. */
@Component({
  selector: 'app-gmud',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [FormsModule, IconComponent, ModalComponent, SkeletonComponent],
  template: `
    <div class="page-head anim-in">
      <div>
        <h1>Monitoramento de GMUD</h1>
        <p class="muted">
          Consulte uma mudança no ServiceNow e verifique liberação para execução ·
          ambiente <strong>{{ activeEnv() }}</strong>
        </p>
      </div>
    </div>

    <form class="card toolbar gmud-form anim-in" (ngSubmit)="verify()">
      <div class="search-box">
        <app-icon name="search" [size]="16" class="muted" />
        <input
          type="search"
          placeholder="Número da GMUD (ex.: CHG0012345)"
          [(ngModel)]="changeInput"
          name="changeNumber"
          aria-label="Número da GMUD"
        />
      </div>
      <button type="submit" class="primary" [disabled]="loading() || !changeInput.trim()">
        <app-icon name="refresh" [size]="16" [class.spin]="loading()" />
        {{ loading() ? 'Verificando…' : 'Verificar' }}
      </button>
    </form>

    @if (loading()) {
      <div class="card"><app-skeleton height="72px" /></div>
      <section class="gmud-grid">
        @for (s of skeletonCells; track s) {
          <div class="card"><app-skeleton height="96px" /></div>
        }
      </section>
    } @else if (error(); as msg) {
      <div class="card">
        <h3>Falha na verificação <span class="badge err">erro</span></h3>
        <p class="muted" style="margin:0">{{ msg }}</p>
      </div>
    } @else if (result(); as d) {
      <article class="card gmud-verdict anim-in">
        <div class="gmud-verdict-main">
          <span class="muted gmud-change">GMUD</span>
          <h2 class="gmud-change-num">{{ d.change }}</h2>
        </div>
        <span class="badge gmud-verdict-badge" [class.ok]="d.allowed" [class.err]="!d.allowed">
          {{ d.allowed ? 'liberada' : 'bloqueada' }}
        </span>
        <button type="button" class="ghost" (click)="detailOpen.set(true)">
          <app-icon name="expand" [size]="15" /> Ver detalhe
        </button>
      </article>

      @if (!d.allowed && d.reasons.length) {
        <div class="note gmud-reasons">
          Pendências:
          @for (r of d.reasons; track r) {
            <span class="badge err">{{ reasonLabel(r) }}</span>
          }
        </div>
      }

      <section class="gmud-grid">
        @for (row of rows(); track row.key) {
          <article class="card gmud-check" [attr.data-ok]="row.check.ok">
            <span class="gmud-check-ic"><app-icon [name]="row.icon" [size]="18" /></span>
            <div class="gmud-check-main">
              <span class="gmud-check-label">{{ row.label }}</span>
              <span class="gmud-check-val">{{ row.valueLabel }}</span>
              <span class="muted gmud-check-hint">
                {{ row.check.required ? 'obrigatória' : 'opcional' }}
              </span>
            </div>
            <span class="badge" [class.ok]="row.check.ok" [class.err]="row.check.required && !row.check.ok">
              {{ badgeLabel(row.check) }}
            </span>
          </article>
        }
      </section>
    } @else {
      <div class="card empty">
        <app-icon name="shield" [size]="28" class="muted" />
        <p class="muted">Informe uma GMUD e clique em “Verificar” para monitorar as checagens.</p>
      </div>
    }

    <app-modal
      [open]="detailOpen() && !!result()"
      [title]="'GMUD ' + (result()?.change ?? '')"
      subtitle="Retrato completo retornado pelo ServiceNow"
      (close)="detailOpen.set(false)"
    >
      @if (result(); as d) {
        <div class="gmud-detail">
          <div class="kd-stats">
            <div class="kd-stat">
              <span class="kd-stat-val">{{ d.allowed ? 'Liberada' : 'Bloqueada' }}</span>
              <span class="muted">Veredito</span>
            </div>
            <div class="kd-stat">
              <span class="kd-stat-val">{{ d.state || '—' }}</span>
              <span class="muted">Estado</span>
            </div>
            <div class="kd-stat">
              <span class="kd-stat-val">{{ d.withinWindow ? 'Sim' : 'Não' }}</span>
              <span class="muted">Na janela</span>
            </div>
            <div class="kd-stat">
              <span class="kd-stat-val">{{ d.approval || '—' }}</span>
              <span class="muted">Aprovação</span>
            </div>
            <div class="kd-stat">
              <span class="kd-stat-val">{{ d.conflictStatus || '—' }}</span>
              <span class="muted">Conflitos</span>
            </div>
            <div class="kd-stat">
              <span class="kd-stat-val">{{ d.taskCount }}</span>
              <span class="muted">Tarefas</span>
            </div>
          </div>
          <pre class="response gmud-raw">{{ pretty(d) }}</pre>
        </div>
      }
    </app-modal>
  `,
  styles: [
    `
      .gmud-form {
        gap: 0.6rem;
      }
      .gmud-form .search-box {
        flex: 1 1 260px;
      }
      .primary .spin {
        animation: gmud-spin 0.8s linear infinite;
      }
      @keyframes gmud-spin {
        to {
          transform: rotate(360deg);
        }
      }
      .gmud-verdict {
        display: flex;
        align-items: center;
        gap: 0.9rem;
        flex-wrap: wrap;
        margin-bottom: 0.85rem;
      }
      .gmud-verdict-main {
        display: flex;
        flex-direction: column;
        gap: 0.1rem;
        flex: 1 1 auto;
        min-width: 0;
      }
      .gmud-change {
        font-size: 0.72rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
      }
      .gmud-change-num {
        margin: 0;
        font-size: 1.35rem;
        letter-spacing: -0.01em;
      }
      .gmud-verdict-badge {
        font-size: 0.82rem;
        padding: 0.28rem 0.7rem;
      }
      .gmud-reasons {
        display: flex;
        align-items: center;
        flex-wrap: wrap;
        gap: 0.4rem;
      }
      .gmud-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
        gap: 0.85rem;
      }
      .gmud-check {
        display: flex;
        align-items: center;
        gap: 0.75rem;
      }
      .gmud-check-ic {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 36px;
        height: 36px;
        border-radius: 9px;
        background: var(--panel-2);
        border: 1px solid var(--border);
        color: var(--muted);
        flex: 0 0 auto;
      }
      .gmud-check[data-ok='true'] .gmud-check-ic {
        color: var(--ok);
        background: var(--soft-ok);
        border-color: transparent;
      }
      .gmud-check-main {
        display: flex;
        flex-direction: column;
        gap: 0.1rem;
        min-width: 0;
        flex: 1 1 auto;
      }
      .gmud-check-label {
        font-weight: 600;
        font-size: 0.88rem;
      }
      .gmud-check-val {
        font-size: 0.82rem;
        word-break: break-word;
      }
      .gmud-check-hint {
        font-size: 0.72rem;
      }
      .gmud-detail {
        display: flex;
        flex-direction: column;
        gap: 1rem;
      }
      .gmud-raw {
        margin: 0;
        max-height: 40vh;
        overflow: auto;
      }
    `,
  ],
})
export class GmudComponent {
  private readonly gmud = inject(GmudService);
  private readonly settings = inject(SettingsService);
  private readonly toast = inject(ToastService);

  readonly activeEnv = this.settings.activeEnv;

  /** Bound to the input via ngModel; the trimmed value drives verification. */
  changeInput = '';

  readonly changeNumber = signal('');
  readonly loading = signal(false);
  readonly result = signal<GmudDetail | null>(null);
  readonly error = signal<string | null>(null);
  readonly detailOpen = signal(false);

  readonly skeletonCells = [0, 1, 2, 3, 4];

  /** Ordered, render-ready rows for the five validation checks. */
  readonly rows = computed<CheckRow[]>(() => {
    const d = this.result();
    if (!d) {
      return [];
    }
    return CHECK_ORDER.filter((key) => d.checks[key] != null).map((key) => {
      const check = d.checks[key];
      const meta = CHECK_META[key];
      return {
        key,
        label: meta.label,
        icon: meta.icon,
        check,
        valueLabel: this.valueLabel(key, check.value),
      };
    });
  });

  async verify(): Promise<void> {
    const number = this.changeInput.trim();
    if (!number || this.loading()) {
      return;
    }
    this.changeNumber.set(number);
    this.loading.set(true);
    this.error.set(null);
    this.detailOpen.set(false);
    try {
      const detail = await this.gmud.status(number);
      this.result.set(detail);
      if (detail.allowed) {
        this.toast.success('GMUD liberada', `${detail.change} atende a todas as checagens.`);
      } else {
        this.toast.warn(
          'GMUD bloqueada',
          `${detail.change}: ${detail.reasons.map((r) => this.reasonLabel(r)).join(', ')}.`,
        );
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      this.result.set(null);
      this.error.set(msg);
      this.toast.error('Falha ao verificar GMUD', msg);
    } finally {
      this.loading.set(false);
    }
  }

  badgeLabel(check: GmudCheck): string {
    if (check.ok) {
      return 'ok';
    }
    return check.required ? 'falha' : 'atenção';
  }

  reasonLabel(key: string): string {
    return CHECK_META[key as GmudCheckKey]?.label ?? key;
  }

  private valueLabel(key: GmudCheckKey, value: unknown): string {
    if (key === 'window' && value && typeof value === 'object') {
      const win = value as { start?: string | null; end?: string | null };
      const start = win.start ?? '—';
      const end = win.end ?? '—';
      return `${start} → ${end}`;
    }
    if (key === 'tasks') {
      return `${value ?? 0} tarefa(s)`;
    }
    if (value === '' || value == null) {
      return '—';
    }
    return String(value);
  }

  pretty(body: unknown): string {
    try {
      return JSON.stringify(body, null, 2);
    } catch {
      return String(body);
    }
  }
}
