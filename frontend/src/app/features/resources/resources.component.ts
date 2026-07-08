import {
  ChangeDetectionStrategy,
  Component,
  computed,
  effect,
  inject,
  signal,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { IconComponent } from '../../shared/icon.component';
import { ModalComponent } from '../../shared/modal.component';
import { ToastService } from '../../shared/toast.service';
import { SkeletonComponent } from '../../shared/skeleton.component';
import { SettingsService } from '../../core/settings.service';
import {
  ProductScope,
  ResourceFilters,
  ResourceItem,
  ResourcesService,
} from './resources.service';

interface ProductChip {
  value: ProductScope;
  label: string;
  icon: string;
}

const PRODUCTS: ProductChip[] = [
  { value: 'all', label: 'Todos', icon: 'layers' },
  { value: 'rds', label: 'RDS', icon: 'server' },
  { value: 'ec2', label: 'EC2', icon: 'server' },
  { value: 'ebs', label: 'EBS', icon: 'layers' },
  { value: 'elb', label: 'ELB', icon: 'activity' },
  { value: 'eip', label: 'EIP', icon: 'bolt' },
  { value: 'snapshot', label: 'Snapshots', icon: 'shield' },
  { value: 'kms', label: 'KMS', icon: 'shield' },
  { value: 'vpc-endpoint', label: 'VPC Endpoints', icon: 'leaf' },
];

const STATUSES = [
  'all',
  'available',
  'running',
  'stopped',
  'in-use',
  'pending',
  'error',
  'terminated',
];

const ENVS = ['all', 'dev', 'homol', 'staging', 'prod'];

/** Inventory landing page: filterable, card-based list of AWS resources. */
@Component({
  selector: 'app-resources',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [FormsModule, RouterLink, IconComponent, SkeletonComponent, ModalComponent],
  template: `
    <div class="page-head anim-in">
      <div>
        <h1>Recursos</h1>
        <p class="muted">
          Inventário de recursos AWS via serviço de insights ·
          ambiente <strong>{{ activeEnv() }}</strong>
        </p>
      </div>
      @if (loaded(); as list) {
        <span class="badge">{{ list.total }} recurso(s)</span>
      }
    </div>

    <div class="chips anim-in">
      @for (p of products; track p.value) {
        <button
          type="button"
          class="chip"
          [class.active]="product() === p.value"
          (click)="product.set(p.value)"
        >
          <app-icon [name]="p.icon" [size]="14" />
          {{ p.label }}
        </button>
      }
    </div>

    <div class="card toolbar filters anim-in">
      <div class="search-box">
        <app-icon name="search" [size]="16" class="muted" />
        <input
          type="search"
          placeholder="Buscar por nome, id ou tag…"
          [(ngModel)]="search"
          aria-label="Buscar recursos"
        />
      </div>
      <label class="inline">
        Status
        <select [(ngModel)]="status" aria-label="Filtrar por status">
          @for (s of statuses; track s) {
            <option [value]="s">{{ s === 'all' ? 'Todos' : s }}</option>
          }
        </select>
      </label>
      <label class="inline">
        Ambiente
        <select [(ngModel)]="env" aria-label="Filtrar por ambiente">
          @for (e of envs; track e) {
            <option [value]="e">{{ e === 'all' ? 'Todos' : e }}</option>
          }
        </select>
      </label>
      <button type="button" class="ghost" (click)="manualReload()" [disabled]="loading()" title="Recarregar">
        <app-icon name="refresh" [size]="16" [class.spin]="loading()" />
      </button>
    </div>

    @if (loading()) {
      <section class="res-grid">
        @for (s of skeletonCards; track s) {
          <div class="card"><app-skeleton height="132px" /></div>
        }
      </section>
    } @else if (error(); as msg) {
      <div class="card">
        <h3>Falha ao carregar <span class="badge err">erro</span></h3>
        <p class="muted" style="margin:0">{{ msg }}</p>
      </div>
    } @else if (items().length === 0) {
      <div class="card empty">
        <app-icon name="layers" [size]="28" class="muted" />
        <p class="muted">Nenhum recurso encontrado com os filtros atuais.</p>
      </div>
    } @else {
      <section class="res-grid">
        @for (r of items(); track r.id; let i = $index) {
          <a
            class="card res-card anim-in"
            [style.animation-delay.ms]="i * 40"
            [routerLink]="['/resources', r.product, r.id]"
          >
            <div class="res-top">
              <div class="res-id">
                <span class="res-name">{{ r.name }}</span>
                <span class="path">{{ r.id }}</span>
              </div>
              <div class="res-top-right">
                <span class="prod-badge">{{ r.product }}</span>
                <button
                  type="button"
                  class="peek-btn"
                  aria-label="Ver detalhes rápidos"
                  title="Detalhes rápidos"
                  (click)="openPeek(r, $event)"
                >
                  <app-icon name="expand" [size]="15" />
                </button>
              </div>
            </div>

            <div class="res-meta">
              <span class="badge" [attr.data-status]="statusTone(r.status)">
                {{ r.status }}
              </span>
              <span class="muted">{{ r.type }}</span>
              <span class="muted">· {{ r.env }} · {{ r.region }}</span>
            </div>

            <div class="res-foot">
              <div class="cost">
                <span class="cost-val">{{ money(r.monthlyCost) }}</span>
                <span class="muted">/mês</span>
              </div>
              <div class="util" [title]="r.utilizationPct + '% de utilização'">
                <div class="util-bar">
                  <div
                    class="util-fill"
                    [attr.data-tone]="utilTone(r.utilizationPct)"
                    [style.width.%]="clamp(r.utilizationPct)"
                  ></div>
                </div>
                <span class="util-pct">{{ r.utilizationPct }}%</span>
              </div>
            </div>
          </a>
        }
      </section>
    }

    <app-modal
      [open]="!!peek()"
      [title]="peek()?.name ?? ''"
      [subtitle]="peek() ? peek()!.product + ' · ' + peek()!.type : ''"
      (close)="peek.set(null)"
    >
      @if (peek(); as r) {
        <div class="peek">
          <div class="peek-head">
            <span class="prod-badge">{{ r.product }}</span>
            <span class="badge" [attr.data-status]="statusTone(r.status)">{{ r.status }}</span>
            <code class="path">{{ r.id }}</code>
          </div>

          <div class="kd-stats">
            <div class="kd-stat">
              <span class="kd-stat-val">{{ money(r.monthlyCost) }}</span>
              <span class="muted">Custo / mês</span>
            </div>
            <div class="kd-stat">
              <span class="kd-stat-val">{{ r.utilizationPct }}%</span>
              <span class="muted">Utilização</span>
            </div>
            <div class="kd-stat">
              <span class="kd-stat-val">{{ r.env }}</span>
              <span class="muted">Ambiente</span>
            </div>
            <div class="kd-stat">
              <span class="kd-stat-val">{{ r.region }}</span>
              <span class="muted">Região</span>
            </div>
          </div>

          <div class="util" [title]="r.utilizationPct + '% de utilização'">
            <div class="util-bar">
              <div
                class="util-fill"
                [attr.data-tone]="utilTone(r.utilizationPct)"
                [style.width.%]="clamp(r.utilizationPct)"
              ></div>
            </div>
            <span class="util-pct">{{ r.utilizationPct }}%</span>
          </div>

          <dl class="peek-list">
            <div class="peek-row">
              <dt>Tipo</dt>
              <dd>{{ r.type }}</dd>
            </div>
            <div class="peek-row">
              <dt>Tamanho</dt>
              <dd>{{ r.size }}</dd>
            </div>
            <div class="peek-row">
              <dt>Criado em</dt>
              <dd>{{ formatDate(r.createdAt) }}</dd>
            </div>
          </dl>

          @if (tagEntries(r).length) {
            <div class="peek-tags">
              <span class="peek-tags-head muted">Tags</span>
              <dl class="peek-list">
                @for (t of tagEntries(r); track t[0]) {
                  <div class="peek-row">
                    <dt>{{ t[0] }}</dt>
                    <dd><code class="path">{{ t[1] }}</code></dd>
                  </div>
                }
              </dl>
            </div>
          }
        </div>
      }
    </app-modal>
  `,
  styles: [
    `
      .chips {
        display: flex;
        flex-wrap: wrap;
        gap: 0.4rem;
        margin-bottom: 1rem;
      }
      .chip {
        display: inline-flex;
        align-items: center;
        gap: 0.35rem;
        padding: 0.4rem 0.75rem;
        border-radius: 999px;
        font-size: 0.82rem;
        color: var(--muted);
      }
      .chip.active {
        background: var(--accent-2);
        border-color: var(--accent-2);
        color: #fff;
      }
      .chip.active :is(svg) {
        color: #fff;
      }

      .filters {
        gap: 0.85rem;
        align-items: center;
      }
      .search-box {
        display: flex;
        align-items: center;
        gap: 0.45rem;
        flex: 1 1 240px;
        padding: 0 0.6rem;
        background: var(--panel-2);
        border: 1px solid var(--border);
        border-radius: 6px;
      }
      .search-box input {
        border: none;
        background: transparent;
        padding-left: 0;
      }
      .search-box input:focus {
        box-shadow: none;
      }
      label.inline {
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
        margin: 0;
        font-size: 0.8rem;
        color: var(--muted);
      }
      label.inline select {
        width: auto;
      }

      .empty {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 0.5rem;
        padding: 2.5rem 1rem;
        text-align: center;
      }

      .res-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
        gap: 1rem;
      }
      .res-card {
        display: flex;
        flex-direction: column;
        gap: 0.7rem;
        color: inherit;
      }
      .res-card:hover {
        text-decoration: none;
        transform: translateY(-3px);
        box-shadow: var(--shadow-md);
        border-color: color-mix(in srgb, var(--accent) 35%, var(--border));
      }
      .res-top {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: 0.6rem;
      }
      .res-id {
        display: flex;
        flex-direction: column;
        min-width: 0;
      }
      .res-name {
        font-weight: 600;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }
      .prod-badge {
        flex: 0 0 auto;
        font-family: ui-monospace, "SF Mono", Menlo, monospace;
        font-size: 0.68rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.03em;
        padding: 0.15rem 0.45rem;
        border-radius: 5px;
        background: var(--soft-accent);
        color: var(--accent);
      }
      .res-meta {
        display: flex;
        align-items: center;
        flex-wrap: wrap;
        gap: 0.4rem;
        font-size: 0.82rem;
      }
      .res-foot {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 0.75rem;
        margin-top: auto;
        padding-top: 0.55rem;
        border-top: 1px solid var(--border);
      }
      .cost {
        display: flex;
        align-items: baseline;
        gap: 0.25rem;
        font-size: 0.8rem;
      }
      .cost-val {
        font-weight: 700;
        font-size: 0.95rem;
      }
      .util {
        display: flex;
        align-items: center;
        gap: 0.45rem;
        flex: 0 0 auto;
      }
      .util-bar {
        width: 84px;
        height: 6px;
        border-radius: 999px;
        background: var(--panel-2);
        overflow: hidden;
      }
      .util-fill {
        height: 100%;
        border-radius: 999px;
        background: var(--accent);
        transition: width 0.4s ease;
      }
      .util-fill[data-tone="ok"] {
        background: var(--ok);
      }
      .util-fill[data-tone="warn"] {
        background: var(--warn);
      }
      .util-fill[data-tone="danger"] {
        background: var(--danger);
      }
      .util-pct {
        font-size: 0.75rem;
        color: var(--muted);
        min-width: 2.4rem;
        text-align: right;
      }

      .badge[data-status="ok"] {
        background: var(--soft-ok);
        color: var(--ok);
      }
      .badge[data-status="warn"] {
        background: var(--soft-warn);
        color: var(--warn);
      }
      .badge[data-status="danger"] {
        background: var(--soft-danger);
        color: var(--danger);
      }
      .badge[data-status="muted"] {
        background: var(--panel-2);
        color: var(--muted);
      }

      .res-top-right {
        display: flex;
        align-items: center;
        gap: 0.4rem;
        flex: 0 0 auto;
      }
      .peek-btn {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 26px;
        height: 26px;
        padding: 0;
        color: var(--muted);
        background: transparent;
        border: 1px solid transparent;
        border-radius: 6px;
        cursor: pointer;
        transition: color 0.12s ease, background 0.12s ease, border-color 0.12s ease;
      }
      .peek-btn:hover {
        color: var(--accent);
        background: var(--hover);
        border-color: var(--border);
      }
      .peek-btn:focus-visible {
        outline: 2px solid var(--accent);
        outline-offset: 2px;
      }

      .peek {
        display: flex;
        flex-direction: column;
        gap: 1rem;
      }
      .peek-head {
        display: flex;
        align-items: center;
        flex-wrap: wrap;
        gap: 0.5rem;
      }
      .peek-list {
        display: flex;
        flex-direction: column;
        gap: 0;
        margin: 0;
      }
      .peek-row {
        display: flex;
        align-items: baseline;
        justify-content: space-between;
        gap: 1rem;
        padding: 0.45rem 0;
        border-bottom: 1px solid var(--border);
      }
      .peek-row:last-child {
        border-bottom: none;
      }
      .peek-row dt {
        color: var(--muted);
        font-size: 0.82rem;
      }
      .peek-row dd {
        margin: 0;
        text-align: right;
        min-width: 0;
        word-break: break-word;
      }
      .peek-tags-head {
        display: block;
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        margin-bottom: 0.35rem;
      }

      @media (max-width: 600px) {
        .res-grid {
          grid-template-columns: 1fr;
        }
      }
    `,
  ],
})
export class ResourcesComponent {
  private readonly resources = inject(ResourcesService);
  private readonly settings = inject(SettingsService);
  private readonly toast = inject(ToastService);

  readonly products = PRODUCTS;
  readonly skeletonCards = [1, 2, 3, 4, 5, 6];
  readonly statuses = STATUSES;
  readonly envs = ENVS;
  readonly activeEnv = this.settings.activeEnv;

  readonly product = signal<ProductScope>('all');
  readonly search = signal('');
  readonly status = signal('all');
  readonly env = signal('all');

  readonly loading = signal(false);
  readonly error = signal<string | null>(null);

  /** Resource shown in the quick-peek modal, or null when closed. */
  readonly peek = signal<ResourceItem | null>(null);
  private readonly result = signal<{ items: ResourceItem[]; total: number } | null>(null);

  readonly loaded = computed(() => this.result());
  readonly items = computed(() => this.result()?.items ?? []);

  private readonly activeFilters = computed<ResourceFilters>(() => ({
    search: this.search().trim() || undefined,
    status: this.status() === 'all' ? undefined : this.status(),
    env: this.env() === 'all' ? undefined : this.env(),
  }));

  private token = 0;

  constructor() {
    // Reload whenever the product or any filter (incl. active env) changes.
    effect(() => {
      const product = this.product();
      const filters = this.activeFilters();
      void this.load(product, filters);
    });
  }

  async manualReload(): Promise<void> {
    if (this.loading()) return;
    await this.load(this.product(), this.activeFilters());
    const err = this.error();
    if (err) {
      this.toast.error('Falha ao recarregar', err);
    } else {
      this.toast.success('Recursos atualizados', `${this.result()?.total ?? 0} recurso(s) encontrados.`);
    }
  }

  private async load(product: ProductScope, filters: ResourceFilters): Promise<void> {
    const current = ++this.token;
    this.loading.set(true);
    this.error.set(null);
    try {
      const detail = await this.resources.list(product, filters);
      if (current !== this.token) {
        return; // a newer request superseded this one
      }
      this.result.set({ items: detail.items, total: detail.total });
    } catch (err) {
      if (current !== this.token) {
        return;
      }
      this.result.set(null);
      this.error.set(err instanceof Error ? err.message : String(err));
    } finally {
      if (current === this.token) {
        this.loading.set(false);
      }
    }
  }

  /** Opens the quick-peek modal without triggering the card's routerLink. */
  openPeek(r: ResourceItem, ev: Event): void {
    ev.stopPropagation();
    ev.preventDefault();
    this.peek.set(r);
  }

  tagEntries(r: ResourceItem): ReadonlyArray<readonly [string, string]> {
    return Object.entries(r.tags);
  }

  formatDate(iso: string): string {
    const date = new Date(iso);
    if (Number.isNaN(date.getTime())) {
      return iso;
    }
    return date.toLocaleString('pt-BR', {
      dateStyle: 'medium',
      timeStyle: 'short',
    });
  }

  money(value: number): string {
    return value.toLocaleString('pt-BR', {
      style: 'currency',
      currency: 'USD',
      maximumFractionDigits: 0,
    });
  }

  clamp(pct: number): number {
    return Math.max(0, Math.min(100, pct));
  }

  utilTone(pct: number): 'ok' | 'accent' | 'warn' | 'danger' {
    if (pct >= 90) {
      return 'danger';
    }
    if (pct >= 75) {
      return 'warn';
    }
    if (pct < 15) {
      return 'ok';
    }
    return 'accent';
  }

  statusTone(status: string): 'ok' | 'warn' | 'danger' | 'muted' {
    const s = status.toLowerCase();
    if (['available', 'running', 'in-use', 'active', 'completed'].includes(s)) {
      return 'ok';
    }
    if (['pending', 'modifying', 'creating', 'rebooting'].includes(s)) {
      return 'warn';
    }
    if (['error', 'failed', 'terminated', 'deleting'].includes(s)) {
      return 'danger';
    }
    return 'muted';
  }
}
