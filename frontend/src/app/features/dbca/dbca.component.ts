import {
  ChangeDetectionStrategy,
  Component,
  computed,
  inject,
  signal,
} from '@angular/core';
import { FormsModule } from '@angular/forms';
import { IconComponent } from '../../shared/icon.component';
import { ToastService } from '../../shared/toast.service';
import { SettingsService, Env } from '../../core/settings.service';
import { DbcaService, DbcaQuery, DbcaResult } from './dbca.service';

const ENVS: readonly Env[] = ['dev', 'homol', 'staging', 'prod'];

/**
 * DBCA — analytics de metadados de banco. As queries (admin) viram botões-ação;
 * o usuário informa recurso + ambiente e vê os dados, sem saber do "como". Tipo
 * de recurso, VPC e credencial são resolvidos no backend automaticamente.
 */
@Component({
  selector: 'app-dbca',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [FormsModule, IconComponent],
  template: `
    <div class="page-head anim-in">
      <div>
        <h1>DBCA · Analytics de banco</h1>
        <p class="muted">
          Rode queries de metadados em qualquer banco por button-actions — o tipo
          de recurso, a VPC e a conexão são descobertos automaticamente.
          @if (demoCatalog()) {
            <span class="badge demo-badge">demo</span>
          }
        </p>
      </div>
    </div>

    <section class="card anim-in" style="animation-delay: 40ms">
      <div class="form-row">
        <div class="field grow">
          <label for="dbca-resource">Recurso</label>
          <input
            id="dbca-resource"
            type="text"
            [(ngModel)]="resource"
            placeholder="cluster Aurora ou tabela DynamoDB (nome/id/ARN)"
            autocomplete="off"
            spellcheck="false"
          />
          <div class="hint">
            Conta <strong>{{ account() }}</strong> (definida em Configurações). Região e tipo são automáticos.
          </div>
        </div>
        <div class="field">
          <label>Ambiente</label>
          <div class="segmented" role="radiogroup" aria-label="Ambiente">
            @for (e of envs; track e) {
              <button
                type="button"
                role="radio"
                [attr.aria-checked]="e === environment()"
                [class.active]="e === environment()"
                (click)="environment.set(e)"
              >
                {{ e }}
              </button>
            }
          </div>
        </div>
      </div>
    </section>

    <section class="card anim-in" style="animation-delay: 90ms">
      <div class="panel-head">
        <h3>Ações de análise</h3>
        <span class="muted">clique para executar no recurso informado</span>
      </div>

      @if (catalogLoading()) {
        <div class="empty-state" role="status">
          <span class="spin" aria-hidden="true"></span>
          <p class="muted">Carregando queries…</p>
        </div>
      } @else {
        <div class="query-grid">
          @for (q of queries(); track q.id) {
            <button
              type="button"
              class="query-card"
              [disabled]="!!running()"
              [attr.aria-label]="'Executar: ' + q.label"
              (click)="run(q)"
            >
              <div class="qc-top">
                <span class="qc-cat">{{ q.category }}</span>
                @if (running() === q.id) {
                  <span class="spin" aria-hidden="true"></span>
                } @else {
                  <app-icon name="bolt" [size]="15" class="qc-run" />
                }
              </div>
              <strong class="qc-label">{{ q.label }}</strong>
              <span class="muted qc-desc">{{ q.description }}</span>
              <div class="qc-engines">
                @for (eng of q.engines; track eng) {
                  <span class="qc-eng">{{ eng }}</span>
                }
              </div>
            </button>
          }
        </div>
      }
    </section>

    @if (error(); as e) {
      <section class="card err-card anim-in" role="alert">
        <div class="panel-head"><h3>Não foi possível executar</h3></div>
        <p class="muted">{{ e }}</p>
      </section>
    }

    @if (result(); as r) {
      <section class="card anim-in" aria-live="polite">
        <div class="panel-head">
          <h3>{{ r.label }}</h3>
          <span class="muted">{{ r.rowCount }} linha(s)</span>
        </div>

        <div class="meta-chips">
          <span class="badge" [attr.data-type]="r.resourceType">{{ r.resourceType }}</span>
          <span class="badge">{{ r.engine }}</span>
          @if (r.vpcId) { <span class="badge">VPC {{ r.vpcId }}</span> }
          <span class="badge">{{ r.region }}</span>
          @if (r.demo) { <span class="badge demo-badge">demo</span> }
        </div>

        @if (r.rows.length) {
          <div class="table-wrap">
            <table class="result-table">
              <thead>
                <tr>
                  @for (c of r.columns; track c) {
                    <th>{{ c }}</th>
                  }
                </tr>
              </thead>
              <tbody>
                @for (row of r.rows; track $index) {
                  <tr>
                    @for (cell of row; track $index) {
                      <td>{{ cell }}</td>
                    }
                  </tr>
                }
              </tbody>
            </table>
          </div>
        } @else {
          <div class="empty-state" role="status">
            <app-icon name="database" [size]="24" class="muted" />
            <p class="muted">A query não retornou linhas.</p>
          </div>
        }
      </section>
    }
  `,
  styles: [
    `
      .form-row {
        display: flex;
        flex-wrap: wrap;
        gap: 1rem 1.25rem;
        align-items: flex-start;
      }
      .field.grow {
        flex: 1 1 320px;
        margin-bottom: 0;
      }
      .field {
        margin-bottom: 0;
      }
      .demo-badge {
        margin-left: 0.4rem;
        color: var(--warn);
        background: var(--soft-warn);
      }
      .query-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
        gap: 0.85rem;
      }
      .query-card {
        display: flex;
        flex-direction: column;
        gap: 0.35rem;
        text-align: left;
        padding: 0.95rem 1rem;
        border: 1px solid var(--border);
        border-radius: var(--radius);
        background: var(--panel-2);
        color: var(--text);
        cursor: pointer;
        transition:
          transform 0.16s ease,
          border-color 0.16s ease,
          background-color 0.16s ease;
      }
      .query-card:hover:not(:disabled) {
        transform: translateY(-2px);
        border-color: color-mix(in srgb, var(--accent) 45%, var(--border));
        background: var(--hover);
      }
      .query-card:disabled {
        opacity: 0.6;
        cursor: default;
      }
      .qc-top {
        display: flex;
        align-items: center;
        justify-content: space-between;
      }
      .qc-cat {
        font-size: 0.68rem;
        font-weight: 700;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: var(--faint);
      }
      .qc-run {
        color: var(--accent);
      }
      .qc-label {
        font-size: 0.98rem;
      }
      .qc-desc {
        font-size: 0.8rem;
        line-height: 1.35;
      }
      .qc-engines {
        display: flex;
        flex-wrap: wrap;
        gap: 0.3rem;
        margin-top: 0.25rem;
      }
      .qc-eng {
        font-size: 0.68rem;
        font-family: var(--font-mono);
        color: var(--muted);
        background: var(--panel-3);
        border-radius: 6px;
        padding: 0.1rem 0.35rem;
      }
      .meta-chips {
        display: flex;
        flex-wrap: wrap;
        gap: 0.4rem;
        margin-bottom: 0.9rem;
      }
      .badge[data-type='aurora'] {
        color: var(--accent);
        background: var(--soft-accent);
      }
      .badge[data-type='dynamodb'] {
        color: var(--accent-3);
        background: color-mix(in srgb, var(--accent-3) 15%, transparent);
      }
      .table-wrap {
        overflow-x: auto;
        border: 1px solid var(--border);
        border-radius: var(--radius);
      }
      .result-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 0.85rem;
      }
      .result-table th,
      .result-table td {
        text-align: left;
        padding: 0.55rem 0.8rem;
        border-bottom: 1px solid var(--border);
        white-space: nowrap;
      }
      .result-table th {
        font-size: 0.72rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        color: var(--muted);
        background: var(--panel-2);
        position: sticky;
        top: 0;
      }
      .result-table td {
        font-family: var(--font-mono);
        font-variant-numeric: tabular-nums;
      }
      .result-table tbody tr:hover {
        background: var(--hover);
      }
      .result-table tbody tr:last-child td {
        border-bottom: none;
      }
      .err-card {
        border-left: 3px solid var(--danger);
      }
      .empty-state {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 0.5rem;
        padding: 2rem 1rem;
        text-align: center;
      }
      .empty-state p {
        margin: 0;
      }
      .spin {
        display: inline-block;
        width: 14px;
        height: 14px;
        border: 2px solid color-mix(in srgb, var(--accent) 35%, transparent);
        border-top-color: var(--accent);
        border-radius: 50%;
        animation: dbca-spin 0.7s linear infinite;
      }
      @keyframes dbca-spin {
        to {
          transform: rotate(360deg);
        }
      }
      @media (prefers-reduced-motion: reduce) {
        .spin {
          animation: none;
        }
      }
    `,
  ],
})
export class DbcaComponent {
  private readonly dbca = inject(DbcaService);
  private readonly toast = inject(ToastService);
  private readonly settings = inject(SettingsService);

  readonly envs = ENVS;
  readonly resource = signal('');
  readonly environment = signal<Env>(this.settings.activeEnv());
  readonly account = computed(() => this.settings.awsEnvelope().account);

  readonly queries = signal<DbcaQuery[]>([]);
  readonly catalogLoading = signal(true);
  readonly demoCatalog = signal(false);
  readonly running = signal<string | null>(null);
  readonly result = signal<DbcaResult | null>(null);
  readonly error = signal<string | null>(null);

  readonly canRun = computed(() => this.resource().trim().length > 0);

  constructor() {
    void this.loadCatalog();
  }

  private async loadCatalog(): Promise<void> {
    const { queries, demo } = await this.dbca.queries();
    this.queries.set(queries);
    this.demoCatalog.set(demo);
    this.catalogLoading.set(false);
  }

  async run(q: DbcaQuery): Promise<void> {
    if (!this.canRun()) {
      this.toast.warn('Informe o recurso', 'Digite o cluster ou tabela alvo antes de executar.');
      return;
    }
    if (this.running()) return;
    this.running.set(q.id);
    this.error.set(null);
    try {
      const r = await this.dbca.run(q.id, this.resource().trim(), this.environment());
      this.result.set(r);
      this.toast.success(q.label, `${r.rowCount} linha(s) · ${r.engine}`);
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Erro inesperado.';
      this.error.set(message);
      this.result.set(null);
      this.toast.error('Falha na análise', message);
    } finally {
      this.running.set(null);
    }
  }
}
