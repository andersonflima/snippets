import {
  afterNextRender,
  ChangeDetectionStrategy,
  Component,
  computed,
  inject,
  signal,
} from '@angular/core';
import { Router, RouterLink } from '@angular/router';
import { RegistryService } from '../core/registry.service';
import { Contract } from '../core/registry.service';
import { Integration } from '../core/openapi-parser';
import { JsonSchema, schemaType } from '../core/json-schema';
import { ToastService } from '../shared/toast.service';
import { SkeletonComponent } from '../shared/skeleton.component';
import { IconComponent } from '../shared/icon.component';
import { ModalComponent } from '../shared/modal.component';

interface ContractGroup {
  contract: Contract;
  integrations: Integration[];
}

/** A single request-body field surfaced in the preview modal. */
interface PreviewParam {
  name: string;
  type: string;
  required: boolean;
  description?: string;
}

/** Lists all integrations grouped by contract; each row links to its run page. */
@Component({
  selector: 'app-integrations',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterLink, SkeletonComponent, IconComponent, ModalComponent],
  template: `
    <div class="page-head anim-in">
      <div>
        <h1>Integrações</h1>
        <p class="muted">
          Ações geradas dinamicamente a partir dos contratos OpenAPI registrados.
        </p>
      </div>
      @if (!loading() && groups().length > 0) {
        <div class="int-summary-badges">
          <span class="badge">{{ groups().length }} contrato(s)</span>
          <span class="badge">{{ totalIntegrations() }} ação(ões)</span>
        </div>
      }
    </div>

    @if (loading()) {
      @for (s of [0, 1, 2]; track s) {
        <div class="card">
          <app-skeleton height="120px" />
        </div>
      }
    } @else if (groups().length === 0) {
      <div class="card int-empty">
        <app-icon name="layers" [size]="28" class="muted" />
        <p class="muted">
          Nenhum contrato registrado. Vá para <a routerLink="/admin">Admin</a>
          para carregar um arquivo OpenAPI.
        </p>
      </div>
    } @else {
      @for (group of groups(); track group.contract.id; let i = $index) {
        <div class="card int-card anim-in" [style.animation-delay.ms]="i * 60">
          <div class="int-head">
            <div class="int-head-main">
              <span class="int-head-ic"><app-icon name="layers" [size]="16" /></span>
              <div class="int-head-text">
                <h2 class="group-title">{{ group.contract.title }}</h2>
                <span class="path int-head-id">{{ group.contract.id }}</span>
              </div>
            </div>
            <span class="badge">{{ group.integrations.length }} op.</span>
          </div>

          <div class="int-list">
            @for (it of group.integrations; track it.id) {
              <a class="int-row" [routerLink]="['/run', it.contractId, it.id]">
                <span class="method verb" [attr.data-verb]="it.method">{{ it.method }}</span>
                <span class="int-row-info">
                  <span class="int-name">{{ it.name }}</span>
                  @if (it.summary) {
                    <span class="muted int-summary">{{ it.summary }}</span>
                  }
                </span>
                <span class="int-meta">
                  <span class="path int-path">{{ it.path }}</span>
                  <button
                    type="button"
                    class="int-preview-btn"
                    aria-label="Pré-visualizar ação"
                    title="Pré-visualizar"
                    (click)="openPreview($event, it)"
                  >
                    <app-icon name="expand" [size]="14" />
                  </button>
                </span>
              </a>
            }
          </div>
        </div>
      }
    }

    <app-modal
      [open]="!!preview()"
      [title]="preview()?.name ?? ''"
      [subtitle]="preview()?.summary ?? 'Detalhe da operação'"
      (close)="closePreview()"
    >
      @if (preview(); as it) {
        <div class="int-preview">
          <div class="ip-summary">
            <span class="method verb" [attr.data-verb]="it.method">{{ it.method }}</span>
            <code class="ip-path">{{ it.path }}</code>
          </div>

          <dl class="ip-meta">
            <div class="ip-meta-row">
              <dt>Contrato</dt>
              <dd>{{ it.contractId }}</dd>
            </div>
            <div class="ip-meta-row">
              <dt>Operação</dt>
              <dd>{{ it.id }}</dd>
            </div>
            @if (it.summary) {
              <div class="ip-meta-row">
                <dt>Resumo</dt>
                <dd>{{ it.summary }}</dd>
              </div>
            }
          </dl>

          <div class="ip-section">
            <div class="ip-section-head">
              <h4>Parâmetros do corpo</h4>
              <span class="badge">{{ previewParams().length }}</span>
            </div>
            @if (previewParams().length > 0) {
              <div class="ip-params">
                @for (p of previewParams(); track p.name) {
                  <div class="ip-param">
                    <div class="ip-param-key">
                      <code>{{ p.name }}</code>
                      @if (p.required) {
                        <span class="ip-req">obrigatório</span>
                      }
                    </div>
                    <span class="ip-type">{{ p.type }}</span>
                    @if (p.description) {
                      <span class="muted ip-param-desc">{{ p.description }}</span>
                    }
                  </div>
                }
              </div>
            } @else {
              <p class="muted">Esta ação não possui parâmetros de corpo.</p>
            }
          </div>

          <div class="ip-actions">
            <button type="button" class="primary ip-run" (click)="runPreview(it)">
              <app-icon name="bolt" [size]="15" />
              Executar ação
            </button>
          </div>
        </div>
      }
    </app-modal>
  `,
  styles: [
    `
      .int-summary-badges {
        display: flex;
        align-items: center;
        gap: 8px;
        flex-wrap: wrap;
      }
      /* Contract card header: icon + title/id cluster, count badge, divider. */
      .int-head {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        padding-bottom: 12px;
        margin-bottom: 14px;
        border-bottom: 1px solid var(--border);
      }
      .int-head-main {
        display: flex;
        align-items: center;
        gap: 10px;
        min-width: 0;
      }
      .int-head-ic {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        flex: 0 0 auto;
        width: 32px;
        height: 32px;
        border-radius: var(--radius);
        color: var(--accent);
        background: color-mix(in srgb, var(--accent) 14%, transparent);
      }
      .int-head-text {
        display: flex;
        flex-direction: column;
        gap: 1px;
        min-width: 0;
      }
      .group-title {
        margin: 0;
        font-size: 1.02rem;
        line-height: 1.3;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }
      .int-head-id {
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }
      .int-list {
        display: flex;
        flex-direction: column;
        gap: 7px;
      }
      .int-row {
        display: flex;
        align-items: center;
        gap: 14px;
        padding: 11px 14px;
        border: 1px solid var(--border);
        border-radius: var(--radius);
        background: var(--panel-2);
        text-decoration: none;
        color: inherit;
        transition:
          background 0.15s ease,
          border-color 0.15s ease,
          transform 0.15s ease;
      }
      .int-row:hover {
        background: var(--hover);
        border-color: var(--accent);
        transform: translateX(2px);
      }
      .int-row:focus-visible {
        outline: 2px solid var(--accent);
        outline-offset: 2px;
      }
      .int-row-info {
        display: flex;
        flex-direction: column;
        gap: 2px;
        min-width: 0;
        flex: 1 1 auto;
      }
      .int-name {
        font-weight: 600;
        color: var(--accent);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }
      .int-summary {
        font-size: 0.85em;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }
      .int-meta {
        display: flex;
        align-items: center;
        gap: 10px;
        flex-shrink: 0;
      }
      .int-path {
        max-width: 320px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }
      /* Verb chip: keeps global .method typography, recolors per HTTP verb. */
      .verb {
        flex: 0 0 auto;
        min-width: 3.4rem;
        text-align: center;
        letter-spacing: 0.02em;
      }
      .verb[data-verb='GET'] {
        background: color-mix(in srgb, var(--ok) 82%, transparent);
      }
      .verb[data-verb='POST'] {
        background: color-mix(in srgb, var(--accent) 82%, transparent);
      }
      .verb[data-verb='PUT'] {
        background: color-mix(in srgb, var(--warn) 82%, transparent);
      }
      .verb[data-verb='PATCH'] {
        background: color-mix(in srgb, var(--warn) 60%, var(--accent));
      }
      .verb[data-verb='DELETE'] {
        background: color-mix(in srgb, var(--danger) 82%, transparent);
      }
      .int-empty {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 12px;
        text-align: center;
        padding: 32px 16px;
      }
      @media (max-width: 640px) {
        .int-row {
          flex-wrap: wrap;
          gap: 8px 12px;
        }
        .int-row-info {
          flex-basis: 100%;
          order: 2;
        }
        .int-meta {
          order: 3;
          width: 100%;
          justify-content: space-between;
        }
        .int-path {
          max-width: none;
          flex: 1 1 auto;
        }
      }
      .int-preview-btn {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 28px;
        height: 28px;
        padding: 0;
        color: var(--muted);
        background: transparent;
        border: 1px solid transparent;
        border-radius: var(--radius);
        cursor: pointer;
        transition:
          color 0.12s ease,
          background 0.12s ease,
          border-color 0.12s ease;
      }
      .int-preview-btn:hover {
        color: var(--accent);
        background: var(--hover);
        border-color: var(--border);
      }
      .int-preview-btn:focus-visible {
        outline: 2px solid var(--accent);
        outline-offset: 2px;
      }
      .int-preview {
        display: flex;
        flex-direction: column;
        gap: 18px;
      }
      .ip-summary {
        display: flex;
        align-items: center;
        gap: 10px;
        flex-wrap: wrap;
      }
      .ip-path {
        font-size: 0.9em;
        word-break: break-all;
      }
      .ip-meta {
        display: flex;
        flex-direction: column;
        gap: 8px;
        margin: 0;
      }
      .ip-meta-row {
        display: grid;
        grid-template-columns: 120px 1fr;
        gap: 12px;
        align-items: baseline;
      }
      .ip-meta-row dt {
        color: var(--muted);
        font-size: 0.85em;
      }
      .ip-meta-row dd {
        margin: 0;
        min-width: 0;
        word-break: break-word;
      }
      .ip-section-head {
        display: flex;
        align-items: center;
        gap: 8px;
        margin-bottom: 8px;
      }
      .ip-section-head h4 {
        margin: 0;
        font-size: 0.95em;
      }
      .ip-params {
        display: flex;
        flex-direction: column;
        gap: 6px;
      }
      .ip-param {
        display: flex;
        align-items: baseline;
        flex-wrap: wrap;
        gap: 8px;
        padding: 8px 10px;
        border: 1px solid var(--border);
        border-radius: var(--radius);
        background: var(--panel-2);
      }
      .ip-param-key {
        display: inline-flex;
        align-items: center;
        gap: 6px;
      }
      .ip-req {
        font-size: 0.72em;
        text-transform: uppercase;
        letter-spacing: 0.03em;
        color: var(--accent);
      }
      .ip-type {
        font-size: 0.8em;
        color: var(--muted);
        font-family: var(--font-mono, monospace);
      }
      .ip-param-desc {
        flex-basis: 100%;
        font-size: 0.85em;
      }
      .ip-actions {
        display: flex;
        justify-content: flex-end;
        gap: 8px;
      }
      .ip-run {
        display: inline-flex;
        align-items: center;
        gap: 6px;
      }
    `,
  ],
})
export class IntegrationsComponent {
  private readonly registry = inject(RegistryService);
  private readonly toast = inject(ToastService);
  private readonly router = inject(Router);

  readonly loading = signal(true);

  /** Integration currently shown in the preview modal (null while closed). */
  readonly preview = signal<Integration | null>(null);

  /** Flattened request-body fields of the previewed integration. */
  readonly previewParams = computed<PreviewParam[]>(() => {
    const it = this.preview();
    if (!it) {
      return [];
    }
    return toPreviewParams(it.requestSchema);
  });

  readonly groups = computed<ContractGroup[]>(() =>
    this.registry.contracts().map((contract) => ({
      contract,
      integrations: this.registry.integrationsByContract(contract.id),
    })),
  );

  readonly totalIntegrations = computed(() =>
    this.groups().reduce((sum, g) => sum + g.integrations.length, 0),
  );

  /** Open the preview modal without triggering the row's run navigation. */
  openPreview(event: Event, it: Integration): void {
    event.stopPropagation();
    event.preventDefault();
    this.preview.set(it);
  }

  closePreview(): void {
    this.preview.set(null);
  }

  /** Navigate to the run page for the previewed integration, then close. */
  runPreview(it: Integration): void {
    this.closePreview();
    void this.router.navigate(['/run', it.contractId, it.id]);
  }

  constructor() {
    afterNextRender(() => {
      setTimeout(() => {
        this.loading.set(false);
        const n = this.totalIntegrations();
        this.toast.info(
          'Integrações carregadas',
          n + ' ações disponíveis em ' + this.groups().length + ' contrato(s).',
        );
      }, 500);
    });
  }
}

/** Flatten a request-body schema's top-level properties into preview rows. */
function toPreviewParams(schema: JsonSchema): PreviewParam[] {
  const properties = schema.properties ?? {};
  const required = new Set(schema.required ?? []);
  return Object.entries(properties).map(([name, prop]) => ({
    name,
    type: describeType(prop),
    required: required.has(name),
    description: prop.description,
  }));
}

/** Human-readable type label for a schema node (e.g. `array<string>`). */
function describeType(schema: JsonSchema): string {
  const base = schemaType(schema) ?? 'any';
  if (base === 'array' && schema.items) {
    return `array<${schemaType(schema.items) ?? 'any'}>`;
  }
  if (schema.enum && schema.enum.length > 0) {
    return `enum(${schema.enum.join(' | ')})`;
  }
  return base;
}
