import {
  afterNextRender,
  ChangeDetectionStrategy,
  Component,
  computed,
  inject,
  signal,
} from '@angular/core';
import { RouterLink } from '@angular/router';
import { RegistryService } from '../core/registry.service';
import { Contract } from '../core/registry.service';
import { Integration } from '../core/openapi-parser';
import { ToastService } from '../shared/toast.service';
import { SkeletonComponent } from '../shared/skeleton.component';
import { IconComponent } from '../shared/icon.component';

interface ContractGroup {
  contract: Contract;
  integrations: Integration[];
}

/** Lists all integrations grouped by contract; each row links to its run page. */
@Component({
  selector: 'app-integrations',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterLink, SkeletonComponent, IconComponent],
  template: `
    <div class="page-head anim-in">
      <div>
        <h1>Integrações</h1>
        <p class="muted">
          Ações geradas dinamicamente a partir dos contratos OpenAPI registrados.
        </p>
      </div>
      @if (!loading() && groups().length > 0) {
        <span class="badge">{{ totalIntegrations() }} ação(ões)</span>
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
        <div class="card anim-in" [style.animation-delay.ms]="i * 60">
          <div class="int-head">
            <div class="int-head-main">
              <h2 class="group-title">{{ group.contract.title }}</h2>
              <span class="path">{{ group.contract.id }}</span>
            </div>
            <span class="badge">{{ group.integrations.length }}</span>
          </div>

          <div class="int-list">
            @for (it of group.integrations; track it.id) {
              <a class="int-row" [routerLink]="['/run', it.contractId, it.id]">
                <span class="int-row-info">
                  <span class="int-name">{{ it.name }}</span>
                  @if (it.summary) {
                    <span class="muted int-summary">{{ it.summary }}</span>
                  }
                </span>
                <span class="int-meta">
                  <span class="method">{{ it.method }}</span>
                  <span class="path int-path">{{ it.path }}</span>
                </span>
              </a>
            }
          </div>
        </div>
      }
    }
  `,
  styles: [
    `
      .int-head {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
        margin-bottom: 12px;
      }
      .int-head-main {
        display: flex;
        flex-direction: column;
        gap: 2px;
        min-width: 0;
      }
      .int-list {
        display: flex;
        flex-direction: column;
        gap: 6px;
      }
      .int-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 16px;
        padding: 10px 12px;
        border: 1px solid var(--border);
        border-radius: var(--radius);
        background: var(--panel-2);
        text-decoration: none;
        color: inherit;
        transition:
          background 0.15s ease,
          border-color 0.15s ease;
      }
      .int-row:hover {
        background: var(--hover);
        border-color: var(--accent);
      }
      .int-row-info {
        display: flex;
        flex-direction: column;
        gap: 2px;
        min-width: 0;
      }
      .int-name {
        font-weight: 600;
        color: var(--accent);
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
        gap: 8px;
        flex-shrink: 0;
      }
      .int-path {
        max-width: 320px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }
      .int-empty {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 12px;
        text-align: center;
        padding: 32px 16px;
      }
    `,
  ],
})
export class IntegrationsComponent {
  private readonly registry = inject(RegistryService);
  private readonly toast = inject(ToastService);

  readonly loading = signal(true);

  readonly groups = computed<ContractGroup[]>(() =>
    this.registry.contracts().map((contract) => ({
      contract,
      integrations: this.registry.integrationsByContract(contract.id),
    })),
  );

  readonly totalIntegrations = computed(() =>
    this.groups().reduce((sum, g) => sum + g.integrations.length, 0),
  );

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
