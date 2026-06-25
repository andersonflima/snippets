import { ChangeDetectionStrategy, Component, computed, inject } from '@angular/core';
import { RouterLink } from '@angular/router';
import { RegistryService } from '../core/registry.service';
import { Contract } from '../core/registry.service';
import { Integration } from '../core/openapi-parser';

interface ContractGroup {
  contract: Contract;
  integrations: Integration[];
}

/** Lists all integrations grouped by contract; each row links to its run page. */
@Component({
  selector: 'app-integrations',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterLink],
  template: `
    <div class="container">
      <h1>Integrações</h1>
      <p class="muted">
        Ações geradas dinamicamente a partir dos contratos OpenAPI registrados.
      </p>

      @if (groups().length === 0) {
        <div class="card">
          <p class="muted">
            Nenhum contrato registrado. Vá para <a routerLink="/admin">Admin</a>
            para carregar um arquivo OpenAPI.
          </p>
        </div>
      }

      @for (group of groups(); track group.contract.id) {
        <div class="card">
          <h2>{{ group.contract.title }}</h2>
          <div class="muted path">{{ group.contract.id }}</div>
          @for (it of group.integrations; track it.id) {
            <div class="row">
              <div>
                <a [routerLink]="['/run', it.contractId, it.id]">{{ it.name }}</a>
                @if (it.summary) {
                  <div class="muted">{{ it.summary }}</div>
                }
              </div>
              <div class="toolbar">
                <span class="method">{{ it.method }}</span>
                <span class="path">{{ it.path }}</span>
              </div>
            </div>
          }
        </div>
      }
    </div>
  `,
})
export class IntegrationsComponent {
  private readonly registry = inject(RegistryService);

  readonly groups = computed<ContractGroup[]>(() =>
    this.registry.contracts().map((contract) => ({
      contract,
      integrations: this.registry.integrationsByContract(contract.id),
    })),
  );
}
