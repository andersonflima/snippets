import { ChangeDetectionStrategy, Component, inject, signal } from '@angular/core';
import { RegistryService, Contract } from '../core/registry.service';
import { parseContract, extractIntegrations } from '../core/openapi-parser';

interface UploadPreview {
  id: string;
  title: string;
  integrationCount: number;
  contract: Contract;
}

/**
 * Admin area: upload an OpenAPI file (JSON/YAML), preview detected integrations,
 * add it to the working registry, remove contracts, and export the merged
 * registry.json for committing back into the repo.
 */
@Component({
  selector: 'app-admin',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <div class="container">
      <h1>Admin</h1>
      <p class="muted">
        Carregue o arquivo OpenAPI do API Gateway (JSON ou YAML). As ações são
        detectadas automaticamente e cada uma vira um formulário dinâmico.
      </p>

      <div class="card">
        <h3>Carregar contrato</h3>
        <div class="field">
          <input
            type="file"
            accept=".json,.yaml,.yml"
            (change)="onFile($event)"
          />
        </div>

        @if (error(); as e) {
          <div class="error">{{ e }}</div>
        }

        @if (preview(); as p) {
          <div class="card">
            <div><strong>{{ p.title }}</strong></div>
            <div class="muted path">id: {{ p.id }}</div>
            <div class="muted">{{ p.integrationCount }} integração(ões) detectada(s)</div>
            <div class="toolbar" style="margin-top: 0.6rem">
              <button class="primary" (click)="add()">Adicionar</button>
              <button (click)="clearPreview()">Cancelar</button>
            </div>
          </div>
        }
      </div>

      <div class="card">
        <div class="toolbar" style="justify-content: space-between">
          <h3 style="margin: 0">Contratos registrados</h3>
          <button (click)="exportRegistry()">Exportar registry.json</button>
        </div>

        @if (contracts().length === 0) {
          <p class="muted">Nenhum contrato registrado.</p>
        }

        @for (c of contracts(); track c.id) {
          <div class="row">
            <div>
              <strong>{{ c.title }}</strong>
              <div class="muted path">
                {{ c.id }} · {{ countIntegrations(c) }} integração(ões) ·
                adicionado {{ c.addedAt }}
              </div>
            </div>
            <button class="danger" (click)="remove(c.id)">remover</button>
          </div>
        }
      </div>

      <div class="note">
        O navegador não grava no repositório. Use <strong>Exportar
        registry.json</strong> e faça commit do arquivo baixado em
        <code>src/assets/registry.json</code> para versionar os contratos.
      </div>
    </div>
  `,
})
export class AdminComponent {
  private readonly registry = inject(RegistryService);

  readonly contracts = this.registry.contracts;
  readonly preview = signal<UploadPreview | null>(null);
  readonly error = signal<string | null>(null);

  countIntegrations(contract: Contract): number {
    return extractIntegrations(contract.id, contract.openapi).length;
  }

  onFile(event: Event): void {
    this.error.set(null);
    this.preview.set(null);
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];
    if (!file) {
      return;
    }
    const reader = new FileReader();
    reader.onload = () => this.handleText(String(reader.result), file.name);
    reader.onerror = () => this.error.set('Falha ao ler o arquivo.');
    reader.readAsText(file);
  }

  private handleText(text: string, fileName: string): void {
    try {
      const doc = parseContract(text);
      const title = doc.info?.title ?? fileName;
      const id = this.slug(title || fileName);
      const integrations = extractIntegrations(id, doc);
      if (integrations.length === 0) {
        this.error.set('Nenhuma integração (path/POST) encontrada no contrato.');
        return;
      }
      this.preview.set({
        id,
        title,
        integrationCount: integrations.length,
        contract: {
          id,
          title,
          addedAt: new Date().toISOString(),
          openapi: doc,
        },
      });
    } catch (err) {
      this.error.set(err instanceof Error ? err.message : 'Erro ao processar contrato.');
    }
  }

  add(): void {
    const p = this.preview();
    if (!p) {
      return;
    }
    this.registry.addContract(p.contract);
    this.preview.set(null);
  }

  clearPreview(): void {
    this.preview.set(null);
    this.error.set(null);
  }

  remove(id: string): void {
    this.registry.removeContract(id);
  }

  exportRegistry(): void {
    const json = this.registry.exportJson();
    const blob = new Blob([json], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = 'registry.json';
    anchor.click();
    URL.revokeObjectURL(url);
  }

  private slug(value: string): string {
    const slug = value
      .toLowerCase()
      .normalize('NFD')
      .replace(/[̀-ͯ]/g, '')
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '')
      .slice(0, 60);
    return slug || 'contract';
  }
}
