import { ChangeDetectionStrategy, Component, inject, signal } from '@angular/core';
import { RegistryService, Contract } from '../core/registry.service';
import { parseContract, extractIntegrations } from '../core/openapi-parser';
import { IconComponent } from '../shared/icon.component';
import { ToastService } from '../shared/toast.service';

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
  imports: [IconComponent],
  template: `
    <div class="container">
      <div class="page-head anim-in">
        <div>
          <h1>Admin de contratos</h1>
          <p class="muted">
            Carregue o OpenAPI do API Gateway (JSON ou YAML). As ações são
            detectadas automaticamente e cada uma vira um formulário dinâmico.
          </p>
        </div>
        <button type="button" (click)="exportRegistry()" [disabled]="contracts().length === 0">
          <app-icon name="expand" [size]="15" /> Exportar registry.json
        </button>
      </div>

      <section class="card anim-in" style="animation-delay: 40ms">
        <div class="settings-head">
          <app-icon name="layers" [size]="18" class="muted" />
          <h3>Carregar contrato</h3>
        </div>

        <label class="dropzone" for="admin-file">
          <app-icon name="database" [size]="22" class="muted" />
          <span class="dz-title">Selecionar arquivo OpenAPI</span>
          <span class="hint">.json · .yaml · .yml</span>
          <input
            id="admin-file"
            type="file"
            accept=".json,.yaml,.yml"
            (change)="onFile($event)"
            aria-label="Selecionar arquivo OpenAPI JSON ou YAML"
          />
        </label>

        @if (error(); as e) {
          <div class="error" role="alert" aria-live="assertive">{{ e }}</div>
        }

        @if (preview(); as p) {
          <div class="card preview-card" style="margin-top: 0.9rem">
            <div class="preview-top">
              <app-icon name="check" [size]="16" class="ok-ico" />
              <strong>{{ p.title }}</strong>
            </div>
            <div class="muted path">id: {{ p.id }}</div>
            <div class="muted">
              {{ p.integrationCount }} integração(ões) detectada(s)
            </div>
            <div class="toolbar" style="margin-top: 0.7rem">
              <button class="primary" type="button" (click)="add()">
                <app-icon name="check" [size]="15" /> Adicionar
              </button>
              <button type="button" (click)="clearPreview()">Cancelar</button>
            </div>
          </div>
        }
      </section>

      <section class="card anim-in" style="animation-delay: 90ms">
        <div class="settings-head">
          <app-icon name="server" [size]="18" class="muted" />
          <h3>Contratos registrados</h3>
          <span class="badge count-badge">{{ contracts().length }}</span>
        </div>

        @if (contracts().length === 0) {
          <div class="empty-state" role="status">
            <app-icon name="database" [size]="26" class="muted" />
            <p class="muted">Nenhum contrato registrado ainda.</p>
            <span class="hint">Carregue um OpenAPI acima para começar.</span>
          </div>
        } @else {
          @for (c of contracts(); track c.id; let i = $index) {
            <div class="row anim-in" [style.animation-delay.ms]="i * 40">
              <div>
                <strong>{{ c.title }}</strong>
                <div class="muted path">
                  {{ c.id }} · {{ countIntegrations(c) }} integração(ões) ·
                  adicionado {{ c.addedAt }}
                </div>
              </div>
              <button
                class="danger"
                type="button"
                (click)="remove(c.id, c.title)"
                [attr.aria-label]="'Remover contrato ' + c.title"
              >
                Remover
              </button>
            </div>
          }
        }
      </section>

      <div class="note anim-in" style="animation-delay: 140ms">
        O navegador não grava no repositório. Use <strong>Exportar
        registry.json</strong> e faça commit do arquivo baixado em
        <code>src/assets/registry.json</code> para versionar os contratos.
      </div>
    </div>
  `,
  styles: [
    `
      .settings-head {
        display: flex;
        align-items: center;
        gap: 0.55rem;
        margin-bottom: 0.9rem;
      }
      .settings-head h3 {
        margin: 0;
        font-size: 1.05rem;
      }
      .count-badge {
        margin-left: auto;
      }
      button app-icon {
        vertical-align: -2px;
      }
      .dropzone {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 0.35rem;
        padding: 1.6rem 1rem;
        border: 1.5px dashed var(--border-strong);
        border-radius: var(--radius);
        background: var(--panel-2);
        cursor: pointer;
        text-align: center;
        transition:
          border-color 0.18s ease,
          background-color 0.18s ease;
      }
      .dropzone:hover {
        border-color: var(--accent);
        background: var(--hover);
      }
      .dropzone .dz-title {
        font-weight: 600;
        color: var(--text);
      }
      .dropzone input[type='file'] {
        display: none;
      }
      .preview-card {
        border-color: color-mix(in srgb, var(--ok) 45%, var(--border));
        background: var(--soft-ok);
      }
      .preview-top {
        display: flex;
        align-items: center;
        gap: 0.4rem;
      }
      .ok-ico {
        color: var(--ok);
      }
      .empty-state {
        display: flex;
        flex-direction: column;
        align-items: center;
        gap: 0.4rem;
        padding: 2rem 1rem;
        text-align: center;
      }
      .empty-state p {
        margin: 0.2rem 0 0;
      }
    `,
  ],
})
export class AdminComponent {
  private readonly registry = inject(RegistryService);
  private readonly toast = inject(ToastService);

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
    this.toast.success('Contrato adicionado', `${p.title} · ${p.integrationCount} integração(ões)`);
  }

  clearPreview(): void {
    this.preview.set(null);
    this.error.set(null);
  }

  remove(id: string, title: string): void {
    this.registry.removeContract(id);
    this.toast.info('Contrato removido', title);
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
    this.toast.success('registry.json exportado', `${this.contracts().length} contrato(s)`);
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
