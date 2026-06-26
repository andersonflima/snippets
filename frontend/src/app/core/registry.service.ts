import { Injectable, computed, inject, signal } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';
import { Integration, OpenApiDoc, extractIntegrations } from './openapi-parser';

/** One uploaded/registered OpenAPI contract. */
export interface Contract {
  id: string;
  title: string;
  addedAt: string;
  openapi: OpenApiDoc;
}

export interface Registry {
  contracts: Contract[];
}

const OVERRIDES_KEY = 'msc.registryOverrides';

/**
 * Source of truth for registered contracts.
 *
 * Boot order: load the versioned seed (`assets/registry.json`), then merge
 * localStorage overrides (working copy) over it. Overrides win by id. The Admin
 * area mutates only the override copy; "Export registry.json" downloads the full
 * merged registry so it can be committed back into the repo seed.
 */
@Injectable({ providedIn: 'root' })
export class RegistryService {
  private readonly http = inject(HttpClient);

  private readonly seed = signal<Contract[]>([]);
  private readonly overrides = signal<Contract[]>(this.readOverrides());
  private readonly loadedSig = signal(false);

  readonly loaded = this.loadedSig.asReadonly();

  /** Merged contracts: seed overlaid by localStorage overrides (override wins by id). */
  readonly contracts = computed<Contract[]>(() => {
    const byId = new Map<string, Contract>();
    for (const c of this.seed()) {
      byId.set(c.id, c);
    }
    for (const c of this.overrides()) {
      byId.set(c.id, c);
    }
    return [...byId.values()];
  });

  /** Flattened list of all runnable integrations across all contracts. */
  readonly integrations = computed<Integration[]>(() =>
    this.contracts().flatMap((c) => extractIntegrations(c.id, c.openapi)),
  );

  /** Load the seed file once. Safe to call repeatedly. */
  async load(): Promise<void> {
    if (this.loadedSig()) {
      return;
    }
    try {
      const reg = await firstValueFrom(
        this.http.get<Registry>('assets/registry.json'),
      );
      this.seed.set(reg?.contracts ?? []);
    } catch {
      // Missing/invalid seed is non-fatal: overrides may still provide contracts.
      this.seed.set([]);
    } finally {
      this.loadedSig.set(true);
    }
  }

  findContract(id: string): Contract | undefined {
    return this.contracts().find((c) => c.id === id);
  }

  findIntegration(contractId: string, opId: string): Integration | undefined {
    return this.integrations().find(
      (i) => i.contractId === contractId && i.id === opId,
    );
  }

  integrationsByContract(contractId: string): Integration[] {
    const contract = this.findContract(contractId);
    return contract ? extractIntegrations(contract.id, contract.openapi) : [];
  }

  /** Add or replace a contract in the working override copy. */
  addContract(contract: Contract): void {
    const next = this.overrides().filter((c) => c.id !== contract.id);
    next.push(contract);
    this.overrides.set(next);
    this.persistOverrides();
  }

  /** Remove a contract from the working override copy (and hide the seed entry). */
  removeContract(id: string): void {
    // Drop from overrides and also from seed so a seeded contract can be removed
    // from the working set; export then reflects the removal.
    this.overrides.set(this.overrides().filter((c) => c.id !== id));
    this.seed.set(this.seed().filter((c) => c.id !== id));
    this.persistOverrides();
  }

  /** Serialize the full merged registry for download/commit. */
  exportJson(): string {
    const registry: Registry = { contracts: this.contracts() };
    return JSON.stringify(registry, null, 2);
  }

  private readOverrides(): Contract[] {
    try {
      const raw = localStorage.getItem(OVERRIDES_KEY);
      if (!raw) {
        return [];
      }
      const parsed = JSON.parse(raw) as Registry;
      return parsed?.contracts ?? [];
    } catch {
      return [];
    }
  }

  private persistOverrides(): void {
    try {
      localStorage.setItem(
        OVERRIDES_KEY,
        JSON.stringify({ contracts: this.overrides() }),
      );
    } catch {
      // Ignore persistence failures; in-memory state remains authoritative.
    }
  }
}
