import { Injectable, inject } from '@angular/core';
import { ApiClientService } from '../../core/api-client.service';
import { SettingsService } from '../../core/settings.service';
import { JsonValue } from '../../core/json-schema';

/** ServiceNow operations the GMUD action exposes. `status`/`validate` are read-only. */
export type GmudOperation = 'status' | 'validate';

/** Identifiers used to key the check map returned by the ServiceNow action. */
export type GmudCheckKey = 'state' | 'window' | 'approval' | 'conflict' | 'tasks';

/** A single validation check: whether it passed, its observed value and if required. */
export interface GmudCheck {
  ok: boolean;
  value: unknown;
  required: boolean;
}

/** `detail` block of a successful `/servicenow/execute` response (status/validate). */
export interface GmudDetail {
  operation: GmudOperation;
  change: string;
  state: string;
  withinWindow: boolean;
  approval: string;
  conflictStatus: string;
  taskCount: number;
  checks: Record<GmudCheckKey, GmudCheck>;
  reasons: string[];
  allowed: boolean;
}

/**
 * Talks to the `servicenow` microservice through the BFF to monitor a GMUD.
 * Wraps the active AWS envelope around a small `params` block (operation +
 * changeNumber) and unwraps `body.detail`, throwing a readable {@link Error} on
 * any non-ok response so the screen renders a single failure path.
 */
@Injectable({ providedIn: 'root' })
export class GmudService {
  private readonly api = inject(ApiClientService);
  private readonly settings = inject(SettingsService);

  /** Full read-only snapshot of a change for monitoring (never gates). */
  status(changeNumber: string): Promise<GmudDetail> {
    return this.execute('status', changeNumber);
  }

  /** Same assessment as {@link status}, tagged as a validate run. */
  validate(changeNumber: string): Promise<GmudDetail> {
    return this.execute('validate', changeNumber);
  }

  /** Build the envelope, POST it, and unwrap `body.detail` (throwing on failure). */
  private async execute(operation: GmudOperation, rawNumber: string): Promise<GmudDetail> {
    const changeNumber = rawNumber.trim();
    if (!changeNumber) {
      throw new Error('Informe o número da GMUD antes de verificar.');
    }

    const envelope = this.settings.awsEnvelope();
    const payload = {
      ...envelope,
      // The ServiceNow contract requires a `resource`; the change number is a
      // sensible, valid placeholder when the profile has no resource of its own.
      resource: changeNumber,
      changeNumber,
      dryRun: true,
      params: { operation, changeNumber },
    } as unknown as JsonValue;

    const res = await this.api.runAction('/servicenow/execute', payload);
    if (!res.ok) {
      throw new Error(this.errorMessage(res.body, res.status, res.statusText));
    }

    const detail = (res.body as { detail?: GmudDetail } | null)?.detail;
    if (detail == null) {
      throw new Error('Resposta inválida do serviço ServiceNow: campo "detail" ausente.');
    }
    return detail;
  }

  /** Extract the most descriptive message available from an error body. */
  private errorMessage(body: unknown, status: number, statusText: string): string {
    if (typeof body === 'string' && body.trim()) {
      return body;
    }
    if (body && typeof body === 'object') {
      const record = body as Record<string, unknown>;
      const candidate = record['message'] ?? record['error'] ?? record['detail'];
      if (typeof candidate === 'string' && candidate.trim()) {
        return candidate;
      }
      try {
        return JSON.stringify(body);
      } catch {
        /* fall through to the status line */
      }
    }
    return `${status} ${statusText}`.trim() || 'Falha ao consultar ServiceNow.';
  }
}
