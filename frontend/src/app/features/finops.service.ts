import { Injectable, inject } from '@angular/core';
import { ApiClientService, ActionResult } from '../core/api-client.service';
import { SettingsService } from '../core/settings.service';

/** FinOps analysis is read-only and always runs in São Paulo. */
export const FINOPS_REGION = 'sa-east-1';

/** Resource scopes the FinOps scan can target. */
export type FinopsScope =
  | 'all'
  | 'rds'
  | 'ec2'
  | 'ebs'
  | 'eip'
  | 'elb'
  | 'snapshots';

/** Severity of an individual cost-saving finding. */
export type FinopsSeverity = 'high' | 'medium' | 'low';

/** Aggregated numbers for the scanned account. */
export interface FinopsSummary {
  estimatedMonthlySavings: number;
  currency: string;
  findingsCount: number;
  byResourceType: Record<string, number>;
}

/** A single cost-saving opportunity detected in the account. */
export interface FinopsFinding {
  resourceType: string;
  resourceId: string;
  issue: string;
  severity: FinopsSeverity;
  recommendation: string;
  estimatedMonthlySavings: number;
  evidence: Record<string, unknown>;
}

/** The `detail` block of a successful FinOps response. */
export interface FinopsDetail {
  region: string;
  scope: FinopsScope;
  summary: FinopsSummary;
  findings: FinopsFinding[];
  notes: string[];
}

/** Full body returned by `POST /api/finops/execute`. */
export interface FinopsResult {
  operationId: string;
  status: string;
  resource: string;
  account: string;
  detail: FinopsDetail;
}

/** Parameters the operator supplies to launch a scan. */
export interface FinopsRequest {
  account: string;
  roleArn: string;
  scope: FinopsScope;
  lookbackDays: number;
}

/** Verdict assigned to a resource after comparing usage vs. what is provisioned. */
export type FinopsVerdict = 'idle' | 'oversized' | 'ok';

/** Headline numbers for the analytical FinOps view. */
export interface FinopsAnalyticsSummary {
  estimatedMonthlySavings: number;
  currency: string;
  idleCount: number;
  oversizedCount: number;
  analyzedCount: number;
}

/** Measured usage of a single resource against its provisioned capacity. */
export interface FinopsUtilizationRow {
  resourceId: string;
  name: string;
  product: string;
  provisioned: Record<string, number>;
  used: {
    cpuPct?: number;
    memoryPct?: number;
    storagePct?: number;
    iopsPct?: number;
  };
  utilizationPct: number;
  verdict: FinopsVerdict;
  monthlySavings: number;
  recommendation: string;
}

/** Potential monthly savings grouped by resource type. */
export interface FinopsSavingsByType {
  type: string;
  savings: number;
}

/** A single month of observed cost and realized/potential savings. */
export interface FinopsSavingsTrendPoint {
  month: string;
  cost: number;
  savings: number;
}

/** The `detail` block returned by the analytical `insights/execute` action. */
export interface FinopsAnalytics {
  summary: FinopsAnalyticsSummary;
  utilization: FinopsUtilizationRow[];
  savingsByType: FinopsSavingsByType[];
  savingsTrend: FinopsSavingsTrendPoint[];
}

/** Scope/period supplied to the analytical rightsizing query. */
export interface FinopsAnalyticsScope {
  product: string;
  lookbackDays: number;
}

/** Wraps {@link ApiClientService} with the FinOps envelope and typed results. */
@Injectable({ providedIn: 'root' })
export class FinopsService {
  private readonly api = inject(ApiClientService);
  private readonly settings = inject(SettingsService);

  /** Build the envelope and POST it to the FinOps microservice via the BFF. */
  async analyze(request: FinopsRequest): Promise<ActionResult> {
    const payload = {
      account: request.account,
      resource: 'all',
      roleArn: request.roleArn,
      region: FINOPS_REGION,
      environment: 'prod',
      dryRun: false,
      params: {
        scope: request.scope,
        lookbackDays: request.lookbackDays,
      },
    };
    return this.api.runAction('/finops/execute', payload);
  }

  /**
   * Rightsizing analytics: compares each resource's real usage against what is
   * provisioned and returns aggregated savings opportunities. Uses the shared
   * AWS envelope from {@link SettingsService}. Throws a friendly error the page
   * can surface without breaking; success returns the typed `detail` payload.
   */
  async analytics(scope: FinopsAnalyticsScope): Promise<FinopsAnalytics> {
    const res = await this.api.runAction('/insights/execute', {
      ...this.settings.awsEnvelope(),
      params: {
        action: 'finops',
        product: 'all',
        filters: {
          product: scope.product,
          lookbackDays: scope.lookbackDays,
        },
      },
    });

    if (!res.ok) {
      throw new Error(this.analyticsErrorMessage(res));
    }

    const detail = (res.body as { detail?: FinopsAnalytics } | null)?.detail;
    if (!detail) {
      throw new Error('A resposta não trouxe dados de análise (detail vazio).');
    }
    return detail;
  }

  private analyticsErrorMessage(res: ActionResult): string {
    const body = res.body;
    if (body && typeof body === 'object' && 'message' in body) {
      const message = (body as { message?: unknown }).message;
      if (typeof message === 'string' && message.trim() !== '') {
        return message;
      }
    }
    if (typeof body === 'string' && body.trim() !== '') {
      return body;
    }
    const status = res.status ? `${res.status} ${res.statusText}` : res.statusText;
    return `Falha ao carregar a análise (${status || 'erro de rede'}).`;
  }
}
