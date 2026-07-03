import { Injectable, inject } from '@angular/core';
import { ApiClientService, ActionResult } from '../core/api-client.service';

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

/** Wraps {@link ApiClientService} with the FinOps envelope and typed results. */
@Injectable({ providedIn: 'root' })
export class FinopsService {
  private readonly api = inject(ApiClientService);

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
}
