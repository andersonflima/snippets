import { Injectable, inject } from '@angular/core';
import { ApiClientService } from '../../core/api-client.service';
import { SettingsService } from '../../core/settings.service';
import { JsonValue } from '../../core/json-schema';

/** AWS products the insights service can enumerate. */
export type ResourceProduct =
  | 'rds'
  | 'ec2'
  | 'ebs'
  | 'elb'
  | 'eip'
  | 'snapshot'
  | 'kms'
  | 'vpc-endpoint';

/** Product selector for the list page — individual products plus the `all` scope. */
export type ProductScope = ResourceProduct | 'all';

/** A single inventory item returned by the `resources` action. */
export interface ResourceItem {
  id: string;
  name: string;
  product: ResourceProduct;
  type: string;
  env: string;
  region: string;
  status: string;
  size: string;
  createdAt: string;
  tags: Record<string, string>;
  monthlyCost: number;
  utilizationPct: number;
}

/** Server-side filters accepted by the `resources` action. */
export interface ResourceFilters {
  search?: string;
  status?: string;
  env?: string;
  type?: string;
}

/** `detail` block of a successful `resources` response. */
export interface ResourceListDetail {
  items: ResourceItem[];
  total: number;
}

/** One sample within a metric series. */
export interface MetricPoint {
  t: string;
  value: number;
}

/** Aggregate statistics for a metric over the requested window. */
export interface MetricStats {
  avg: number;
  max: number;
  min: number;
  p95: number;
}

/** A single named metric with its samples and stats. */
export interface MetricSeries {
  metric: string;
  unit: string;
  points: MetricPoint[];
  stats: MetricStats;
}

/** `detail` block of a successful `metrics` response. */
export interface ResourceMetricsDetail {
  resourceId: string;
  series: MetricSeries[];
}

/** A single log line for a resource. */
export interface LogEntry {
  ts: string;
  level: string;
  message: string;
  source: string;
}

/** `detail` block of a successful `logs` response. */
export interface ResourceLogsDetail {
  resourceId: string;
  entries: LogEntry[];
  total: number;
}

/** Options accepted when fetching logs. */
export interface LogOptions {
  level?: string;
  search?: string;
  limit?: number;
}

/** Parameters embedded under `params` in every insights request. */
interface InsightsParams {
  action: 'resources' | 'metrics' | 'logs';
  product: ProductScope;
  resourceId?: string;
  filters?: ResourceFilters;
  metric?: string;
  lookback?: number;
  level?: string;
  limit?: number;
}

/**
 * Talks to the `insights` microservice through the BFF. Every call wraps the
 * active AWS envelope around a small `params` block and unwraps `body.detail`,
 * throwing a readable {@link Error} on any non-ok response so callers can render
 * a single failure path.
 */
@Injectable({ providedIn: 'root' })
export class ResourcesService {
  private readonly api = inject(ApiClientService);
  private readonly settings = inject(SettingsService);

  /** Enumerate resources for a product (or all) with optional server-side filters. */
  list(product: ProductScope, filters: ResourceFilters = {}): Promise<ResourceListDetail> {
    const cleaned = this.compactFilters(filters);
    return this.execute<ResourceListDetail>({
      action: 'resources',
      product,
      ...(cleaned ? { filters: cleaned } : {}),
    });
  }

  /** Fetch metric series for a single resource over a `lookback` window (minutes). */
  metrics(
    resourceId: string,
    product: ProductScope,
    lookback: number,
  ): Promise<ResourceMetricsDetail> {
    return this.execute<ResourceMetricsDetail>({
      action: 'metrics',
      product,
      resourceId,
      lookback,
    });
  }

  /** Fetch recent log entries for a resource, optionally filtered by level/search. */
  logs(
    resourceId: string,
    product: ProductScope,
    opts: LogOptions = {},
  ): Promise<ResourceLogsDetail> {
    const search = opts.search?.trim();
    return this.execute<ResourceLogsDetail>({
      action: 'logs',
      product,
      resourceId,
      ...(opts.level ? { level: opts.level } : {}),
      ...(search ? { filters: { search } } : {}),
      ...(opts.limit != null ? { limit: opts.limit } : {}),
    });
  }

  /** Build the envelope, POST it, and unwrap `body.detail` (throwing on failure). */
  private async execute<T>(params: InsightsParams): Promise<T> {
    const payload = {
      ...this.settings.awsEnvelope(),
      params,
    } as unknown as JsonValue;

    const res = await this.api.runAction('/insights/execute', payload);
    if (!res.ok) {
      throw new Error(this.errorMessage(res.body, res.status, res.statusText));
    }

    const detail = (res.body as { detail?: T } | null)?.detail;
    if (detail == null) {
      throw new Error('Resposta inválida do serviço de insights: campo "detail" ausente.');
    }
    return detail;
  }

  /** Drop blank/whitespace-only filter fields; return undefined when nothing remains. */
  private compactFilters(filters: ResourceFilters): ResourceFilters | undefined {
    const entries = Object.entries(filters).filter(
      ([, value]) => typeof value === 'string' && value.trim() !== '',
    );
    return entries.length ? Object.fromEntries(entries) : undefined;
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
    return `${status} ${statusText}`.trim() || 'Falha ao consultar insights.';
  }
}
