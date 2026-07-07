import { Injectable, inject } from '@angular/core';
import { ApiClientService } from '../../core/api-client.service';
import { SettingsService } from '../../core/settings.service';

/** Product the insights microservice inspects for this feature. */
const RDS_PRODUCT = 'rds';
const INSIGHTS_PATH = '/insights/execute';

/** One RDS instance the operator can pick to analyze. */
export interface DbInstance {
  id: string;
  name: string;
  env: string;
  region: string;
  status: string;
  size: string;
}

/** Index attached to a table, as reported by the metadata scan. */
export interface DbIndex {
  name: string;
  unique: boolean;
  columns: string[];
  sizeMb: number;
  scans: number;
  unused: boolean;
}

/** A single table with its physical stats and indexes. */
export interface DbTable {
  schema: string;
  name: string;
  rows: number;
  sizeMb: number;
  partitioned: boolean;
  partitions: number;
  indexes: DbIndex[];
}

/** Index flagged as an unused removal candidate. */
export interface DbUnusedIndex {
  table: string;
  index: string;
  sizeMb: number;
}

/** Slow query captured from the statement statistics. */
export interface DbSlowQuery {
  query: string;
  meanMs: number;
  calls: number;
  rowsAvg: number;
}

/** Table bloat estimate (reclaimable space). */
export interface DbBloat {
  table: string;
  wastedMb: number;
}

/** Practical performance recommendation with a severity. */
export interface DbRecommendation {
  title: string;
  detail: string;
  severity: 'high' | 'medium' | 'low';
}

/** Storage layer stats for the instance. */
export interface DbStorage {
  type: string;
  allocatedGb: number;
  usedGb: number;
  iops: number;
  throughput?: number;
}

/** Connection pool stats for the instance. */
export interface DbConnections {
  max: number;
  current: number;
}

/**
 * The `detail` block returned by the insights microservice for a `metadata`
 * action on an RDS resource. This is the canonical shape the component renders.
 */
export interface DbMetadata {
  engine: string;
  engineVersion: string;
  instanceClass: string;
  storage: DbStorage;
  connections: DbConnections;
  tables: DbTable[];
  unusedIndexes: DbUnusedIndex[];
  slowQueries: DbSlowQuery[];
  bloat: DbBloat[];
  recommendations: DbRecommendation[];
}

interface InsightsBody<T> {
  detail?: T;
}

interface ResourcesDetail {
  items?: DbInstance[];
}

/**
 * Read-only access to the insights microservice for database performance
 * analysis. Builds the AWS envelope from {@link SettingsService} and delegates
 * transport to {@link ApiClientService}, returning the typed `detail` block.
 */
@Injectable({ providedIn: 'root' })
export class DbPerformanceService {
  private readonly api = inject(ApiClientService);
  private readonly settings = inject(SettingsService);

  /** List the RDS instances available to analyze in the active environment. */
  async listInstances(): Promise<DbInstance[]> {
    const payload = {
      ...this.settings.awsEnvelope(),
      params: { action: 'resources', product: RDS_PRODUCT },
    };
    const detail = await this.execute<ResourcesDetail>(payload);
    return detail.items ?? [];
  }

  /** Load the performance metadata for a single RDS instance. */
  async metadata(resourceId: string): Promise<DbMetadata> {
    const payload = {
      ...this.settings.awsEnvelope(),
      params: { action: 'metadata', product: RDS_PRODUCT, resourceId },
    };
    return this.execute<DbMetadata>(payload);
  }

  private async execute<T>(payload: object): Promise<T> {
    const res = await this.api.runAction(INSIGHTS_PATH, payload as never);
    if (!res.ok) {
      throw new Error(
        `Insights request failed: ${res.status} ${res.statusText}`,
      );
    }
    const detail = (res.body as InsightsBody<T> | null)?.detail;
    if (detail == null) {
      throw new Error('Insights response missing detail payload');
    }
    return detail;
  }
}
