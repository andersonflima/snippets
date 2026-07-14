import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';
import { ApiClientService, ActionResult } from '../../core/api-client.service';
import { SettingsService } from '../../core/settings.service';

/** A metadata query exposed as a button-action. */
export interface DbcaQuery {
  id: string;
  label: string;
  description: string;
  category: string;
  engines: string[];
}

/** Normalized tabular result the screen renders. */
export interface DbcaResult {
  query: string;
  label: string;
  resourceType: string;
  engine: string;
  vpcId: string | null;
  endpoint: string | null;
  region: string;
  columns: string[];
  rows: unknown[][];
  rowCount: number;
  demo?: boolean;
}

/** Local catalog used when the BFF is unreachable (demo mode). Mirrors the service default. */
const DEMO_QUERIES: DbcaQuery[] = [
  { id: 'db-overview', label: 'Visão geral do banco', description: 'Versão, tamanho total e conexões ativas.', category: 'Visão geral', engines: ['aurora-mysql', 'aurora-postgresql', 'dynamodb'] },
  { id: 'table-sizes', label: 'Tamanho das tabelas', description: 'Top tabelas por tamanho em disco.', category: 'Storage', engines: ['aurora-postgresql', 'dynamodb'] },
  { id: 'active-connections', label: 'Conexões ativas', description: 'Sessões abertas por estado e usuário.', category: 'Atividade', engines: ['aurora-mysql', 'aurora-postgresql'] },
  { id: 'long-running', label: 'Queries longas em execução', description: 'Consultas ativas há mais tempo (gargalos).', category: 'Atividade', engines: ['aurora-mysql', 'aurora-postgresql'] },
  { id: 'index-usage', label: 'Uso de índices', description: 'Índices por leituras (identifica ociosos).', category: 'Performance', engines: ['aurora-postgresql'] },
  { id: 'capacity', label: 'Capacidade & throughput', description: 'Cobrança, capacidade e índices (DynamoDB).', category: 'Capacidade', engines: ['dynamodb'] },
];

@Injectable({ providedIn: 'root' })
export class DbcaService {
  private readonly http = inject(HttpClient);
  private readonly api = inject(ApiClientService);
  private readonly settings = inject(SettingsService);

  /** Fetch the admin query catalog; falls back to the demo catalog off-BFF. */
  async queries(): Promise<{ queries: DbcaQuery[]; demo: boolean }> {
    const url = `${this.settings.baseUrl()}/api/dbca/queries`;
    try {
      const res = await firstValueFrom(
        this.http.get<{ queries: DbcaQuery[] }>(url, { withCredentials: true }),
      );
      const queries = res?.queries ?? [];
      return { queries: queries.length ? queries : DEMO_QUERIES, demo: !queries.length };
    } catch {
      return { queries: DEMO_QUERIES, demo: true };
    }
  }

  /** Run a query against a resource. Demo mode returns sample data so the flow is explorable. */
  async run(queryId: string, resource: string, environment: string): Promise<DbcaResult> {
    const env = this.settings.awsEnvelope();
    const payload = {
      account: env.account,
      resource,
      environment,
      params: { queryId },
    };
    const res: ActionResult = await this.api.runAction('/dbca/execute', payload);
    if (res.ok) {
      const detail = (res.body as { detail?: DbcaResult } | null)?.detail;
      if (detail) return { ...detail, demo: false };
    }
    if (this.isDemoMode()) {
      return this.demoResult(queryId, resource);
    }
    throw new Error(this.errorMessage(res));
  }

  private isDemoMode(): boolean {
    return this.settings.activeProfile().account.trim() === '';
  }

  private errorMessage(res: ActionResult): string {
    const body = res.body as { message?: string } | string | null;
    if (body && typeof body === 'object' && typeof body.message === 'string' && body.message.trim()) {
      return body.message;
    }
    if (typeof body === 'string' && body.trim()) return body;
    return `Falha ao executar (${res.status} ${res.statusText || 'erro de rede'}).`;
  }

  /** Deterministic-ish sample result per query, for local/demo exploration. */
  private demoResult(queryId: string, resource: string): DbcaResult {
    const isDynamo = /table|dynamo|ddb/i.test(resource);
    const engine = isDynamo ? 'dynamodb' : 'aurora-postgresql';
    const base = {
      query: queryId,
      label: DEMO_QUERIES.find((q) => q.id === queryId)?.label ?? queryId,
      resourceType: isDynamo ? 'dynamodb' : 'aurora',
      engine,
      vpcId: isDynamo ? null : 'vpc-0a1b2c3d',
      endpoint: isDynamo ? null : `${resource}.cluster-xxxx.sa-east-1.rds.amazonaws.com`,
      region: 'sa-east-1',
      demo: true,
    };
    const table = DEMO_TABLES[isDynamo ? 'dynamodb' : 'aurora'][queryId] ?? DEMO_TABLES.aurora['db-overview'];
    return { ...base, columns: table.columns, rows: table.rows, rowCount: table.rows.length };
  }
}

const DEMO_TABLES: Record<string, Record<string, { columns: string[]; rows: unknown[][] }>> = {
  aurora: {
    'db-overview': { columns: ['versao', 'tamanho', 'conexoes'], rows: [['15.4', '42 GB', 37]] },
    'table-sizes': {
      columns: ['schema', 'tabela', 'tamanho', 'linhas_estimadas'],
      rows: [
        ['public', 'orders', '12 GB', 4820000],
        ['public', 'events', '8.1 GB', 21500000],
        ['public', 'users', '3.4 GB', 1290000],
        ['public', 'sessions', '1.2 GB', 640000],
      ],
    },
    'active-connections': {
      columns: ['usuario', 'estado', 'conexoes'],
      rows: [['app', 'active', 18], ['app', 'idle', 11], ['reports', 'active', 4], ['admin', 'idle', 2]],
    },
    'long-running': {
      columns: ['pid', 'usuario', 'estado', 'segundos', 'query'],
      rows: [
        [48213, 'reports', 'active', 312, 'SELECT ... FROM events JOIN orders ...'],
        [48190, 'app', 'active', 47, 'UPDATE ... (aguardando lock)'],
      ],
    },
    'index-usage': {
      columns: ['schema', 'tabela', 'indice', 'leituras'],
      rows: [
        ['public', 'orders', 'idx_orders_legacy', 0],
        ['public', 'events', 'idx_events_tmp', 12],
        ['public', 'users', 'idx_users_email', 984210],
      ],
    },
  },
  dynamodb: {
    'db-overview': {
      columns: ['Métrica', 'Valor'],
      rows: [
        ['Status', 'ACTIVE'],
        ['Itens (aprox.)', 1284000],
        ['Tamanho (bytes, aprox.)', 934000000],
        ['Cobrança', 'PAY_PER_REQUEST'],
        ['GSIs', 2],
        ['LSIs', 0],
      ],
    },
    'table-sizes': {
      columns: ['Atributo', 'Tipo', 'Chave'],
      rows: [['pk', 'S', 'HASH'], ['sk', 'S', 'RANGE'], ['gsi1pk', 'S', '-'], ['status', 'S', '-']],
    },
    capacity: {
      columns: ['Alvo', 'Cobrança', 'RCU', 'WCU'],
      rows: [['Tabela', 'PAY_PER_REQUEST', '-', '-'], ['GSI gsi1', 'PAY_PER_REQUEST', '-', '-']],
    },
  },
};
