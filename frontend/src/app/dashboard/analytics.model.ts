/** Domain types for the dashboard analytics (mock now, API-backed later). */

export type ActionStatus = 'success' | 'failed' | 'pending';
export type Environment = 'dev' | 'homol' | 'staging' | 'prod';
export type Tone = 'accent' | 'ok' | 'warn' | 'danger';

export interface Kpi {
  key: string;
  label: string;
  value: number;
  decimals: number;
  prefix?: string;
  suffix?: string;
  /** Percentage change vs the previous equal-length period. */
  deltaPct: number;
  /** True when a positive delta is good (e.g. success rate) — drives arrow color. */
  higherIsBetter: boolean;
  spark: number[];
  tone: Tone;
  icon: string;
}

export interface DayPoint {
  date: string;
  success: number;
  failed: number;
}

export interface ServiceCount {
  service: string;
  count: number;
}

export interface StatusSlice {
  name: string;
  value: number;
  tone: Tone;
}

export interface CostPoint {
  month: string;
  cost: number;
  savings: number;
}

export interface HeatCell {
  service: string;
  env: Environment;
  value: number;
}

export interface ActivityItem {
  id: string;
  service: string;
  action: string;
  actor: string;
  env: Environment;
  status: ActionStatus;
  at: string;
  durationMs: number;
}

export interface Insight {
  title: string;
  detail: string;
  tone: Tone;
}

export interface DashboardData {
  kpis: Kpi[];
  actionsOverTime: DayPoint[];
  byService: ServiceCount[];
  statusBreakdown: StatusSlice[];
  costTrend: CostPoint[];
  heatmap: HeatCell[];
  activity: ActivityItem[];
  insights: Insight[];
}

export const SERVICES = [
  'create',
  'destroy',
  'modify',
  'start-stop',
  'storage',
  'replicate',
  'restore',
  'db-password',
  'vpc-link',
  'kms',
  'finops',
  'servicenow',
  'rds-data',
] as const;

export const ENVIRONMENTS: Environment[] = ['dev', 'homol', 'staging', 'prod'];
