import { Injectable, Signal, computed, signal } from '@angular/core';

const STORAGE_KEY = 'msc.baseUrl';
const DEFAULT_BASE_URL = '/bff';

const PROFILES_KEY = 'msc.awsProfiles';
const ACTIVE_ENV_KEY = 'msc.activeEnv';

/** Deployment environments the operator can target. */
export type Env = 'dev' | 'homol' | 'staging' | 'prod';

/** AWS connection profile bound to a single environment. */
export interface AwsProfile {
  account: string;
  region: string;
  roleArn: string;
}

/** Payload other features embed to authorize AWS-backed actions. */
export interface AwsEnvelope {
  account: string;
  roleArn: string;
  region: string;
  environment: Env;
}

const ENVS: readonly Env[] = ['dev', 'homol', 'staging', 'prod'];
const DEFAULT_REGION = 'sa-east-1';

/** Valid-pattern demo values accepted by the mock backend when the profile is blank. */
const DEMO_ACCOUNT = '000000000000';
const DEMO_ROLE_ARN = 'arn:aws:iam::000000000000:role/insights-demo';

const emptyProfile = (): AwsProfile => ({
  account: '',
  region: DEFAULT_REGION,
  roleArn: '',
});

const emptyProfiles = (): Record<Env, AwsProfile> =>
  ENVS.reduce(
    (acc, env) => ({ ...acc, [env]: emptyProfile() }),
    {} as Record<Env, AwsProfile>,
  );

/**
 * Holds operator settings. The base URL points at the BFF, the single backend
 * the frontend talks to. The BFF proxies microservice actions and owns auth:
 * it sets an httpOnly session cookie, so calls go out with `withCredentials`
 * and the browser never sees the token.
 *
 * It also stores AWS connection profiles per environment. Other features read
 * the active profile through `awsEnvelope()` to prefill/authorize their request
 * payloads. All state is persisted to localStorage.
 */
@Injectable({ providedIn: 'root' })
export class SettingsService {
  private readonly baseUrlSig = signal<string>(this.readInitialBaseUrl());
  private readonly awsProfilesSig = signal<Record<Env, AwsProfile>>(
    this.readInitialProfiles(),
  );
  private readonly activeEnvSig = signal<Env>(this.readInitialActiveEnv());

  readonly baseUrl = this.baseUrlSig.asReadonly();
  readonly awsProfiles = this.awsProfilesSig.asReadonly();
  readonly activeEnv = this.activeEnvSig.asReadonly();

  /** Profile for the currently active environment (never null). */
  readonly activeProfile: Signal<AwsProfile> = computed(
    () => this.awsProfilesSig()[this.activeEnvSig()] ?? emptyProfile(),
  );

  setBaseUrl(url: string): void {
    const trimmed = url.trim().replace(/\/+$/, '');
    this.baseUrlSig.set(trimmed);
    this.persist(STORAGE_KEY, trimmed);
  }

  setActiveEnv(env: Env): void {
    this.activeEnvSig.set(env);
    this.persist(ACTIVE_ENV_KEY, env);
  }

  setAwsProfile(env: Env, profile: AwsProfile): void {
    const next: Record<Env, AwsProfile> = {
      ...this.awsProfilesSig(),
      [env]: {
        account: profile.account.trim(),
        region: profile.region.trim() || DEFAULT_REGION,
        roleArn: profile.roleArn.trim(),
      },
    };
    this.awsProfilesSig.set(next);
    this.persist(PROFILES_KEY, JSON.stringify(next));
  }

  /**
   * Builds the AWS envelope other features embed in request payloads. Returns
   * the active profile, but falls back to valid-pattern demo values whenever
   * account/roleArn are empty so the mock backend still accepts the call.
   */
  awsEnvelope(): AwsEnvelope {
    const profile = this.activeProfile();
    const account = profile.account.trim();
    const roleArn = profile.roleArn.trim();
    return {
      account: account || DEMO_ACCOUNT,
      roleArn: roleArn || DEMO_ROLE_ARN,
      region: profile.region.trim() || DEFAULT_REGION,
      environment: this.activeEnvSig(),
    };
  }

  private persist(key: string, value: string): void {
    try {
      localStorage.setItem(key, value);
    } catch {
      // localStorage may be unavailable (private mode); keep in-memory value.
    }
  }

  private readInitialBaseUrl(): string {
    try {
      return localStorage.getItem(STORAGE_KEY) ?? DEFAULT_BASE_URL;
    } catch {
      return DEFAULT_BASE_URL;
    }
  }

  private readInitialActiveEnv(): Env {
    try {
      const raw = localStorage.getItem(ACTIVE_ENV_KEY);
      return raw && (ENVS as readonly string[]).includes(raw)
        ? (raw as Env)
        : 'dev';
    } catch {
      return 'dev';
    }
  }

  private readInitialProfiles(): Record<Env, AwsProfile> {
    const base = emptyProfiles();
    try {
      const raw = localStorage.getItem(PROFILES_KEY);
      if (!raw) return base;
      const parsed = JSON.parse(raw) as Partial<Record<Env, AwsProfile>>;
      for (const env of ENVS) {
        const p = parsed?.[env];
        if (p && typeof p === 'object') {
          base[env] = {
            account: typeof p.account === 'string' ? p.account : '',
            region: typeof p.region === 'string' && p.region ? p.region : DEFAULT_REGION,
            roleArn: typeof p.roleArn === 'string' ? p.roleArn : '',
          };
        }
      }
      return base;
    } catch {
      return base;
    }
  }
}
