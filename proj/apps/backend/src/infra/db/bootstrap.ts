import { randomUUID } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import type { AwsAccount, PlatformUser, UserRole } from '@platform/shared';
import { buildDefaultPermissions } from '../../domain/default-permissions.js';
import { hashPassword } from '../security/password.js';
import type { DatabaseClient } from './postgres.js';

type SeedUser = Omit<PlatformUser, 'passwordHash'> & {
  rawPassword: string;
};

type SeedAccountKey = 'production' | 'sandbox' | 'data' | 'qa' | 'shared';
type SeedAccountCatalog = Readonly<Record<SeedAccountKey, AwsAccount>>;

const ROLE_ARN_STORED_PLACEHOLDER = 'managed-by-env-template';
const seedAccountKeys: readonly SeedAccountKey[] = ['production', 'sandbox', 'data', 'qa', 'shared'];
const defaultSeedAccountsFileUrl = new URL('../../../.localstack-organization-accounts.json', import.meta.url);

const createSeedAccount = (
  accountId: string,
  name: string,
  allowedRegions: readonly string[]
): AwsAccount => ({
  accountId,
  name,
  allowedRegions
});

const defaultSeedAccounts = Object.freeze({
  production: createSeedAccount('111111111111', 'Production', ['us-east-1', 'us-west-2', 'sa-east-1']),
  sandbox: createSeedAccount('222222222222', 'Sandbox', ['us-east-1', 'eu-west-1', 'sa-east-1']),
  data: createSeedAccount('333333333333', 'Data', ['us-east-1', 'us-east-2']),
  qa: createSeedAccount('444444444444', 'QA', ['us-east-1', 'eu-central-1']),
  shared: createSeedAccount('555555555555', 'Shared Services', ['us-east-1', 'sa-east-1'])
});

const isNonEmptyString = (input: unknown): input is string =>
  typeof input === 'string' && input.trim().length > 0;

const isStringArray = (input: unknown): input is readonly string[] =>
  Array.isArray(input) && input.every((entry) => typeof entry === 'string' && entry.trim().length > 0);

const toSeedAccount = (key: SeedAccountKey, candidate: unknown): AwsAccount => {
  if (typeof candidate !== 'object' || candidate === null) {
    throw new Error(`Invalid seed account config for ${key}.`);
  }

  const record = candidate as Record<string, unknown>;

  if (
    !isNonEmptyString(record.accountId) ||
    !isNonEmptyString(record.name) ||
    !isStringArray(record.allowedRegions)
  ) {
    throw new Error(`Invalid seed account config for ${key}.`);
  }

  return createSeedAccount(record.accountId.trim(), record.name.trim(), record.allowedRegions);
};

const toSeedAccountCatalog = (candidate: unknown): SeedAccountCatalog => {
  if (typeof candidate !== 'object' || candidate === null) {
    throw new Error('Invalid seed account catalog.');
  }

  const record = candidate as Record<string, unknown>;

  return Object.freeze(
    seedAccountKeys.reduce<Record<SeedAccountKey, AwsAccount>>(
      (accumulator, key) => ({
        ...accumulator,
        [key]: toSeedAccount(key, record[key])
      }),
      {} as Record<SeedAccountKey, AwsAccount>
    )
  );
};

const resolveSeedAccountsFilePath = (): string | URL | undefined => {
  const envPath = process.env.PLATFORM_SEED_ACCOUNTS_FILE?.trim();

  if (envPath) {
    return envPath;
  }

  return existsSync(defaultSeedAccountsFileUrl) ? defaultSeedAccountsFileUrl : undefined;
};

const readSeedAccountsFromFile = (filePath: string | URL): SeedAccountCatalog => {
  const rawContent = readFileSync(filePath, 'utf8');
  const parsed = JSON.parse(rawContent) as unknown;

  if (typeof parsed === 'object' && parsed !== null && 'seedAccounts' in parsed) {
    const wrapped = parsed as { seedAccounts?: unknown };
    return toSeedAccountCatalog(wrapped.seedAccounts);
  }

  return toSeedAccountCatalog(parsed);
};

const resolveSeedAccounts = (): SeedAccountCatalog => {
  const filePath = resolveSeedAccountsFilePath();

  if (!filePath) {
    return defaultSeedAccounts;
  }

  return readSeedAccountsFromFile(filePath);
};

const seededUsers = (): readonly SeedUser[] => {
  const seedAccounts = resolveSeedAccounts();

  return [
    {
      id: 'u-admin',
      name: 'Platform Admin',
      email: 'admin@platform.local',
      rawPassword: 'change-me-please',
      role: 'admin',
      accounts: [
        seedAccounts.production,
        seedAccounts.sandbox,
        seedAccounts.data,
        seedAccounts.qa,
        seedAccounts.shared
      ]
    },
    {
      id: 'u-operator',
      name: 'Platform Operator',
      email: 'operator@platform.local',
      rawPassword: 'change-me-please',
      role: 'operator',
      accounts: [seedAccounts.sandbox, seedAccounts.data, seedAccounts.qa]
    },
    {
      id: 'u-viewer',
      name: 'Platform Viewer',
      email: 'viewer@platform.local',
      rawPassword: 'change-me-please',
      role: 'viewer',
      accounts: [seedAccounts.sandbox, seedAccounts.shared]
    }
  ];
};

const createSchema = async (databaseClient: DatabaseClient): Promise<void> => {
  await databaseClient.query(`
    CREATE TABLE IF NOT EXISTS platform_users (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT UNIQUE NOT NULL,
      password_hash TEXT NOT NULL,
      role TEXT NOT NULL CHECK (role IN ('admin', 'operator', 'viewer'))
    );
  `);

  await databaseClient.query(`
    CREATE TABLE IF NOT EXISTS user_accounts (
      user_id TEXT NOT NULL REFERENCES platform_users(id) ON DELETE CASCADE,
      account_id TEXT NOT NULL,
      account_name TEXT NOT NULL,
      role_arn TEXT NOT NULL,
      allowed_regions TEXT[] NOT NULL,
      PRIMARY KEY (user_id, account_id)
    );
  `);

  await databaseClient.query(`
    CREATE TABLE IF NOT EXISTS user_contexts (
      user_id TEXT PRIMARY KEY REFERENCES platform_users(id) ON DELETE CASCADE,
      account_id TEXT NOT NULL,
      region TEXT NOT NULL,
      category TEXT NOT NULL CHECK (
        category IN ('compute', 'storage', 'database', 'network', 'security', 'management')
      ),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await databaseClient.query(`
    CREATE TABLE IF NOT EXISTS delete_intents (
      id UUID PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES platform_users(id) ON DELETE CASCADE,
      account_id TEXT NOT NULL,
      region TEXT NOT NULL,
      category TEXT NOT NULL CHECK (
        category IN ('compute', 'storage', 'database', 'network', 'security', 'management')
      ),
      resource_type TEXT NOT NULL,
      resource_id TEXT NOT NULL,
      expires_at TIMESTAMPTZ NOT NULL
    );
  `);

  await databaseClient.query(`
    CREATE INDEX IF NOT EXISTS delete_intents_expires_at_idx
      ON delete_intents (expires_at);
  `);

  await databaseClient.query(`
    CREATE TABLE IF NOT EXISTS user_permissions (
      id UUID PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES platform_users(id) ON DELETE CASCADE,
      account_id TEXT NOT NULL DEFAULT '*',
      category TEXT NOT NULL CHECK (
        category IN ('*', 'compute', 'storage', 'database', 'network', 'security', 'management')
      ),
      resource_type TEXT NOT NULL DEFAULT '*',
      action TEXT NOT NULL CHECK (
        action IN ('list', 'get', 'create', 'update', 'delete')
      ),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (user_id, account_id, category, resource_type, action)
    );
  `);

  await databaseClient.query(`
    CREATE INDEX IF NOT EXISTS user_permissions_lookup_idx
      ON user_permissions (user_id, account_id, category, resource_type, action);
  `);

  await databaseClient.query(`
    CREATE TABLE IF NOT EXISTS resource_states (
      id UUID PRIMARY KEY,
      user_id TEXT NOT NULL REFERENCES platform_users(id) ON DELETE CASCADE,
      account_id TEXT NOT NULL,
      region TEXT NOT NULL,
      category TEXT NOT NULL CHECK (
        category IN ('compute', 'storage', 'database', 'network', 'security', 'management')
      ),
      type_name TEXT NOT NULL,
      identifier TEXT NOT NULL,
      version INTEGER NOT NULL,
      operation TEXT NOT NULL CHECK (operation IN ('create', 'update', 'delete')),
      status TEXT NOT NULL CHECK (status IN ('planned', 'submitted', 'applied', 'failed')),
      desired_state JSONB NOT NULL DEFAULT '{}'::JSONB,
      patch_document JSONB,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      created_by TEXT NOT NULL,
      UNIQUE (account_id, region, category, type_name, identifier, version)
    );
  `);

  await databaseClient.query(`
    CREATE INDEX IF NOT EXISTS resource_states_lookup_idx
      ON resource_states (account_id, region, category, type_name, identifier, created_at DESC);
  `);

  await databaseClient.query(`
    CREATE INDEX IF NOT EXISTS resource_states_latest_idx
      ON resource_states (account_id, region, category, type_name, identifier, version DESC, created_at DESC);
  `);
};

const insertSeedUserIfMissing = async (
  databaseClient: DatabaseClient,
  user: SeedUser
): Promise<void> => {
  await databaseClient.query(
    `
      INSERT INTO platform_users (id, name, email, password_hash, role)
      VALUES ($1, $2, LOWER($3), $4, $5)
      ON CONFLICT (id) DO NOTHING;
    `,
    [user.id, user.name, user.email, hashPassword(user.rawPassword), user.role]
  );
};

const upsertSeedAccounts = async (
  databaseClient: DatabaseClient,
  user: SeedUser
): Promise<void> => {
  for (const account of user.accounts) {
    await databaseClient.query(
      `
        INSERT INTO user_accounts (
          user_id,
          account_id,
          account_name,
          role_arn,
          allowed_regions
        )
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (user_id, account_id) DO UPDATE
        SET
          account_name = EXCLUDED.account_name,
          role_arn = EXCLUDED.role_arn,
          allowed_regions = EXCLUDED.allowed_regions;
      `,
      [user.id, account.accountId, account.name, ROLE_ARN_STORED_PLACEHOLDER, account.allowedRegions]
    );
  }
};

const deleteMissingSeedAccounts = async (
  databaseClient: DatabaseClient,
  user: SeedUser
): Promise<void> => {
  const desiredAccountIds = user.accounts.map((account) => account.accountId);

  await databaseClient.query(
    `
      DELETE FROM user_contexts
      WHERE user_id = $1
        AND NOT (account_id = ANY($2::text[]));
    `,
    [user.id, desiredAccountIds]
  );

  await databaseClient.query(
    `
      DELETE FROM user_permissions
      WHERE user_id = $1
        AND account_id <> '*'
        AND NOT (account_id = ANY($2::text[]));
    `,
    [user.id, desiredAccountIds]
  );

  await databaseClient.query(
    `
      DELETE FROM user_accounts
      WHERE user_id = $1
        AND NOT (account_id = ANY($2::text[]));
    `,
    [user.id, desiredAccountIds]
  );
};

const upsertSeedPermissions = async (
  databaseClient: DatabaseClient,
  user: SeedUser
): Promise<void> => {
  const permissionScopes = buildDefaultPermissions(user.role, user.accounts);

  for (const scope of permissionScopes) {
    await databaseClient.query(
      `
        INSERT INTO user_permissions (
          id,
          user_id,
          account_id,
          category,
          resource_type,
          action
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (user_id, account_id, category, resource_type, action) DO NOTHING;
      `,
      [randomUUID(), user.id, scope.accountId, scope.category, scope.resourceType, scope.action]
    );
  }
};

const syncSeedUsers = async (databaseClient: DatabaseClient): Promise<void> => {
  const users = seededUsers();

  await databaseClient.query('BEGIN');

  try {
    for (const user of users) {
      await insertSeedUserIfMissing(databaseClient, user);
      await upsertSeedAccounts(databaseClient, user);
      await deleteMissingSeedAccounts(databaseClient, user);
      await upsertSeedPermissions(databaseClient, user);
    }

    await databaseClient.query('COMMIT');
  } catch (error: unknown) {
    await databaseClient.query('ROLLBACK');
    throw error;
  }
};

type UserPermissionSeedRow = {
  id: string;
  role: UserRole;
  accounts: unknown;
};

const parseAccounts = (rawValue: unknown): readonly AwsAccount[] => {
  if (!Array.isArray(rawValue)) {
    return [];
  }

  return rawValue.flatMap((entry) => {
    if (typeof entry !== 'object' || entry === null) {
      return [];
    }

    const candidate = entry as Record<string, unknown>;
    const regions = Array.isArray(candidate.allowedRegions)
      ? candidate.allowedRegions.filter((region): region is string => typeof region === 'string')
      : [];

    if (typeof candidate.accountId !== 'string' || typeof candidate.name !== 'string') {
      return [];
    }

    return [
      {
        accountId: candidate.accountId,
        name: candidate.name,
        allowedRegions: regions
      }
    ];
  });
};

const seedMissingPermissions = async (databaseClient: DatabaseClient): Promise<void> => {
  const users = await databaseClient.query<UserPermissionSeedRow>(
    `
      SELECT
        u.id,
        u.role,
        COALESCE(
          JSON_AGG(
            JSON_BUILD_OBJECT(
              'accountId', ua.account_id,
              'name', ua.account_name,
              'allowedRegions', ua.allowed_regions
            )
          ) FILTER (WHERE ua.user_id IS NOT NULL),
          '[]'::json
        ) AS accounts
      FROM platform_users u
      LEFT JOIN user_accounts ua ON ua.user_id = u.id
      GROUP BY u.id, u.role;
    `
  );

  for (const user of users.rows) {
    const permissionCount = await databaseClient.query<{ total: string }>(
      `
        SELECT COUNT(*)::text AS total
        FROM user_permissions
        WHERE user_id = $1;
      `,
      [user.id]
    );

    const hasPermissions = Number(permissionCount.rows[0]?.total ?? '0') > 0;
    if (hasPermissions) {
      continue;
    }

    const accounts = parseAccounts(user.accounts);
    const permissionScopes = buildDefaultPermissions(user.role, accounts);

    for (const scope of permissionScopes) {
      await databaseClient.query(
        `
          INSERT INTO user_permissions (
            id,
            user_id,
            account_id,
            category,
            resource_type,
            action
          )
          VALUES ($1, $2, $3, $4, $5, $6)
          ON CONFLICT (user_id, account_id, category, resource_type, action) DO NOTHING;
        `,
        [randomUUID(), user.id, scope.accountId, scope.category, scope.resourceType, scope.action]
      );
    }
  }
};

export const prepareDatabase = async (databaseClient: DatabaseClient): Promise<void> => {
  await createSchema(databaseClient);
  await syncSeedUsers(databaseClient);
  await seedMissingPermissions(databaseClient);
};
