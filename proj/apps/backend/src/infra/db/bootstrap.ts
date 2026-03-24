import { randomUUID } from 'node:crypto';
import type { AwsAccount, PlatformUser, UserRole } from '@platform/shared';
import { buildDefaultPermissions } from '../../domain/default-permissions.js';
import { hashPassword } from '../security/password.js';
import type { DatabaseClient } from './postgres.js';

type SeedUser = Omit<PlatformUser, 'passwordHash'> & {
  rawPassword: string;
};
const ROLE_ARN_STORED_PLACEHOLDER = 'managed-by-env-template';

const seedAccount = (
  accountId: string,
  name: string,
  allowedRegions: readonly string[]
): AwsAccount => ({
  accountId,
  name,
  allowedRegions
});

const seededUsers = (): readonly SeedUser[] => [
  {
    id: 'u-admin',
    name: 'Platform Admin',
    email: 'admin@platform.local',
    rawPassword: 'change-me-please',
    role: 'admin',
    accounts: [
      seedAccount(
        '111111111111',
        'Production',
        ['us-east-1', 'us-west-2', 'sa-east-1']
      ),
      seedAccount(
        '222222222222',
        'Sandbox',
        ['us-east-1', 'eu-west-1', 'sa-east-1']
      )
    ]
  },
  {
    id: 'u-operator',
    name: 'Platform Operator',
    email: 'operator@platform.local',
    rawPassword: 'change-me-please',
    role: 'operator',
    accounts: [
      seedAccount(
        '222222222222',
        'Sandbox',
        ['us-east-1', 'eu-west-1']
      ),
      seedAccount(
        '333333333333',
        'Data',
        ['us-east-1', 'us-east-2']
      )
    ]
  },
  {
    id: 'u-viewer',
    name: 'Platform Viewer',
    email: 'viewer@platform.local',
    rawPassword: 'change-me-please',
    role: 'viewer',
    accounts: [
      seedAccount(
        '222222222222',
        'Sandbox',
        ['us-east-1']
      )
    ]
  }
];

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
};

const seedUsers = async (databaseClient: DatabaseClient): Promise<void> => {
  const existingUsers = await databaseClient.query<{ total: string }>(
    `
      SELECT COUNT(*)::text AS total
      FROM platform_users;
    `
  );

  const hasAnyUser = Number(existingUsers.rows[0]?.total ?? '0') > 0;
  if (hasAnyUser) {
    return;
  }

  const users = seededUsers();

  await databaseClient.query('BEGIN');

  try {
    for (const user of users) {
      await databaseClient.query(
        `
          INSERT INTO platform_users (id, name, email, password_hash, role)
          VALUES ($1, $2, LOWER($3), $4, $5)
          ON CONFLICT (id) DO UPDATE
          SET
            name = EXCLUDED.name,
            email = EXCLUDED.email,
            password_hash = EXCLUDED.password_hash,
            role = EXCLUDED.role;
        `,
        [user.id, user.name, user.email, hashPassword(user.rawPassword), user.role]
      );

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
          [
            user.id,
            account.accountId,
            account.name,
            ROLE_ARN_STORED_PLACEHOLDER,
            account.allowedRegions
          ]
        );
      }

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

    if (
      typeof candidate.accountId !== 'string' ||
      typeof candidate.name !== 'string'
    ) {
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
  await seedUsers(databaseClient);
  await seedMissingPermissions(databaseClient);
};
