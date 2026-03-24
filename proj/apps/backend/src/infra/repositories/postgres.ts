import { randomUUID } from 'node:crypto';
import type {
  AwsAccount,
  AwsCategory,
  DeleteIntent,
  PermissionCategory,
  PermissionRule,
  PermissionScope,
  PlatformUser,
  ResourceAction,
  UserRole,
  UserContext
} from '@platform/shared';
import type { DatabaseClient } from '../db/postgres.js';
import type {
  ContextRepository,
  DeleteIntentRepository,
  PermissionQuery,
  PermissionRepository,
  UserRepository,
  CreateUserInput,
  UpdateUserInput
} from './types.js';

type UserWithAccountsRow = {
  id: string;
  name: string;
  email: string;
  password_hash: string;
  role: UserRole;
  accounts: unknown;
};

type UserCoreRow = {
  id: string;
  name: string;
  email: string;
  password_hash: string;
  role: UserRole;
};

type UserContextRow = {
  account_id: string;
  region: string;
  category: AwsCategory;
};

type DeleteIntentRow = {
  id: string;
  user_id: string;
  account_id: string;
  region: string;
  category: AwsCategory;
  resource_type: string;
  resource_id: string;
  expires_at: Date;
};

type PermissionRow = {
  id: string;
  user_id: string;
  account_id: string;
  category: PermissionCategory;
  resource_type: string;
  action: ResourceAction;
};

type PermissionAllowedRow = {
  allowed: boolean;
};

const normalizeEmail = (email: string): string => email.trim().toLowerCase();
const normalizeResourceType = (resourceType: string | undefined): string =>
  resourceType && resourceType.trim().length > 0 ? resourceType.trim() : '*';
const normalizeAccountId = (accountId: string | undefined): string =>
  accountId && accountId.trim().length > 0 ? accountId.trim() : '*';
const normalizeCategory = (category: PermissionCategory | undefined): PermissionCategory =>
  category ?? '*';
const ROLE_ARN_STORED_PLACEHOLDER = 'managed-by-env-template';

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

const mapPlatformUser = (row: UserWithAccountsRow): PlatformUser => ({
  id: row.id,
  name: row.name,
  email: row.email,
  passwordHash: row.password_hash,
  role: row.role,
  accounts: parseAccounts(row.accounts)
});

const mapDeleteIntent = (row: DeleteIntentRow): DeleteIntent => ({
  id: row.id,
  userId: row.user_id,
  accountId: row.account_id,
  region: row.region,
  category: row.category,
  resourceType: row.resource_type,
  resourceId: row.resource_id,
  expiresAt: row.expires_at.getTime()
});

const mapPermissionRule = (row: PermissionRow): PermissionRule => ({
  id: row.id,
  userId: row.user_id,
  accountId: row.account_id,
  category: row.category,
  resourceType: row.resource_type,
  action: row.action
});

const userQuery = `
  SELECT
    u.id,
    u.name,
    u.email,
    u.password_hash,
    u.role,
    COALESCE(
      JSON_AGG(
        JSON_BUILD_OBJECT(
          'accountId', ua.account_id,
          'name', ua.account_name,
          'allowedRegions', ua.allowed_regions
        )
        ORDER BY ua.account_id
      ) FILTER (WHERE ua.user_id IS NOT NULL),
      '[]'::json
    ) AS accounts
  FROM platform_users u
  LEFT JOIN user_accounts ua ON ua.user_id = u.id
`;

const fetchUserById = async (
  databaseClient: DatabaseClient,
  userId: string
): Promise<PlatformUser | undefined> => {
  const result = await databaseClient.query<UserWithAccountsRow>(
    `
      ${userQuery}
      WHERE u.id = $1
      GROUP BY u.id, u.name, u.email, u.password_hash, u.role;
    `,
    [userId]
  );

  const row = result.rows[0];
  return row ? mapPlatformUser(row) : undefined;
};

const replaceAccountsForUser = async (
  databaseClient: DatabaseClient,
  userId: string,
  accounts: readonly AwsAccount[]
): Promise<readonly AwsAccount[]> => {
  await databaseClient.query('BEGIN');

  try {
    await databaseClient.query(
      `
        DELETE FROM user_accounts
        WHERE user_id = $1;
      `,
      [userId]
    );

    for (const account of accounts) {
      await databaseClient.query(
        `
          INSERT INTO user_accounts (
            user_id,
            account_id,
            account_name,
            role_arn,
            allowed_regions
          )
          VALUES ($1, $2, $3, $4, $5);
        `,
        [userId, account.accountId, account.name, ROLE_ARN_STORED_PLACEHOLDER, account.allowedRegions]
      );
    }

    await databaseClient.query('COMMIT');
  } catch (error: unknown) {
    await databaseClient.query('ROLLBACK');
    throw error;
  }

  const user = await fetchUserById(databaseClient, userId);
  return user?.accounts ?? [];
};

const deduplicateScopes = (scopes: readonly PermissionScope[]): readonly PermissionScope[] => {
  const byKey = new Map<string, PermissionScope>();

  for (const scope of scopes) {
    const normalizedScope: PermissionScope = {
      accountId: normalizeAccountId(scope.accountId),
      category: normalizeCategory(scope.category),
      resourceType: normalizeResourceType(scope.resourceType),
      action: scope.action
    };

    const key = [
      normalizedScope.accountId,
      normalizedScope.category,
      normalizedScope.resourceType,
      normalizedScope.action
    ].join('|');

    byKey.set(key, normalizedScope);
  }

  return [...byKey.values()];
};

export const createUserRepository = (databaseClient: DatabaseClient): UserRepository => ({
  findByEmail: async (email: string) => {
    const result = await databaseClient.query<UserWithAccountsRow>(
      `
        ${userQuery}
        WHERE LOWER(u.email) = $1
        GROUP BY u.id, u.name, u.email, u.password_hash, u.role;
      `,
      [normalizeEmail(email)]
    );

    const row = result.rows[0];
    return row ? mapPlatformUser(row) : undefined;
  },

  findById: async (id: string) => fetchUserById(databaseClient, id),

  listById: async (ids: readonly string[]) => {
    if (ids.length === 0) {
      return [];
    }

    const result = await databaseClient.query<UserWithAccountsRow>(
      `
        ${userQuery}
        WHERE u.id = ANY($1::text[])
        GROUP BY u.id, u.name, u.email, u.password_hash, u.role;
      `,
      [ids]
    );

    return result.rows.map(mapPlatformUser);
  },

  listAll: async () => {
    const result = await databaseClient.query<UserWithAccountsRow>(
      `
        ${userQuery}
        GROUP BY u.id, u.name, u.email, u.password_hash, u.role
        ORDER BY u.email;
      `
    );

    return result.rows.map(mapPlatformUser);
  },

  create: async (input: CreateUserInput) => {
    await databaseClient.query<UserCoreRow>(
      `
        INSERT INTO platform_users (id, name, email, password_hash, role)
        VALUES ($1, $2, LOWER($3), $4, $5);
      `,
      [input.id, input.name, input.email, input.passwordHash, input.role]
    );

    const createdUser = await fetchUserById(databaseClient, input.id);
    return (
      createdUser ?? {
        id: input.id,
        name: input.name,
        email: normalizeEmail(input.email),
        passwordHash: input.passwordHash,
        role: input.role,
        accounts: []
      }
    );
  },

  update: async (input: UpdateUserInput) => {
    await databaseClient.query<UserCoreRow>(
      `
        UPDATE platform_users
        SET
          name = $2,
          email = LOWER($3),
          password_hash = $4,
          role = $5
        WHERE id = $1;
      `,
      [input.id, input.name, input.email, input.passwordHash, input.role]
    );

    const updatedUser = await fetchUserById(databaseClient, input.id);
    if (updatedUser) {
      return updatedUser;
    }

    return {
      id: input.id,
      name: input.name,
      email: normalizeEmail(input.email),
      passwordHash: input.passwordHash,
      role: input.role,
      accounts: []
    };
  },

  deleteById: async (id: string) => {
    await databaseClient.query(
      `
        DELETE FROM platform_users
        WHERE id = $1;
      `,
      [id]
    );
  },

  replaceAccounts: async (userId: string, accounts: readonly AwsAccount[]) =>
    replaceAccountsForUser(databaseClient, userId, accounts)
});

export const createContextRepository = (databaseClient: DatabaseClient): ContextRepository => ({
  getByUserId: async (userId: string) => {
    const result = await databaseClient.query<UserContextRow>(
      `
        SELECT account_id, region, category
        FROM user_contexts
        WHERE user_id = $1;
      `,
      [userId]
    );

    const row = result.rows[0];
    if (!row) {
      return undefined;
    }

    return {
      accountId: row.account_id,
      region: row.region,
      category: row.category
    } as UserContext;
  },

  save: async (userId: string, context: UserContext) => {
    const result = await databaseClient.query<UserContextRow>(
      `
        INSERT INTO user_contexts (user_id, account_id, region, category, updated_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (user_id) DO UPDATE
        SET
          account_id = EXCLUDED.account_id,
          region = EXCLUDED.region,
          category = EXCLUDED.category,
          updated_at = NOW()
        RETURNING account_id, region, category;
      `,
      [userId, context.accountId, context.region, context.category]
    );

    const row = result.rows[0];
    return {
      accountId: row.account_id,
      region: row.region,
      category: row.category
    };
  }
});

export const createDeleteIntentRepository = (
  databaseClient: DatabaseClient
): DeleteIntentRepository => ({
  create: async (input) => {
    const id = randomUUID();
    const expiresAt = new Date(Date.now() + input.ttlInSeconds * 1000);

    const result = await databaseClient.query<DeleteIntentRow>(
      `
        INSERT INTO delete_intents (
          id,
          user_id,
          account_id,
          region,
          category,
          resource_type,
          resource_id,
          expires_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id, user_id, account_id, region, category, resource_type, resource_id, expires_at;
      `,
      [
        id,
        input.userId,
        input.accountId,
        input.region,
        input.category,
        input.resourceType,
        input.resourceId,
        expiresAt.toISOString()
      ]
    );

    return mapDeleteIntent(result.rows[0]);
  },

  findById: async (id: string) => {
    const result = await databaseClient.query<DeleteIntentRow>(
      `
        SELECT id, user_id, account_id, region, category, resource_type, resource_id, expires_at
        FROM delete_intents
        WHERE id = $1 AND expires_at > NOW();
      `,
      [id]
    );

    const row = result.rows[0];
    return row ? mapDeleteIntent(row) : undefined;
  },

  removeById: async (id: string) => {
    await databaseClient.query(
      `
        DELETE FROM delete_intents
        WHERE id = $1;
      `,
      [id]
    );
  },

  purgeExpired: async (now?: number) => {
    if (typeof now === 'number') {
      await databaseClient.query(
        `
          DELETE FROM delete_intents
          WHERE expires_at <= TO_TIMESTAMP($1 / 1000.0);
        `,
        [now]
      );

      return;
    }

    await databaseClient.query(
      `
        DELETE FROM delete_intents
        WHERE expires_at <= NOW();
      `
    );
  }
});

export const createPermissionRepository = (
  databaseClient: DatabaseClient
): PermissionRepository => ({
  listByUserId: async (userId: string) => {
    const result = await databaseClient.query<PermissionRow>(
      `
        SELECT id, user_id, account_id, category, resource_type, action
        FROM user_permissions
        WHERE user_id = $1
        ORDER BY account_id, category, resource_type, action;
      `,
      [userId]
    );

    return result.rows.map(mapPermissionRule);
  },

  replaceByUserId: async (userId: string, scopes: readonly PermissionScope[]) => {
    const deduplicatedScopes = deduplicateScopes(scopes);

    await databaseClient.query('BEGIN');

    try {
      await databaseClient.query(
        `
          DELETE FROM user_permissions
          WHERE user_id = $1;
        `,
        [userId]
      );

      for (const scope of deduplicatedScopes) {
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
            VALUES ($1, $2, $3, $4, $5, $6);
          `,
          [
            randomUUID(),
            userId,
            normalizeAccountId(scope.accountId),
            normalizeCategory(scope.category),
            normalizeResourceType(scope.resourceType),
            scope.action
          ]
        );
      }

      await databaseClient.query('COMMIT');
    } catch (error: unknown) {
      await databaseClient.query('ROLLBACK');
      throw error;
    }

    const persistedRules = await databaseClient.query<PermissionRow>(
      `
        SELECT id, user_id, account_id, category, resource_type, action
        FROM user_permissions
        WHERE user_id = $1
        ORDER BY account_id, category, resource_type, action;
      `,
      [userId]
    );

    return persistedRules.rows.map(mapPermissionRule);
  },

  isAllowed: async (query: PermissionQuery) => {
    const result = await databaseClient.query<PermissionAllowedRow>(
      `
        SELECT EXISTS (
          SELECT 1
          FROM user_permissions p
          WHERE
            p.user_id = $1
            AND p.action = $2
            AND p.account_id IN ('*', $3)
            AND p.category IN ('*', $4)
            AND ($5::text IS NULL OR p.resource_type IN ('*', $5))
        ) AS allowed;
      `,
      [
        query.userId,
        query.action,
        normalizeAccountId(query.accountId),
        normalizeCategory(query.category),
        query.resourceType ? normalizeResourceType(query.resourceType) : null
      ]
    );

    const row = result.rows[0];
    return row?.allowed ?? false;
  }
});
