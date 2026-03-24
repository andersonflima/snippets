import type {
  AwsAccount,
  AwsCategory,
  DeleteIntent,
  PermissionRule,
  PermissionScope,
  PlatformUser,
  ResourceAction,
  UserContext,
  UserRole
} from '@platform/shared';

export type CreateUserInput = {
  id: string;
  email: string;
  name: string;
  passwordHash: string;
  role: UserRole;
};

export type UpdateUserInput = {
  id: string;
  email: string;
  name: string;
  passwordHash: string;
  role: UserRole;
};

export type UserRepository = {
  findByEmail: (email: string) => Promise<PlatformUser | undefined>;
  findById: (id: string) => Promise<PlatformUser | undefined>;
  listById: (ids: readonly string[]) => Promise<readonly PlatformUser[]>;
  listAll: () => Promise<readonly PlatformUser[]>;
  create: (input: CreateUserInput) => Promise<PlatformUser>;
  update: (input: UpdateUserInput) => Promise<PlatformUser>;
  deleteById: (id: string) => Promise<void>;
  replaceAccounts: (userId: string, accounts: readonly AwsAccount[]) => Promise<readonly AwsAccount[]>;
};

export type ContextRepository = {
  getByUserId: (userId: string) => Promise<UserContext | undefined>;
  save: (userId: string, context: UserContext) => Promise<UserContext>;
};

export type DeleteIntentRepository = {
  create: (
    input: Omit<DeleteIntent, 'id' | 'expiresAt'> & {
      ttlInSeconds: number;
    }
  ) => Promise<DeleteIntent>;
  findById: (id: string) => Promise<DeleteIntent | undefined>;
  removeById: (id: string) => Promise<void>;
  purgeExpired: (now?: number) => Promise<void>;
};

export type PermissionQuery = {
  userId: string;
  accountId: string;
  category: AwsCategory;
  action: ResourceAction;
  resourceType?: string;
};

export type PermissionRepository = {
  listByUserId: (userId: string) => Promise<readonly PermissionRule[]>;
  replaceByUserId: (userId: string, scopes: readonly PermissionScope[]) => Promise<readonly PermissionRule[]>;
  isAllowed: (query: PermissionQuery) => Promise<boolean>;
};
