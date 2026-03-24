export type UserRole = 'admin' | 'operator' | 'viewer';

export type AwsCategory =
  | 'compute'
  | 'storage'
  | 'database'
  | 'network'
  | 'security'
  | 'management';

export type ResourceAction = 'list' | 'get' | 'create' | 'update' | 'delete';
export type PermissionCategory = AwsCategory | '*';

export type AwsAccount = {
  accountId: string;
  name: string;
  allowedRegions: readonly string[];
};

export type PlatformUser = {
  id: string;
  email: string;
  name: string;
  passwordHash: string;
  role: UserRole;
  accounts: readonly AwsAccount[];
};

export type JwtClaims = {
  sub: string;
  email: string;
  role: UserRole;
};

export type UserContext = {
  accountId: string;
  region: string;
  category: AwsCategory;
};

export type CategoryResourceType = {
  category: AwsCategory;
  resourceTypes: readonly string[];
};

export type ResourceSummary = {
  identifier: string;
  typeName: string;
  displayName: string;
  region: string;
  accountId: string;
};

export type CheckupResult = {
  category: AwsCategory;
  accountId: string;
  region: string;
  resourceCounts: Record<string, number>;
};

export type DeleteIntent = {
  id: string;
  userId: string;
  accountId: string;
  region: string;
  category: AwsCategory;
  resourceType: string;
  resourceId: string;
  expiresAt: number;
};

export type UpsertResourcePayload = {
  typeName: string;
  identifier?: string;
  desiredState: Record<string, unknown>;
  patchDocument?: readonly Record<string, unknown>[];
};

export type PermissionScope = {
  accountId: string;
  category: PermissionCategory;
  resourceType: string;
  action: ResourceAction;
};

export type PermissionRule = PermissionScope & {
  id: string;
  userId: string;
};
