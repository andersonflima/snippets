import type { AwsCategory, ResourceAction, UserRole } from '@platform/shared';

const completeAccess: readonly ResourceAction[] = ['list', 'get', 'create', 'update', 'delete'];
const operatorAccess: readonly ResourceAction[] = ['list', 'get', 'create', 'update'];
const readOnlyAccess: readonly ResourceAction[] = ['list', 'get'];

const rolePolicy: Record<UserRole, Record<AwsCategory, readonly ResourceAction[]>> = {
  admin: {
    compute: completeAccess,
    storage: completeAccess,
    database: completeAccess,
    network: completeAccess,
    security: completeAccess,
    management: completeAccess
  },
  operator: {
    compute: completeAccess,
    storage: completeAccess,
    database: completeAccess,
    network: completeAccess,
    security: operatorAccess,
    management: operatorAccess
  },
  viewer: {
    compute: readOnlyAccess,
    storage: readOnlyAccess,
    database: readOnlyAccess,
    network: readOnlyAccess,
    security: readOnlyAccess,
    management: readOnlyAccess
  }
};

export const canPerformAction = (
  role: UserRole,
  category: AwsCategory,
  action: ResourceAction
): boolean => rolePolicy[role][category].includes(action);
