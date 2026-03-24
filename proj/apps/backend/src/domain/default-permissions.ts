import type {
  AwsAccount,
  AwsCategory,
  PermissionScope,
  ResourceAction,
  UserRole
} from '@platform/shared';
import { allCategories } from './categories.js';

const allActions: readonly ResourceAction[] = ['list', 'get', 'create', 'update', 'delete'];
const readActions: readonly ResourceAction[] = ['list', 'get'];
const noDeleteActions: readonly ResourceAction[] = ['list', 'get', 'create', 'update'];

const roleCategoryActions = (role: UserRole, category: AwsCategory): readonly ResourceAction[] => {
  if (role === 'admin') {
    return allActions;
  }

  if (role === 'viewer') {
    return readActions;
  }

  if (category === 'security' || category === 'management') {
    return noDeleteActions;
  }

  return allActions;
};

const toScopedPermissions = (
  accountId: string,
  categories: readonly AwsCategory[],
  role: UserRole
): readonly PermissionScope[] =>
  categories.flatMap((category) =>
    roleCategoryActions(role, category).map((action) => ({
      accountId,
      category,
      resourceType: '*',
      action
    }))
  );

export const buildDefaultPermissions = (
  role: UserRole,
  accounts: readonly AwsAccount[]
): readonly PermissionScope[] => {
  if (role === 'admin') {
    return allActions.map((action) => ({
      accountId: '*',
      category: '*',
      resourceType: '*',
      action
    }));
  }

  const categories = allCategories();
  return accounts.flatMap((account) => toScopedPermissions(account.accountId, categories, role));
};
