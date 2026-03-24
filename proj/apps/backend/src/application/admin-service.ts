import { randomUUID } from 'node:crypto';
import type {
  AwsCategory,
  AwsAccount,
  DeleteIntent,
  PermissionScope,
  PlatformUser,
  UserRole
} from '@platform/shared';
import { buildDefaultPermissions } from '../domain/default-permissions.js';
import { createAppError } from '../domain/errors.js';
import type {
  DeleteIntentRepository,
  PermissionRepository,
  UserRepository
} from '../infra/repositories/types.js';
import { hashPassword } from '../infra/security/password.js';

export type AdminUserView = {
  id: string;
  name: string;
  email: string;
  role: UserRole;
  accounts: readonly AwsAccount[];
  permissions: readonly PermissionScope[];
};

type CreateManagedUserInput = {
  name: string;
  email: string;
  password: string;
  role: UserRole;
  accounts: readonly AwsAccount[];
  permissions?: readonly PermissionScope[];
};

type UpdateManagedUserInput = {
  userId: string;
  name?: string;
  email?: string;
  password?: string;
  role?: UserRole;
};

type CreateAdminServiceDependencies = {
  userRepository: UserRepository;
  permissionRepository: PermissionRepository;
  deleteIntentRepository: DeleteIntentRepository;
};

const ADMIN_DELETE_INTENT_ACCOUNT_ID = 'platform';
const ADMIN_DELETE_INTENT_REGION = 'global';
const ADMIN_DELETE_INTENT_CATEGORY: AwsCategory = 'management';
const ADMIN_DELETE_INTENT_RESOURCE_TYPE = 'platform:user';

const toPermissionScopes = (scopes: readonly PermissionScope[]): readonly PermissionScope[] =>
  scopes.map((scope) => ({
    accountId: scope.accountId?.trim() || '*',
    category: scope.category ?? '*',
    resourceType: scope.resourceType?.trim() || '*',
    action: scope.action
  }));

const toView = async (
  user: PlatformUser,
  permissionRepository: PermissionRepository
): Promise<AdminUserView> => {
  const rules = await permissionRepository.listByUserId(user.id);

  return {
    id: user.id,
    name: user.name,
    email: user.email,
    role: user.role,
    accounts: user.accounts,
    permissions: rules.map((rule) => ({
      accountId: rule.accountId,
      category: rule.category,
      resourceType: rule.resourceType,
      action: rule.action
    }))
  };
};

export const createAdminService = ({
  userRepository,
  permissionRepository,
  deleteIntentRepository
}: CreateAdminServiceDependencies) => ({
  listUsers: async (): Promise<readonly AdminUserView[]> => {
    const users = await userRepository.listAll();
    return Promise.all(users.map((user) => toView(user, permissionRepository)));
  },

  createUser: async (input: CreateManagedUserInput): Promise<AdminUserView> => {
    const existingUser = await userRepository.findByEmail(input.email);
    if (existingUser) {
      throw createAppError('USER_ALREADY_EXISTS', 'Email ja cadastrado.', 409);
    }

    const userId = randomUUID();

    await userRepository.create({
      id: userId,
      name: input.name,
      email: input.email,
      passwordHash: hashPassword(input.password),
      role: input.role
    });

    await userRepository.replaceAccounts(userId, input.accounts);

    const scopes =
      input.permissions && input.permissions.length > 0
        ? toPermissionScopes(input.permissions)
        : buildDefaultPermissions(input.role, input.accounts);

    await permissionRepository.replaceByUserId(userId, scopes);

    const persistedUser = await userRepository.findById(userId);
    if (!persistedUser) {
      throw createAppError('USER_NOT_FOUND', 'Usuario nao encontrado.', 404);
    }

    return toView(persistedUser, permissionRepository);
  },

  updateUser: async (input: UpdateManagedUserInput): Promise<AdminUserView> => {
    const existingUser = await userRepository.findById(input.userId);
    if (!existingUser) {
      throw createAppError('USER_NOT_FOUND', 'Usuario nao encontrado.', 404);
    }

    if (input.email && input.email !== existingUser.email) {
      const duplicateUser = await userRepository.findByEmail(input.email);
      if (duplicateUser && duplicateUser.id !== existingUser.id) {
        throw createAppError('USER_ALREADY_EXISTS', 'Email ja cadastrado.', 409);
      }
    }

    const updatedUser = await userRepository.update({
      id: existingUser.id,
      name: input.name ?? existingUser.name,
      email: input.email ?? existingUser.email,
      role: input.role ?? existingUser.role,
      passwordHash: input.password
        ? hashPassword(input.password)
        : existingUser.passwordHash
    });

    return toView(updatedUser, permissionRepository);
  },

  replaceAccounts: async (userId: string, accounts: readonly AwsAccount[]) => {
    const existingUser = await userRepository.findById(userId);
    if (!existingUser) {
      throw createAppError('USER_NOT_FOUND', 'Usuario nao encontrado.', 404);
    }

    await userRepository.replaceAccounts(userId, accounts);
    const updatedUser = await userRepository.findById(userId);
    if (!updatedUser) {
      throw createAppError('USER_NOT_FOUND', 'Usuario nao encontrado.', 404);
    }

    return toView(updatedUser, permissionRepository);
  },

  getPermissions: async (userId: string): Promise<readonly PermissionScope[]> => {
    const existingUser = await userRepository.findById(userId);
    if (!existingUser) {
      throw createAppError('USER_NOT_FOUND', 'Usuario nao encontrado.', 404);
    }

    const rules = await permissionRepository.listByUserId(userId);
    return rules.map((rule) => ({
      accountId: rule.accountId,
      category: rule.category,
      resourceType: rule.resourceType,
      action: rule.action
    }));
  },

  replacePermissions: async (
    userId: string,
    permissions: readonly PermissionScope[]
  ): Promise<readonly PermissionScope[]> => {
    const existingUser = await userRepository.findById(userId);
    if (!existingUser) {
      throw createAppError('USER_NOT_FOUND', 'Usuario nao encontrado.', 404);
    }

    const normalizedPermissions = toPermissionScopes(permissions);
    const rules = await permissionRepository.replaceByUserId(userId, normalizedPermissions);

    return rules.map((rule) => ({
      accountId: rule.accountId,
      category: rule.category,
      resourceType: rule.resourceType,
      action: rule.action
    }));
  },

  resetPermissionsToRoleDefaults: async (userId: string): Promise<readonly PermissionScope[]> => {
    const user = await userRepository.findById(userId);
    if (!user) {
      throw createAppError('USER_NOT_FOUND', 'Usuario nao encontrado.', 404);
    }

    const defaultPermissions = buildDefaultPermissions(user.role, user.accounts);
    const rules = await permissionRepository.replaceByUserId(userId, defaultPermissions);

    return rules.map((rule) => ({
      accountId: rule.accountId,
      category: rule.category,
      resourceType: rule.resourceType,
      action: rule.action
    }));
  },

  requestDeleteUserIntent: async (actorUserId: string, userId: string): Promise<DeleteIntent> => {
    if (actorUserId === userId) {
      throw createAppError('INVALID_OPERATION', 'Nao e permitido remover o proprio usuario.', 409);
    }

    const existingUser = await userRepository.findById(userId);
    if (!existingUser) {
      throw createAppError('USER_NOT_FOUND', 'Usuario nao encontrado.', 404);
    }

    return deleteIntentRepository.create({
      userId: actorUserId,
      accountId: ADMIN_DELETE_INTENT_ACCOUNT_ID,
      region: ADMIN_DELETE_INTENT_REGION,
      category: ADMIN_DELETE_INTENT_CATEGORY,
      resourceType: ADMIN_DELETE_INTENT_RESOURCE_TYPE,
      resourceId: userId,
      ttlInSeconds: 120
    });
  },

  deleteUser: async (actorUserId: string, userId: string, intentId: string): Promise<void> => {
    if (actorUserId === userId) {
      throw createAppError('INVALID_OPERATION', 'Nao e permitido remover o proprio usuario.', 409);
    }

    const existingUser = await userRepository.findById(userId);
    if (!existingUser) {
      throw createAppError('USER_NOT_FOUND', 'Usuario nao encontrado.', 404);
    }

    const intent = await deleteIntentRepository.findById(intentId);
    if (!intent) {
      throw createAppError(
        'DELETE_INTENT_NOT_FOUND',
        'Confirmacao de delete expirada ou inexistente.',
        410
      );
    }

    const intentMatchesRequest =
      intent.userId === actorUserId &&
      intent.accountId === ADMIN_DELETE_INTENT_ACCOUNT_ID &&
      intent.region === ADMIN_DELETE_INTENT_REGION &&
      intent.category === ADMIN_DELETE_INTENT_CATEGORY &&
      intent.resourceType === ADMIN_DELETE_INTENT_RESOURCE_TYPE &&
      intent.resourceId === userId;

    if (!intentMatchesRequest) {
      throw createAppError(
        'DELETE_INTENT_MISMATCH',
        'A confirmacao de delete nao corresponde ao usuario selecionado.',
        409
      );
    }

    await userRepository.deleteById(userId);
    await deleteIntentRepository.removeById(intentId);
  }
});
