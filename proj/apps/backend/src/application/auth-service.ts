import { randomUUID } from 'node:crypto';
import type { AwsAccount, PlatformUser } from '@platform/shared';
import { buildDefaultPermissions } from '../domain/default-permissions.js';
import { createAppError } from '../domain/errors.js';
import type { PermissionRepository, UserRepository } from '../infra/repositories/types.js';
import { hashPassword, verifyPassword } from '../infra/security/password.js';

export type PublicUser = {
  id: string;
  name: string;
  email: string;
  role: PlatformUser['role'];
  accounts: PlatformUser['accounts'];
};

type CreateAuthServiceDependencies = {
  userRepository: UserRepository;
  permissionRepository: PermissionRepository;
};

const toPublicUser = (user: PlatformUser): PublicUser => ({
  id: user.id,
  name: user.name,
  email: user.email,
  role: user.role,
  accounts: user.accounts
});

type RegisterInput = {
  name: string;
  email: string;
  password: string;
  accounts: readonly AwsAccount[];
};

export const createAuthService = ({
  userRepository,
  permissionRepository
}: CreateAuthServiceDependencies) => ({
  login: async (email: string, password: string): Promise<PlatformUser> => {
    const user = await userRepository.findByEmail(email);

    const authenticatedUser =
      user && verifyPassword(password, user.passwordHash) ? user : undefined;

    if (!authenticatedUser) {
      throw createAppError('INVALID_CREDENTIALS', 'Credenciais invalidas.', 401);
    }

    return authenticatedUser;
  },

  register: async (input: RegisterInput): Promise<PlatformUser> => {
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
      role: 'viewer'
    });

    await userRepository.replaceAccounts(userId, input.accounts);

    const defaultPermissions = buildDefaultPermissions('viewer', input.accounts);
    await permissionRepository.replaceByUserId(userId, defaultPermissions);

    const persistedUser = await userRepository.findById(userId);
    if (!persistedUser) {
      throw createAppError('USER_NOT_FOUND', 'Usuario nao encontrado.', 404);
    }

    return persistedUser;
  },

  getById: async (userId: string): Promise<PlatformUser> => {
    const user = await userRepository.findById(userId);

    if (!user) {
      throw createAppError('USER_NOT_FOUND', 'Usuario nao encontrado.', 404);
    }

    return user;
  },

  toPublicUser
});
