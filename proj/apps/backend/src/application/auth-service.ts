import type { PlatformUser } from '@platform/shared';
import { createAppError } from '../domain/errors.js';
import type { UserRepository } from '../infra/repositories/types.js';
import { verifyPassword } from '../infra/security/password.js';

export type PublicUser = {
  id: string;
  name: string;
  email: string;
  role: PlatformUser['role'];
  accounts: PlatformUser['accounts'];
};

type CreateAuthServiceDependencies = {
  userRepository: UserRepository;
};

const toPublicUser = (user: PlatformUser): PublicUser => ({
  id: user.id,
  name: user.name,
  email: user.email,
  role: user.role,
  accounts: user.accounts
});

export const createAuthService = ({ userRepository }: CreateAuthServiceDependencies) => ({
  login: async (email: string, password: string): Promise<PlatformUser> => {
    const user = await userRepository.findByEmail(email);

    const authenticatedUser =
      user && verifyPassword(password, user.passwordHash) ? user : undefined;

    if (!authenticatedUser) {
      throw createAppError('INVALID_CREDENTIALS', 'Credenciais invalidas.', 401);
    }

    return authenticatedUser;
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
