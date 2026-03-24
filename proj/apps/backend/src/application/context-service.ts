import type { AwsCategory, AwsAccount, PlatformUser, UserContext } from '@platform/shared';
import { getCategoryResourceTypes } from '../domain/categories.js';
import { createAppError } from '../domain/errors.js';
import type { ResourceGateway } from '../infra/aws/cloud-control.js';
import type {
  ContextRepository,
  UserRepository
} from '../infra/repositories/types.js';

export type SwitchContextInput = {
  userId: string;
  category: AwsCategory;
  accountId: string;
  region: string;
};

type CreateContextServiceDependencies = {
  userRepository: UserRepository;
  contextRepository: ContextRepository;
  resourceGateway: ResourceGateway;
};

const findAccount = (user: PlatformUser, accountId: string): AwsAccount => {
  const account = user.accounts.find((entry) => entry.accountId === accountId);

  if (!account) {
    throw createAppError(
      'ACCOUNT_NOT_ALLOWED',
      `A conta ${accountId} nao esta vinculada ao usuario autenticado.`,
      403
    );
  }

  return account;
};

const assertRegionAllowed = (account: AwsAccount, region: string): void => {
  const regionAllowed = account.allowedRegions.includes(region);

  if (!regionAllowed) {
    throw createAppError(
      'REGION_NOT_ALLOWED',
      `A regiao ${region} nao esta autorizada para a conta ${account.accountId}.`,
      403
    );
  }
};

export const createContextService = ({
  userRepository,
  contextRepository,
  resourceGateway
}: CreateContextServiceDependencies) => ({
  getCurrentContext: async (userId: string): Promise<UserContext | undefined> =>
    contextRepository.getByUserId(userId),

  switchContext: async (input: SwitchContextInput) => {
    const user = await userRepository.findById(input.userId);

    if (!user) {
      throw createAppError('USER_NOT_FOUND', 'Usuario nao encontrado.', 404);
    }

    const targetAccount = findAccount(user, input.accountId);
    assertRegionAllowed(targetAccount, input.region);

    const nextContext: UserContext = {
      accountId: input.accountId,
      region: input.region,
      category: input.category
    };

    await contextRepository.save(user.id, nextContext);

    const checkup = await resourceGateway.runCategoryCheckup({
      userId: user.id,
      account: targetAccount,
      region: input.region,
      category: input.category
    });

    return {
      context: nextContext,
      resourceTypes: getCategoryResourceTypes(input.category),
      checkup
    };
  }
});
