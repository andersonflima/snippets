import type {
  AwsCategory,
  ResourceAction,
  UpsertResourcePayload,
  UserRole
} from '@platform/shared';
import { canPerformAction } from '../domain/acl.js';
import { getCategoryResourceTypes } from '../domain/categories.js';
import { createAppError } from '../domain/errors.js';
import type { ResourceGateway } from '../infra/aws/cloud-control.js';
import type {
  ContextRepository,
  DeleteIntentRepository,
  PermissionRepository,
  UserRepository
} from '../infra/repositories/types.js';

type CreateResourceServiceDependencies = {
  userRepository: UserRepository;
  contextRepository: ContextRepository;
  deleteIntentRepository: DeleteIntentRepository;
  permissionRepository: PermissionRepository;
  resourceGateway: ResourceGateway;
};

export const createResourceService = ({
  userRepository,
  contextRepository,
  deleteIntentRepository,
  permissionRepository,
  resourceGateway
}: CreateResourceServiceDependencies) => {
  const assertPermission = async (input: {
    userId: string;
    role: UserRole;
    accountId: string;
    category: AwsCategory;
    action: ResourceAction;
    resourceType?: string;
  }): Promise<void> => {
    if (input.role === 'admin') {
      return;
    }

    const roleAllowsAction = canPerformAction(input.role, input.category, input.action);
    if (!roleAllowsAction) {
      throw createAppError(
        'INSUFFICIENT_PERMISSION',
        `Permissao insuficiente para executar ${input.action} em ${input.category}.`,
        403
      );
    }

    const allowedByAcl = await permissionRepository.isAllowed({
      userId: input.userId,
      accountId: input.accountId,
      category: input.category,
      action: input.action,
      resourceType: input.resourceType
    });

    if (!allowedByAcl) {
      throw createAppError(
        'INSUFFICIENT_PERMISSION',
        `ACL bloqueou ${input.action} para o recurso solicitado.`,
        403
      );
    }
  };

  const resolveExecution = async (userId: string) => {
    const user = await userRepository.findById(userId);

    if (!user) {
      throw createAppError('USER_NOT_FOUND', 'Usuario nao encontrado.', 404);
    }

    const context = await contextRepository.getByUserId(userId);

    if (!context) {
      throw createAppError(
        'CONTEXT_NOT_SELECTED',
        'Selecione conta, regiao e categoria antes de consultar recursos.',
        409
      );
    }

    const account = user.accounts.find((entry) => entry.accountId === context.accountId);

    if (!account) {
      throw createAppError('ACCOUNT_NOT_ALLOWED', 'Conta nao autorizada para o usuario.', 403);
    }

    return {
      user,
      context,
      execution: {
        userId,
        account,
        region: context.region,
        category: context.category
      }
    };
  };

  return {
    listTypes: async (userId: string): Promise<readonly string[]> => {
      const { user, context } = await resolveExecution(userId);
      const categoryTypes = getCategoryResourceTypes(context.category);

      if (user.role === 'admin') {
        return categoryTypes;
      }

      const accessChecks = await Promise.all(
        categoryTypes.map(async (typeName) => {
          const allowed = await permissionRepository.isAllowed({
            userId,
            accountId: context.accountId,
            category: context.category,
            action: 'list',
            resourceType: typeName
          });

          return allowed ? typeName : null;
        })
      );

      return accessChecks.flatMap((typeName) => (typeName ? [typeName] : []));
    },

    listResources: async (userId: string, typeName?: string) => {
      const { user, execution } = await resolveExecution(userId);
      await assertPermission({
        userId,
        role: user.role,
        accountId: execution.account.accountId,
        category: execution.category,
        action: 'list',
        resourceType: typeName
      });

      return resourceGateway.listResources({ execution, typeName });
    },

    discoverResources: async (userId: string, typeName: string) => {
      const { user, execution } = await resolveExecution(userId);
      await assertPermission({
        userId,
        role: user.role,
        accountId: execution.account.accountId,
        category: execution.category,
        action: 'list',
        resourceType: typeName
      });

      const discoveredByRegion = await resourceGateway.discoverResources({
        execution,
        typeName,
        regions: execution.account.allowedRegions
      });

      const resources = discoveredByRegion.flatMap((entry) => entry.resources);
      const regions = discoveredByRegion.map((entry) => ({
        region: entry.region,
        status: entry.status,
        total: entry.resources.length,
        message: entry.message
      }));

      return {
        accountId: execution.account.accountId,
        category: execution.category,
        typeName,
        totalResources: resources.length,
        regions,
        resources
      };
    },

    getResourceDetails: async (userId: string, typeName: string, identifier: string) => {
      const { user, execution } = await resolveExecution(userId);
      await assertPermission({
        userId,
        role: user.role,
        accountId: execution.account.accountId,
        category: execution.category,
        action: 'get',
        resourceType: typeName
      });

      return resourceGateway.getResourceDetails({ execution, typeName, identifier });
    },

    createResource: async (userId: string, payload: UpsertResourcePayload) => {
      const { user, execution } = await resolveExecution(userId);
      await assertPermission({
        userId,
        role: user.role,
        accountId: execution.account.accountId,
        category: execution.category,
        action: 'create',
        resourceType: payload.typeName
      });

      return resourceGateway.createResource({ execution, payload });
    },

    updateResource: async (userId: string, payload: UpsertResourcePayload) => {
      const { user, execution } = await resolveExecution(userId);
      await assertPermission({
        userId,
        role: user.role,
        accountId: execution.account.accountId,
        category: execution.category,
        action: 'update',
        resourceType: payload.typeName
      });

      return resourceGateway.updateResource({ execution, payload });
    },

    requestDeleteIntent: async (userId: string, typeName: string, resourceId: string) => {
      const { user, context } = await resolveExecution(userId);
      await assertPermission({
        userId,
        role: user.role,
        accountId: context.accountId,
        category: context.category,
        action: 'delete',
        resourceType: typeName
      });

      return deleteIntentRepository.create({
        userId,
        accountId: context.accountId,
        region: context.region,
        category: context.category,
        resourceType: typeName,
        resourceId,
        ttlInSeconds: 120
      });
    },

    deleteResource: async (userId: string, intentId: string, typeName: string, resourceId: string) => {
      const { user, context, execution } = await resolveExecution(userId);
      await assertPermission({
        userId,
        role: user.role,
        accountId: context.accountId,
        category: context.category,
        action: 'delete',
        resourceType: typeName
      });

      const intent = await deleteIntentRepository.findById(intentId);

      if (!intent) {
        throw createAppError(
          'DELETE_INTENT_NOT_FOUND',
          'Confirmacao de delete expirada ou inexistente.',
          410
        );
      }

      const intentMatchesRequest =
        intent.userId === userId &&
        intent.accountId === context.accountId &&
        intent.region === context.region &&
        intent.category === context.category &&
        intent.resourceType === typeName &&
        intent.resourceId === resourceId;

      if (!intentMatchesRequest) {
        throw createAppError(
          'DELETE_INTENT_MISMATCH',
          'A confirmacao de delete nao corresponde ao recurso atual.',
          409
        );
      }

      const result = await resourceGateway.deleteResource({
        execution,
        typeName,
        identifier: resourceId
      });

      await deleteIntentRepository.removeById(intentId);
      return result;
    }
  };
};
