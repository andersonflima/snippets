import {
  AwsCategory,
  getResourceTemplate,
  getResourceTemplates,
  ResourceAction,
  ResourceStateAction,
  ResourceStateRecord,
  ResourceTemplate,
  UpsertResourcePayload,
  UserRole
} from '@platform/shared';
import { canPerformAction } from '../domain/acl.js';
import { getCategoryResourceTypes } from '../domain/categories.js';
import { createAppError } from '../domain/errors.js';
import type { AwsExecutionContext } from '../infra/aws/cloud-control.js';
import type { ResourceGateway } from '../infra/aws/cloud-control.js';
import type {
  ContextRepository,
  DeleteIntentRepository,
  PermissionRepository,
  ResourceStateRepository,
  UserRepository
} from '../infra/repositories/types.js';

type CreateResourceServiceDependencies = {
  userRepository: UserRepository;
  contextRepository: ContextRepository;
  deleteIntentRepository: DeleteIntentRepository;
  permissionRepository: PermissionRepository;
  resourceStateRepository: ResourceStateRepository;
  resourceGateway: ResourceGateway;
};

type StateHistoryQuery = {
  typeName?: string;
  identifier?: string;
  limit?: number;
};

const FALLBACK_IDENTIFIER = '__pending__';
const STATE_IDENTIFIER_HINTS = [
  'Identifier',
  'Name',
  'BucketName',
  'DBInstanceIdentifier',
  'DBClusterIdentifier',
  'TableName',
  'FunctionName',
  'ServiceName',
  'ClusterName',
  'RoleName',
  'RoleArn',
  'SecretId',
  'KmsKeyId',
  'KeyId',
  'AlarmName',
  'RuleName',
  'LoadBalancerName',
  'VPCId',
  'SubnetId',
  'SecurityGroupId'
];

const getResourceIdentifierHint = (state: Record<string, unknown>): string | undefined => {
  const hit = STATE_IDENTIFIER_HINTS.find((key) => {
    const value = state[key];
    return typeof value === 'string' && value.trim().length > 0;
  });

  if (!hit) {
    return undefined;
  }

  return String(state[hit]).trim();
};

const normalizeIdentifier = (candidate: string | undefined): string =>
  candidate?.trim().length ? candidate.trim() : FALLBACK_IDENTIFIER;

const parseResourceIdentifierFromOperationResult = (value: unknown): string | undefined => {
  if (!value || typeof value !== 'object') {
    return undefined;
  }

  const asRecord = value as Record<string, unknown>;
  if (typeof asRecord.Identifier === 'string' && asRecord.Identifier.trim().length > 0) {
    return asRecord.Identifier.trim();
  }

  if (typeof asRecord.identifier === 'string' && asRecord.identifier.trim().length > 0) {
    return asRecord.identifier.trim();
  }

  const resourceModel = asRecord.ResourceModel;
  if (typeof resourceModel === 'string') {
    try {
      const parsedModel = JSON.parse(resourceModel) as unknown;
      if (parsedModel && typeof parsedModel === 'object' && 'Identifier' in parsedModel) {
        const modelIdentifier = (parsedModel as Record<string, unknown>).Identifier;
        if (typeof modelIdentifier === 'string' && modelIdentifier.trim().length > 0) {
          return modelIdentifier.trim();
        }
      }
    } catch {
      return undefined;
    }
  }

  return undefined;
};

const toTemplateField = (template: ResourceTemplate | undefined): ResourceTemplate | undefined => {
  if (!template) {
    return undefined;
  }

  return template;
};

const resolveIdentifierFromPayload = (payload: UpsertResourcePayload): string => {
  const explicitIdentifier = payload.identifier;
  if (explicitIdentifier && explicitIdentifier.trim().length > 0) {
    return normalizeIdentifier(explicitIdentifier);
  }

  return normalizeIdentifier(getResourceIdentifierHint(payload.desiredState));
};

const asListStateLimit = (value: number | undefined): number => {
  if (typeof value !== 'number' || !Number.isFinite(value) || value <= 0) {
    return 25;
  }

  return Math.max(1, Math.min(250, Math.floor(value)));
};

const isPresentTemplateValue = (value: unknown): boolean => {
  if (value === undefined || value === null) {
    return false;
  }

  if (typeof value === 'string') {
    return value.trim().length > 0;
  }

  return true;
};

const assertTemplateFieldValue = (
  typeName: string,
  field: ResourceTemplate['fields'][number],
  value: unknown
): void => {
  if (value === undefined || value === null) {
    return;
  }

  const allowedEnumValues = field.enumValues ?? [];
  if (field.kind === 'string' && typeof value !== 'string') {
    throw createAppError(
      'INVALID_RESOURCE_DATA',
      `Campo ${field.key} deve ser string para ${typeName}.`,
      422
    );
  }

  if (field.kind === 'number') {
    if (typeof value !== 'number' || !Number.isFinite(value)) {
      throw createAppError(
        'INVALID_RESOURCE_DATA',
        `Campo ${field.key} deve ser number para ${typeName}.`,
        422
      );
    }
  }

  if (field.kind === 'boolean' && typeof value !== 'boolean') {
    throw createAppError(
      'INVALID_RESOURCE_DATA',
      `Campo ${field.key} deve ser boolean para ${typeName}.`,
      422
    );
  }

  if (field.kind === 'enum') {
    if (typeof value !== 'string') {
      throw createAppError(
        'INVALID_RESOURCE_DATA',
        `Campo ${field.key} deve ser string enum para ${typeName}.`,
        422
      );
    }

    if (allowedEnumValues.length > 0 && !allowedEnumValues.includes(value)) {
      throw createAppError(
        'INVALID_RESOURCE_DATA',
        `Valor invalido para ${field.key} em ${typeName}: ${value}. Valores permitidos: ${allowedEnumValues.join(', ')}`,
        422
      );
    }
  }

  if (field.kind === 'array' && !Array.isArray(value)) {
    throw createAppError(
      'INVALID_RESOURCE_DATA',
      `Campo ${field.key} deve ser array para ${typeName}.`,
      422
    );
  }

  if (
    field.kind === 'object' &&
    (typeof value !== 'object' || value === null || Array.isArray(value))
  ) {
    throw createAppError(
      'INVALID_RESOURCE_DATA',
      `Campo ${field.key} deve ser objeto para ${typeName}.`,
      422
    );
  }
};

const assertTemplateDesiredStateConforms = (
  typeName: string,
  desiredState: Record<string, unknown>
): void => {
  const template = getResourceTemplate(typeName);
  if (!template) {
    throw createAppError(
      'RESOURCE_TEMPLATE_NOT_FOUND',
      `Template para tipo de recurso ${typeName} nao encontrado.`,
      404
    );
  }

  for (const [fieldKey, fieldValue] of Object.entries(desiredState)) {
    const templateField = template.fields.find((entry) => entry.key === fieldKey);
    if (!templateField) {
      continue;
    }

    assertTemplateFieldValue(typeName, templateField, fieldValue);
  }
};

const buildDesiredStateWithTemplateDefaults = (
  typeName: string,
  desiredState: Record<string, unknown>,
  options?: { skipDefaultsWhenEmpty?: boolean }
): Record<string, unknown> => {
  const template = getResourceTemplate(typeName);
  if (!template) {
    return desiredState;
  }

  if (options?.skipDefaultsWhenEmpty && Object.keys(desiredState).length === 0) {
    return desiredState;
  }

  const withDefaults = template.fields.reduce<Record<string, unknown>>((accumulator, field) => {
    if (Object.prototype.hasOwnProperty.call(desiredState, field.key)) {
      return accumulator;
    }

    if (field.defaultValue === undefined) {
      return accumulator;
    }

    return {
      ...accumulator,
      [field.key]: field.defaultValue
    };
  }, desiredState);

  return withDefaults;
};

const assertTemplateRequiredValues = (typeName: string, desiredState: Record<string, unknown>): void => {
  const template = getResourceTemplate(typeName);
  if (!template) {
    throw createAppError(
      'RESOURCE_TEMPLATE_NOT_FOUND',
      `Template para tipo de recurso ${typeName} nao encontrado.`,
      404
    );
  }

  const missingFields = template.fields
    .filter((field) => field.required)
    .filter((field) => !isPresentTemplateValue(desiredState[field.key]));

  if (missingFields.length > 0) {
    const missingFieldNames = missingFields.map((field) => field.key).join(', ');
    throw createAppError(
      'INVALID_RESOURCE_DATA',
      `Campos obrigatorios ausentes para ${typeName}: ${missingFieldNames}.`,
      422
    );
  }
};

export const createResourceService = ({
  userRepository,
  contextRepository,
  deleteIntentRepository,
  permissionRepository,
  resourceStateRepository,
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

  const persistState = async (input: {
    execution: AwsExecutionContext;
    typeName: string;
    identifier: string;
    operation: ResourceStateAction;
    status: ResourceStateRecord['status'];
    desiredState: Record<string, unknown>;
    patchDocument?: readonly Record<string, unknown>[];
  }) => {
    try {
      await resourceStateRepository.create({
        userId: input.execution.userId,
        accountId: input.execution.account.accountId,
        region: input.execution.region,
        category: input.execution.category,
        typeName: input.typeName,
        identifier: input.identifier,
        operation: input.operation,
        status: input.status,
        desiredState: input.desiredState,
        patchDocument: input.patchDocument,
        createdBy: input.execution.userId
      });
    } catch {
      // Estado e histórico sao observabilidade da operacao.
      // Em caso de falha de persistencia, não bloqueamos a operação principal.
    }
  };

  const writeStateAndRunOperation = async <T>(
    execution: AwsExecutionContext,
    typeName: string,
    desiredState: Record<string, unknown>,
    operation: ResourceStateAction,
    operationFn: () => Promise<T>,
    patchDocument?: readonly Record<string, unknown>[],
    identifierHint?: string
  ): Promise<T> => {
    const requestedIdentifier = normalizeIdentifier(
      resolveIdentifierFromPayload({
        typeName,
        desiredState,
        identifier: identifierHint
      })
    );
    await persistState({
      execution,
      typeName,
      identifier: requestedIdentifier,
      operation,
      status: 'submitted',
      desiredState,
      patchDocument
    });

    try {
      const result = await operationFn();
      const finalIdentifier = normalizeIdentifier(
        parseResourceIdentifierFromOperationResult(result) ?? requestedIdentifier
      );

      await persistState({
        execution,
        typeName,
        identifier: finalIdentifier,
        operation,
        status: 'applied',
        desiredState,
        patchDocument
      });

      return result;
    } catch (error) {
      await persistState({
        execution,
        typeName,
        identifier: requestedIdentifier,
        operation,
        status: 'failed',
        desiredState,
        patchDocument
      });
      throw error;
    }
  };

  const getContextAwareStateHistory = async (userId: string, query: StateHistoryQuery) => {
    const { user, context } = await resolveExecution(userId);
    await assertPermission({
      userId,
      role: user.role,
      accountId: context.accountId,
      category: context.category,
      action: 'list',
      resourceType: query.typeName
    });

    return resourceStateRepository.listByContext({
      accountId: context.accountId,
      region: context.region,
      category: context.category,
      typeName: query.typeName,
      identifier: query.identifier,
      limit: asListStateLimit(query.limit)
    });
  };

  return {
    listTemplates: async () => getResourceTemplates(),
    getTemplateByType: async (typeName: string) => toTemplateField(getResourceTemplate(typeName)),

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

      const details = await resourceGateway.getResourceDetails({ execution, typeName, identifier });
      const platformState = await resourceStateRepository.getLatestByResource({
        accountId: execution.account.accountId,
        region: execution.region,
        category: execution.category,
        typeName,
        identifier
      });

      return {
        ...details,
        platformState
      };
    },

    getResourceStateHistory: async (userId: string, query: StateHistoryQuery) => {
      const history = await getContextAwareStateHistory(userId, query);
      return history;
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

      const desiredState = payload.desiredState ?? {};
      const normalizedDesiredState = buildDesiredStateWithTemplateDefaults(
        payload.typeName,
        desiredState
      );
      assertTemplateDesiredStateConforms(payload.typeName, normalizedDesiredState);
      assertTemplateRequiredValues(payload.typeName, normalizedDesiredState);

      return writeStateAndRunOperation(
        execution,
        payload.typeName,
        normalizedDesiredState,
        'create',
        () =>
          resourceGateway.createResource({
            execution,
            payload: {
              ...payload,
              desiredState: normalizedDesiredState
            }
          }),
        undefined
      );
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

      const desiredState = payload.desiredState ?? {};
      const normalizedDesiredState = buildDesiredStateWithTemplateDefaults(
        payload.typeName,
        desiredState,
        { skipDefaultsWhenEmpty: true }
      );
      assertTemplateDesiredStateConforms(payload.typeName, normalizedDesiredState);
      const patchDocument = payload.patchDocument;

      if (
        Object.keys(normalizedDesiredState).length === 0 &&
        (!patchDocument || patchDocument.length === 0)
      ) {
        throw createAppError(
          'INVALID_RESOURCE_DATA',
          'Informe desiredState ou patchDocument para update.',
          422
        );
      }

      return writeStateAndRunOperation(
        execution,
        payload.typeName,
        normalizedDesiredState,
        'update',
        () =>
          resourceGateway.updateResource({
            execution,
            payload: {
              ...payload,
              desiredState: normalizedDesiredState
            }
          }),
        patchDocument,
        payload.identifier
      );
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

      const result = await writeStateAndRunOperation(
        execution,
        typeName,
        { identifier: resourceId },
        'delete',
        () =>
          resourceGateway.deleteResource({
            execution,
            typeName,
            identifier: resourceId
          }),
        undefined
      );

      await deleteIntentRepository.removeById(intentId);
      return result;
    }
  };
};
