import {
  AwsCategory,
  getResourceUpdateProfile,
  getResourceTemplate,
  getResourceTemplates,
  FinOpsOverview,
  FinOpsResourceSummary,
  ResourceAction,
  ResourceStateAction,
  ResourceSummary,
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

type FinOpsOverviewQuery = {
  staleDays?: number;
};

type FinOpsObservation = {
  resource: ResourceSummary;
  latestState?: ResourceStateRecord;
  isObsolete: boolean;
  lastStateAt: number | null;
  daysWithoutUpdate: number | null;
  obsolescenceReason: string;
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

const toFiniteNonZeroInt = (value: number | undefined, fallback: number): number => {
  if (typeof value !== 'number' || !Number.isFinite(value) || !Number.isInteger(value)) {
    return fallback;
  }

  if (value <= 0) {
    return fallback;
  }

  return value;
};

const toSafeErrorMessage = (error: unknown, fallback: string): string => {
  if (typeof error === 'object' && error !== null && 'message' in error) {
    const candidate = error as { message?: unknown };
    if (typeof candidate.message === 'string' && candidate.message.trim().length > 0) {
      return candidate.message;
    }
  }

  return fallback;
};

const buildResourceStateKey = (typeName: string, identifier: string): string =>
  `${typeName}|${identifier}`;

const toDaysSinceEpoch = (timestamp: number, now: number): number =>
  Math.max(0, Math.floor((now - timestamp) / (24 * 60 * 60 * 1000)));

const getFinopsObsolescence = (input: {
  latestState?: ResourceStateRecord;
  staleDays: number;
  now: number;
}) => {
  if (!input.latestState) {
    return {
      isObsolete: true,
      lastStateAt: null as number | null,
      daysWithoutUpdate: null as number | null,
      obsolescenceReason: 'Nenhum estado de operacao registrado para o recurso.'
    };
  }

  const lastStateAt = input.latestState.createdAt;
  const daysWithoutUpdate = toDaysSinceEpoch(lastStateAt, input.now);

  return {
    isObsolete: daysWithoutUpdate >= input.staleDays,
    lastStateAt,
    daysWithoutUpdate,
    obsolescenceReason:
      daysWithoutUpdate >= input.staleDays
        ? `Ultima atualizacao ha ${daysWithoutUpdate} dias.`
        : 'Recente'
  };
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

const assertFocusedUpdateProfile = (
  typeName: string,
  updateProfileId: string | undefined,
  desiredState: Record<string, unknown>,
  patchDocument?: readonly Record<string, unknown>[]
): void => {
  if (!updateProfileId || updateProfileId.trim().length === 0) {
    throw createAppError(
      'INVALID_RESOURCE_DATA',
      `Informe o updateProfileId para atualizar ${typeName}.`,
      422
    );
  }

  if (patchDocument && patchDocument.length > 0) {
    throw createAppError(
      'INVALID_RESOURCE_DATA',
      'PatchDocument nao e permitido em updates focados.',
      422
    );
  }

  const profile = getResourceUpdateProfile(typeName, updateProfileId);
  if (!profile) {
    throw createAppError(
      'INVALID_RESOURCE_DATA',
      `Perfil de update ${updateProfileId} nao encontrado para ${typeName}.`,
      422
    );
  }

  const allowedFieldKeys = new Set(profile.fieldKeys);
  const desiredStateKeys = Object.keys(desiredState);
  const invalidKeys = desiredStateKeys.filter((fieldKey) => !allowedFieldKeys.has(fieldKey));

  if (invalidKeys.length > 0) {
    throw createAppError(
      'INVALID_RESOURCE_DATA',
      `O update ${profile.label} permite apenas: ${profile.fieldKeys.join(', ')}. Recebido: ${invalidKeys.join(', ')}.`,
      422
    );
  }

  const hasAtLeastOneProfileField = profile.fieldKeys.some((fieldKey) =>
    Object.prototype.hasOwnProperty.call(desiredState, fieldKey)
  );

  if (!hasAtLeastOneProfileField) {
    throw createAppError(
      'INVALID_RESOURCE_DATA',
      `Informe ao menos um valor para o update ${profile.label}.`,
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

  const getAllowedResourceTypesForExecution = async (executionContext: {
    userId: string;
    accountId: string;
    category: AwsCategory;
  }): Promise<readonly string[]> => {
    const user = await userRepository.findById(executionContext.userId);

    if (!user) {
      return [];
    }

    const categoryTypes = getCategoryResourceTypes(executionContext.category);

    if (user.role === 'admin') {
      return categoryTypes;
    }

    const accessChecks = await Promise.all(
      categoryTypes.map(async (typeName) => {
        const allowed = await permissionRepository.isAllowed({
          userId: executionContext.userId,
          accountId: executionContext.accountId,
          category: executionContext.category,
          action: 'list',
          resourceType: typeName
        });

        return allowed ? typeName : null;
      })
    );

    return accessChecks.flatMap((typeName) => (typeName ? [typeName] : []));
  };

  const normalizeFinOpsResourceSummary = (
    observation: FinOpsObservation,
    latestState: ResourceStateRecord | undefined
  ): FinOpsResourceSummary => ({
    ...observation.resource,
    isObsolete: observation.isObsolete,
    lastStateAt: observation.lastStateAt,
    daysWithoutUpdate: observation.daysWithoutUpdate,
    lastStateStatus: latestState?.status ?? null,
    lastStateOperation: latestState?.operation ?? null,
    obsolescenceReason: observation.obsolescenceReason
  });

  return {
    listTemplates: async () => getResourceTemplates(),
    getTemplateByType: async (typeName: string) => toTemplateField(getResourceTemplate(typeName)),

    listTypes: async (userId: string): Promise<readonly string[]> => {
      const { user, context } = await resolveExecution(userId);
      return getAllowedResourceTypesForExecution({
        userId,
        accountId: context.accountId,
        category: context.category
      });
    },

    getFinopsOverview: async (userId: string, query: FinOpsOverviewQuery): Promise<FinOpsOverview> => {
      const { execution } = await resolveExecution(userId);
      const context = {
        accountId: execution.account.accountId,
        region: execution.region,
        category: execution.category
      };

      const staleThresholdDays = toFiniteNonZeroInt(query.staleDays, 30);
      const now = Date.now();
      const allowedTypes = await getAllowedResourceTypesForExecution({
        userId,
        accountId: context.accountId,
        category: context.category
      });

      if (allowedTypes.length === 0) {
        return {
          accountId: context.accountId,
          region: context.region,
          category: context.category,
          staleThresholdDays,
          totalResources: 0,
          obsoleteResources: 0,
          resourcesWithoutState: 0,
          staleRatePercent: 0,
          resourcesByType: {},
          obsoleteByType: {},
          resources: [],
          warnings: []
        };
      }

      const latestStates = await resourceStateRepository.listLatestByContext({
        accountId: context.accountId,
        region: context.region,
        category: context.category
      });

      const latestStateByResource = new Map<string, ResourceStateRecord>(
        latestStates.map((state) => [buildResourceStateKey(state.typeName, state.identifier), state])
      );

      const collectByType = await Promise.all(
        allowedTypes.map(async (typeName) => {
          try {
            const resources = await resourceGateway.listResources({
              execution,
              typeName
            });

            return {
              typeName,
              resources,
              warning: undefined as string | undefined
            };
          } catch (error) {
            return {
              typeName,
              resources: [] as readonly ResourceSummary[],
              warning: toSafeErrorMessage(error, `Nao foi possivel listar ${typeName} neste momento.`)
            };
          }
        })
      );

      const warnings = collectByType.flatMap((entry) => (entry.warning ? [entry.warning] : []));
      const resourcesByType: Record<string, number> = {};
      const obsoleteByType: Record<string, number> = {};
      const observations: FinOpsResourceSummary[] = [];
      let obsoleteResources = 0;
      let resourcesWithoutState = 0;

      collectByType.forEach(({ typeName, resources }) => {
        resourcesByType[typeName] = resources.length;
        obsoleteByType[typeName] = 0;

        resources.forEach((resource) => {
          const latestState = latestStateByResource.get(buildResourceStateKey(typeName, resource.identifier));
          const finopsState = getFinopsObsolescence({
            latestState,
            staleDays: staleThresholdDays,
            now
          });

          const summary = normalizeFinOpsResourceSummary(
            {
              resource,
              latestState,
              isObsolete: finopsState.isObsolete,
              lastStateAt: finopsState.lastStateAt,
              daysWithoutUpdate: finopsState.daysWithoutUpdate,
              obsolescenceReason: finopsState.obsolescenceReason
            },
            latestState
          );

          observations.push(summary);

          if (finopsState.isObsolete) {
            obsoleteResources += 1;
            obsoleteByType[typeName] += 1;
          }

          if (!latestState) {
            resourcesWithoutState += 1;
          }
        });
      });

      const staleRatePercent =
        observations.length === 0
          ? 0
          : Number(((obsoleteResources / observations.length) * 100).toFixed(2));

      return {
        accountId: context.accountId,
        region: context.region,
        category: context.category,
        staleThresholdDays,
        totalResources: observations.length,
        obsoleteResources,
        resourcesWithoutState,
        staleRatePercent,
        resourcesByType,
        obsoleteByType,
        resources: observations,
        warnings: [...new Set(warnings)]
      };
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
      const normalizedDesiredState = desiredState;
      assertTemplateDesiredStateConforms(payload.typeName, normalizedDesiredState);
      assertFocusedUpdateProfile(
        payload.typeName,
        payload.updateProfileId,
        normalizedDesiredState,
        payload.patchDocument
      );
      const patchDocument = payload.patchDocument;

      if (Object.keys(normalizedDesiredState).length === 0) {
        throw createAppError(
          'INVALID_RESOURCE_DATA',
          'Informe dados para o update selecionado.',
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
