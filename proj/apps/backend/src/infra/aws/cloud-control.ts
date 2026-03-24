import {
  CloudControlClient,
  CreateResourceCommand,
  DeleteResourceCommand,
  GetResourceCommand,
  GetResourceRequestStatusCommand,
  ListResourcesCommand,
  UpdateResourceCommand,
  type ProgressEvent,
  type ResourceDescription
} from '@aws-sdk/client-cloudcontrol';
import type {
  AwsAccount,
  AwsCategory,
  CheckupResult,
  ResourceSummary,
  UpsertResourcePayload
} from '@platform/shared';
import { getCategoryResourceTypes } from '../../domain/categories.js';
import { createAppError } from '../../domain/errors.js';
import type { AssumeRoleFn, AwsTemporaryCredentials } from './assume-role.js';

export type AwsExecutionContext = {
  userId: string;
  account: AwsAccount;
  region: string;
  category: AwsCategory;
};

export type ResourceGateway = {
  listResources: (input: {
    execution: AwsExecutionContext;
    typeName?: string;
  }) => Promise<readonly ResourceSummary[]>;
  discoverResources: (input: {
    execution: AwsExecutionContext;
    typeName: string;
    regions: readonly string[];
  }) => Promise<
    readonly {
      region: string;
      status: 'ok' | 'error';
      resources: readonly ResourceSummary[];
      message?: string;
    }[]
  >;
  getResourceDetails: (input: {
    execution: AwsExecutionContext;
    typeName: string;
    identifier: string;
  }) => Promise<Record<string, unknown>>;
  createResource: (input: {
    execution: AwsExecutionContext;
    payload: UpsertResourcePayload;
  }) => Promise<ProgressEvent>;
  updateResource: (input: {
    execution: AwsExecutionContext;
    payload: UpsertResourcePayload;
  }) => Promise<ProgressEvent>;
  deleteResource: (input: {
    execution: AwsExecutionContext;
    typeName: string;
    identifier: string;
  }) => Promise<ProgressEvent>;
  runCategoryCheckup: (execution: AwsExecutionContext) => Promise<CheckupResult>;
};

type CreateGatewayDependencies = {
  assumeRole: AssumeRoleFn;
  createCloudControlClient?: (
    region: string,
    credentials: AwsTemporaryCredentials
  ) => CloudControlClient;
};

const buildCloudControlClient = (
  region: string,
  credentials: AwsTemporaryCredentials
): CloudControlClient =>
  new CloudControlClient({
    region,
    credentials
  });

const delay = (milliseconds: number): Promise<void> =>
  new Promise((resolvePromise) => {
    setTimeout(resolvePromise, milliseconds);
  });

const mapWithConcurrency = async <InputType, OutputType>(
  values: readonly InputType[],
  concurrency: number,
  mapFn: (value: InputType) => Promise<OutputType>
): Promise<readonly OutputType[]> => {
  const safeConcurrency = Math.max(1, concurrency);
  const results: OutputType[] = new Array(values.length);
  let nextIndex = 0;

  const worker = async (): Promise<void> => {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;

      if (currentIndex >= values.length) {
        return;
      }

      results[currentIndex] = await mapFn(values[currentIndex]);
    }
  };

  await Promise.all(Array.from({ length: Math.min(safeConcurrency, values.length) }, worker));
  return results;
};

const parseJsonObject = (value: string | undefined): Record<string, unknown> => {
  if (!value) {
    return {};
  }

  try {
    const parsed = JSON.parse(value) as unknown;
    if (typeof parsed === 'object' && parsed !== null) {
      return parsed as Record<string, unknown>;
    }

    return {};
  } catch {
    return {};
  }
};

const pickDisplayName = (
  properties: Record<string, unknown>,
  identifier: string,
  typeName: string
): string => {
  const candidates = [
    properties.Name,
    properties.BucketName,
    properties.TableName,
    properties.DBInstanceIdentifier,
    properties.DBClusterIdentifier,
    properties.FunctionName,
    properties.ServiceName,
    properties.ClusterName,
    properties.RoleName,
    properties.GroupName,
    properties.SecurityGroupId,
    properties.StackName,
    properties.AlarmName,
    properties.RuleName,
    properties.Id,
    properties.Arn
  ];

  const foundCandidate = candidates.find(
    (candidate) => typeof candidate === 'string' && candidate.trim().length > 0
  );

  if (typeof foundCandidate === 'string') {
    return foundCandidate;
  }

  const identifierParts = identifier.split('/');
  return identifierParts[identifierParts.length - 1] ?? typeName;
};

const asResourceSummary = (
  accountId: string,
  region: string,
  typeName: string,
  description: ResourceDescription
): ResourceSummary => {
  const identifier = description.Identifier ?? 'unknown';
  const properties = parseJsonObject(description.Properties);

  return {
    accountId,
    region,
    typeName,
    identifier,
    displayName: pickDisplayName(properties, identifier, typeName)
  };
};

const isTerminalStatus = (status: string | undefined): boolean =>
  status === 'SUCCESS' || status === 'FAILED' || status === 'CANCEL_COMPLETE';

const isFailureStatus = (status: string | undefined): boolean =>
  status === 'FAILED' || status === 'CANCEL_COMPLETE';

const toPatchDocument = (payload: UpsertResourcePayload): string => {
  if (payload.patchDocument && payload.patchDocument.length > 0) {
    return JSON.stringify(payload.patchDocument);
  }

  const generatedPatch = Object.entries(payload.desiredState).map(([key, value]) => ({
    op: 'replace',
    path: `/${key}`,
    value
  }));

  if (generatedPatch.length === 0) {
    throw createAppError('INVALID_PATCH', 'Patch document vazio para update.', 422);
  }

  return JSON.stringify(generatedPatch);
};

const normalizeGatewayError = (error: unknown, fallbackMessage: string): never => {
  if (typeof error === 'object' && error !== null) {
    const namedError = error as { name?: string; message?: string; $metadata?: unknown };

    if (namedError.name === 'TypeNotFoundException') {
      throw createAppError('RESOURCE_TYPE_NOT_FOUND', namedError.message ?? fallbackMessage, 404, error);
    }

    if (namedError.name === 'AccessDeniedException') {
      throw createAppError('ACCESS_DENIED', namedError.message ?? fallbackMessage, 403, error);
    }

    throw createAppError('AWS_GATEWAY_ERROR', namedError.message ?? fallbackMessage, 502, error);
  }

  throw createAppError('AWS_GATEWAY_ERROR', fallbackMessage, 502, error);
};

const getGatewayErrorMessage = (error: unknown, fallbackMessage: string): string => {
  if (typeof error === 'object' && error !== null) {
    const namedError = error as { message?: string };
    if (typeof namedError.message === 'string' && namedError.message.trim().length > 0) {
      return namedError.message;
    }
  }

  return fallbackMessage;
};

const resolveClient = async (
  dependencies: CreateGatewayDependencies,
  execution: AwsExecutionContext
): Promise<CloudControlClient> => {
  const credentials = await dependencies.assumeRole({
    account: execution.account,
    region: execution.region,
    userId: execution.userId
  });

  const factory = dependencies.createCloudControlClient ?? buildCloudControlClient;
  return factory(execution.region, credentials);
};

const waitForProgressEvent = async (
  client: CloudControlClient,
  initialProgressEvent: ProgressEvent | undefined,
  operationName: string
): Promise<ProgressEvent> => {
  if (!initialProgressEvent) {
    throw createAppError(
      'MISSING_PROGRESS_EVENT',
      `Operacao ${operationName} sem evento de progresso retornado pela AWS.`,
      502
    );
  }

  if (isTerminalStatus(initialProgressEvent.OperationStatus)) {
    if (isFailureStatus(initialProgressEvent.OperationStatus)) {
      throw createAppError(
        'AWS_OPERATION_FAILED',
        initialProgressEvent.StatusMessage ?? `Operacao ${operationName} falhou na AWS.`,
        502,
        initialProgressEvent
      );
    }

    return initialProgressEvent;
  }

  const requestToken = initialProgressEvent.RequestToken;

  if (!requestToken) {
    throw createAppError(
      'MISSING_REQUEST_TOKEN',
      `Operacao ${operationName} nao retornou request token para polling.`,
      502,
      initialProgressEvent
    );
  }

  let attempts = 0;

  while (attempts < 30) {
    await delay(1000);

    const requestStatusOutput = await client.send(
      new GetResourceRequestStatusCommand({
        RequestToken: requestToken
      })
    );

    const progressEvent = requestStatusOutput.ProgressEvent;

    if (progressEvent && isTerminalStatus(progressEvent.OperationStatus)) {
      if (isFailureStatus(progressEvent.OperationStatus)) {
        throw createAppError(
          'AWS_OPERATION_FAILED',
          progressEvent.StatusMessage ?? `Operacao ${operationName} falhou na AWS.`,
          502,
          progressEvent
        );
      }

      return progressEvent;
    }

    attempts += 1;
  }

  throw createAppError(
    'AWS_OPERATION_TIMEOUT',
    `Tempo limite excedido na operacao ${operationName}.`,
    504,
    initialProgressEvent
  );
};

const listResourcesByType = async (
  client: CloudControlClient,
  accountId: string,
  region: string,
  typeName: string
): Promise<readonly ResourceSummary[]> => {
  const resources: ResourceSummary[] = [];
  let nextToken: string | undefined;
  let pageCount = 0;

  do {
    const listOutput = await client.send(
      new ListResourcesCommand({
        TypeName: typeName,
        MaxResults: 50,
        NextToken: nextToken
      })
    );

    const descriptions = listOutput.ResourceDescriptions ?? [];
    const normalizedResources = descriptions.map((description) =>
      asResourceSummary(accountId, region, typeName, description)
    );

    resources.push(...normalizedResources);

    nextToken = listOutput.NextToken;
    pageCount += 1;
  } while (nextToken && pageCount < 10);

  return resources;
};

export const createCloudControlGateway = (
  dependencies: CreateGatewayDependencies
): ResourceGateway => ({
  listResources: async ({ execution, typeName }) => {
    const client = await resolveClient(dependencies, execution);
    const targetResourceTypes = typeName
      ? [typeName]
      : [...getCategoryResourceTypes(execution.category)];

    try {
      const groupedResources = await Promise.all(
        targetResourceTypes.map((currentTypeName) =>
          listResourcesByType(client, execution.account.accountId, execution.region, currentTypeName)
        )
      );

      return groupedResources.flat();
    } catch (error: unknown) {
      return normalizeGatewayError(error, 'Falha ao listar recursos.');
    }
  },

  discoverResources: async ({ execution, typeName, regions }) => {
    const targetRegions = [...new Set(regions.map((region) => region.trim()).filter(Boolean))];
    const orderedRegions = [...targetRegions].sort((a, b) => a.localeCompare(b));

    if (orderedRegions.length === 0) {
      return [];
    }

    const regionDiscovery = await mapWithConcurrency(orderedRegions, 4, async (region) => {
      try {
        const client = await resolveClient(dependencies, {
          ...execution,
          region
        });

        const resources = await listResourcesByType(
          client,
          execution.account.accountId,
          region,
          typeName
        );

        return {
          region,
          status: 'ok' as const,
          resources
        };
      } catch (error: unknown) {
        return {
          region,
          status: 'error' as const,
          resources: [],
          message: getGatewayErrorMessage(error, `Falha ao consultar recursos em ${region}.`)
        };
      }
    });

    return regionDiscovery;
  },

  getResourceDetails: async ({ execution, typeName, identifier }) => {
    const client = await resolveClient(dependencies, execution);

    try {
      const response = await client.send(
        new GetResourceCommand({
          TypeName: typeName,
          Identifier: identifier
        })
      );

      if (!response.ResourceDescription) {
        throw createAppError('RESOURCE_NOT_FOUND', 'Recurso nao encontrado.', 404);
      }

      return {
        identifier: response.ResourceDescription.Identifier,
        typeName,
        properties: parseJsonObject(response.ResourceDescription.Properties)
      };
    } catch (error: unknown) {
      return normalizeGatewayError(error, 'Falha ao obter detalhes do recurso.');
    }
  },

  createResource: async ({ execution, payload }) => {
    const client = await resolveClient(dependencies, execution);

    try {
      const response = await client.send(
        new CreateResourceCommand({
          TypeName: payload.typeName,
          DesiredState: JSON.stringify(payload.desiredState)
        })
      );

      return waitForProgressEvent(client, response.ProgressEvent, 'create');
    } catch (error: unknown) {
      return normalizeGatewayError(error, 'Falha ao criar recurso.');
    }
  },

  updateResource: async ({ execution, payload }) => {
    if (!payload.identifier) {
      throw createAppError('MISSING_IDENTIFIER', 'Identifier obrigatorio para update.', 422);
    }

    const client = await resolveClient(dependencies, execution);

    try {
      const response = await client.send(
        new UpdateResourceCommand({
          TypeName: payload.typeName,
          Identifier: payload.identifier,
          PatchDocument: toPatchDocument(payload)
        })
      );

      return waitForProgressEvent(client, response.ProgressEvent, 'update');
    } catch (error: unknown) {
      return normalizeGatewayError(error, 'Falha ao atualizar recurso.');
    }
  },

  deleteResource: async ({ execution, typeName, identifier }) => {
    const client = await resolveClient(dependencies, execution);

    try {
      const response = await client.send(
        new DeleteResourceCommand({
          TypeName: typeName,
          Identifier: identifier
        })
      );

      return waitForProgressEvent(client, response.ProgressEvent, 'delete');
    } catch (error: unknown) {
      return normalizeGatewayError(error, 'Falha ao remover recurso.');
    }
  },

  runCategoryCheckup: async (execution) => {
    const client = await resolveClient(dependencies, execution);
    const resourceTypes = getCategoryResourceTypes(execution.category);

    const counts = await Promise.all(
      resourceTypes.map(async (typeName) => {
        try {
          const resources = await listResourcesByType(
            client,
            execution.account.accountId,
            execution.region,
            typeName
          );

          return [typeName, resources.length] as const;
        } catch {
          return [typeName, 0] as const;
        }
      })
    );

    const resourceCounts = counts.reduce<Record<string, number>>((accumulator, [typeName, total]) => {
      accumulator[typeName] = total;
      return accumulator;
    }, {});

    return {
      accountId: execution.account.accountId,
      region: execution.region,
      category: execution.category,
      resourceCounts
    } as CheckupResult;
  }
});
