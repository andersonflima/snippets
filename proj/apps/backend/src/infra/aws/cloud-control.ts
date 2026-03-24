import { Agent } from 'node:https';
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
import {
  DescribeDBClustersCommand,
  DescribeDBInstancesCommand,
  RDSClient,
  type DBCluster,
  type DBInstance
} from '@aws-sdk/client-rds';
import type {
  AwsAccount,
  AwsCategory,
  CheckupResult,
  ResourceSummary,
  UpsertResourcePayload
} from '@platform/shared';
import { NodeHttpHandler } from '@smithy/node-http-handler';
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
    credentials?: AwsTemporaryCredentials
  ) => CloudControlClient;
  createRdsClient?: (
    region: string,
    credentials?: AwsTemporaryCredentials
  ) => RDSClient;
  endpoint?: string;
  tlsInsecure?: boolean;
};

const buildCloudControlClient = (
  region: string,
  tlsInsecure: boolean,
  endpoint: string | undefined,
  credentials?: AwsTemporaryCredentials
): CloudControlClient =>
  new CloudControlClient({
    region,
    credentials,
    endpoint,
    requestHandler: tlsInsecure
      ? new NodeHttpHandler({
          httpsAgent: new Agent({
            rejectUnauthorized: false
          })
        })
      : undefined
  });

const buildRdsClient = (
  region: string,
  tlsInsecure: boolean,
  endpoint: string | undefined,
  credentials?: AwsTemporaryCredentials
): RDSClient =>
  new RDSClient({
    region,
    credentials,
    endpoint,
    requestHandler: tlsInsecure
      ? new NodeHttpHandler({
          httpsAgent: new Agent({
            rejectUnauthorized: false
          })
        })
      : undefined
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

const asResourceSummaryFromProperties = (
  accountId: string,
  region: string,
  typeName: string,
  identifier: string,
  properties: Record<string, unknown>
): ResourceSummary => ({
  accountId,
  region,
  typeName,
  identifier,
  displayName: pickDisplayName(properties, identifier, typeName)
});

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

const isUnsupportedLocalstackListOperation = (error: unknown): boolean => {
  if (typeof error !== 'object' || error === null) {
    return false;
  }

  const candidate = error as { message?: unknown };
  return (
    typeof candidate.message === 'string' &&
    candidate.message.includes("The 'List' operation for the CloudFormation resource type") &&
    candidate.message.includes('CloudControl service in LocalStack')
  );
};

type ResolvedAwsClients = {
  cloudControlClient: CloudControlClient;
  credentials?: AwsTemporaryCredentials;
};

const resolveClients = async (
  dependencies: CreateGatewayDependencies,
  execution: AwsExecutionContext
): Promise<ResolvedAwsClients> => {
  const credentials = await dependencies.assumeRole({
    account: execution.account,
    region: execution.region,
    userId: execution.userId
  });

  const factory =
    dependencies.createCloudControlClient ??
    ((region: string, nextCredentials?: AwsTemporaryCredentials) =>
      buildCloudControlClient(
        region,
        dependencies.tlsInsecure ?? false,
        dependencies.endpoint,
        nextCredentials
      ));

  return {
    cloudControlClient: factory(execution.region, credentials),
    credentials
  };
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

const listRdsDbInstances = async (
  accountId: string,
  region: string,
  client: RDSClient
): Promise<readonly ResourceSummary[]> => {
  const resources: ResourceSummary[] = [];
  let marker: string | undefined;
  let pageCount = 0;

  do {
    const output = await client.send(
      new DescribeDBInstancesCommand({
        Marker: marker,
        MaxRecords: 100
      })
    );

    const normalizedResources = (output.DBInstances ?? []).map((instance: DBInstance) => {
      const identifier = instance.DBInstanceIdentifier?.trim() || instance.DBInstanceArn?.trim() || 'unknown';

      return asResourceSummaryFromProperties(accountId, region, 'AWS::RDS::DBInstance', identifier, {
        DBInstanceIdentifier: instance.DBInstanceIdentifier,
        Arn: instance.DBInstanceArn,
        Engine: instance.Engine,
        Status: instance.DBInstanceStatus
      });
    });

    resources.push(...normalizedResources);

    marker = output.Marker;
    pageCount += 1;
  } while (marker && pageCount < 10);

  return resources;
};

const listRdsDbClusters = async (
  accountId: string,
  region: string,
  client: RDSClient
): Promise<readonly ResourceSummary[]> => {
  const resources: ResourceSummary[] = [];
  let marker: string | undefined;
  let pageCount = 0;

  do {
    const output = await client.send(
      new DescribeDBClustersCommand({
        Marker: marker,
        MaxRecords: 100
      })
    );

    const normalizedResources = (output.DBClusters ?? []).map((cluster: DBCluster) => {
      const identifier = cluster.DBClusterIdentifier?.trim() || cluster.DBClusterArn?.trim() || 'unknown';

      return asResourceSummaryFromProperties(accountId, region, 'AWS::RDS::DBCluster', identifier, {
        DBClusterIdentifier: cluster.DBClusterIdentifier,
        Arn: cluster.DBClusterArn,
        Engine: cluster.Engine,
        Status: cluster.Status
      });
    });

    resources.push(...normalizedResources);

    marker = output.Marker;
    pageCount += 1;
  } while (marker && pageCount < 10);

  return resources;
};

const listResourcesByNativeFallback = async (
  dependencies: CreateGatewayDependencies,
  accountId: string,
  region: string,
  typeName: string,
  credentials?: AwsTemporaryCredentials
): Promise<readonly ResourceSummary[] | undefined> => {
  const createRdsClient =
    dependencies.createRdsClient ??
    ((nextRegion: string, nextCredentials?: AwsTemporaryCredentials) =>
      buildRdsClient(nextRegion, dependencies.tlsInsecure ?? false, dependencies.endpoint, nextCredentials));

  switch (typeName) {
    case 'AWS::RDS::DBInstance':
      return listRdsDbInstances(accountId, region, createRdsClient(region, credentials));
    case 'AWS::RDS::DBCluster':
      return listRdsDbClusters(accountId, region, createRdsClient(region, credentials));
    default:
      return undefined;
  }
};

const listResourcesByType = async (
  dependencies: CreateGatewayDependencies,
  client: CloudControlClient,
  accountId: string,
  region: string,
  typeName: string,
  credentials?: AwsTemporaryCredentials
): Promise<readonly ResourceSummary[]> => {
  const resources: ResourceSummary[] = [];
  let nextToken: string | undefined;
  let pageCount = 0;

  try {
    do {
      const listOutput = await client.send(
        new ListResourcesCommand({
          TypeName: typeName,
          MaxResults: 50,
          NextToken: nextToken
        })
      );

      const descriptions = listOutput.ResourceDescriptions ?? [];
      const normalizedResources = descriptions.map((description: ResourceDescription) =>
        asResourceSummary(accountId, region, typeName, description)
      );

      resources.push(...normalizedResources);

      nextToken = listOutput.NextToken;
      pageCount += 1;
    } while (nextToken && pageCount < 10);
  } catch (error: unknown) {
    if (!isUnsupportedLocalstackListOperation(error)) {
      throw error;
    }

    const fallbackResources = await listResourcesByNativeFallback(
      dependencies,
      accountId,
      region,
      typeName,
      credentials
    );

    if (fallbackResources) {
      return fallbackResources;
    }

    throw error;
  }

  return resources;
};

export const createCloudControlGateway = (
  dependencies: CreateGatewayDependencies
): ResourceGateway => ({
  listResources: async ({ execution, typeName }) => {
    const { cloudControlClient, credentials } = await resolveClients(dependencies, execution);
    const targetResourceTypes = typeName
      ? [typeName]
      : [...getCategoryResourceTypes(execution.category)];

    try {
      const groupedResources = await Promise.all(
        targetResourceTypes.map((currentTypeName) =>
          listResourcesByType(
            dependencies,
            cloudControlClient,
            execution.account.accountId,
            execution.region,
            currentTypeName,
            credentials
          )
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
        const { cloudControlClient, credentials } = await resolveClients(dependencies, {
          ...execution,
          region
        });

        const resources = await listResourcesByType(
          dependencies,
          cloudControlClient,
          execution.account.accountId,
          region,
          typeName,
          credentials
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
    const { cloudControlClient } = await resolveClients(dependencies, execution);

    try {
      const response = await cloudControlClient.send(
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
    const { cloudControlClient } = await resolveClients(dependencies, execution);

    try {
      const response = await cloudControlClient.send(
        new CreateResourceCommand({
          TypeName: payload.typeName,
          DesiredState: JSON.stringify(payload.desiredState)
        })
      );

      return waitForProgressEvent(cloudControlClient, response.ProgressEvent, 'create');
    } catch (error: unknown) {
      return normalizeGatewayError(error, 'Falha ao criar recurso.');
    }
  },

  updateResource: async ({ execution, payload }) => {
    if (!payload.identifier) {
      throw createAppError('MISSING_IDENTIFIER', 'Identifier obrigatorio para update.', 422);
    }

    const { cloudControlClient } = await resolveClients(dependencies, execution);

    try {
      const response = await cloudControlClient.send(
        new UpdateResourceCommand({
          TypeName: payload.typeName,
          Identifier: payload.identifier,
          PatchDocument: toPatchDocument(payload)
        })
      );

      return waitForProgressEvent(cloudControlClient, response.ProgressEvent, 'update');
    } catch (error: unknown) {
      return normalizeGatewayError(error, 'Falha ao atualizar recurso.');
    }
  },

  deleteResource: async ({ execution, typeName, identifier }) => {
    const { cloudControlClient } = await resolveClients(dependencies, execution);

    try {
      const response = await cloudControlClient.send(
        new DeleteResourceCommand({
          TypeName: typeName,
          Identifier: identifier
        })
      );

      return waitForProgressEvent(cloudControlClient, response.ProgressEvent, 'delete');
    } catch (error: unknown) {
      return normalizeGatewayError(error, 'Falha ao remover recurso.');
    }
  },

  runCategoryCheckup: async (execution) => {
    const { cloudControlClient, credentials } = await resolveClients(dependencies, execution);
    const resourceTypes = getCategoryResourceTypes(execution.category);

    const counts = await Promise.all(
      resourceTypes.map(async (typeName) => {
        try {
          const resources = await listResourcesByType(
            dependencies,
            cloudControlClient,
            execution.account.accountId,
            execution.region,
            typeName,
            credentials
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
