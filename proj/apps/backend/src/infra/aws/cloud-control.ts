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
  DescribeInstancesCommand,
  EC2Client,
  type Instance as Ec2Instance,
  type Reservation
} from '@aws-sdk/client-ec2';
import {
  DescribeFileSystemsCommand as DescribeEfsFileSystemsCommand,
  EFSClient,
  type FileSystemDescription
} from '@aws-sdk/client-efs';
import {
  DescribeClustersCommand,
  DescribeServicesCommand,
  ECSClient,
  ListClustersCommand,
  ListServicesCommand,
  type Cluster as EcsCluster,
  type Service as EcsService
} from '@aws-sdk/client-ecs';
import {
  DescribeFileSystemsCommand as DescribeFsxFileSystemsCommand,
  FSxClient,
  type FileSystem as FsxFileSystem
} from '@aws-sdk/client-fsx';
import {
  DescribeDBClustersCommand,
  DescribeDBInstancesCommand,
  RDSClient,
  type DBCluster,
  type DBInstance
} from '@aws-sdk/client-rds';
import {
  AbortMultipartUploadCommand,
  CreateBucketCommand,
  DeleteBucketCommand,
  DeleteObjectsCommand,
  GetBucketEncryptionCommand,
  GetBucketVersioningCommand,
  GetPublicAccessBlockCommand,
  HeadBucketCommand,
  ListBucketsCommand,
  ListMultipartUploadsCommand,
  ListObjectsV2Command,
  ListObjectVersionsCommand,
  PutBucketEncryptionCommand,
  PutBucketVersioningCommand,
  PutPublicAccessBlockCommand,
  S3Client,
  type BucketLocationConstraint,
  type BucketVersioningStatus,
  type MFADelete,
  type ObjectIdentifier,
  type PublicAccessBlockConfiguration,
  type ServerSideEncryptionRule
} from '@aws-sdk/client-s3';
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
  createEc2Client?: (
    region: string,
    credentials?: AwsTemporaryCredentials
  ) => EC2Client;
  createEfsClient?: (
    region: string,
    credentials?: AwsTemporaryCredentials
  ) => EFSClient;
  createEcsClient?: (
    region: string,
    credentials?: AwsTemporaryCredentials
  ) => ECSClient;
  createFsxClient?: (
    region: string,
    credentials?: AwsTemporaryCredentials
  ) => FSxClient;
  createS3Client?: (
    region: string,
    credentials?: AwsTemporaryCredentials
  ) => S3Client;
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

const buildEc2Client = (
  region: string,
  tlsInsecure: boolean,
  endpoint: string | undefined,
  credentials?: AwsTemporaryCredentials
): EC2Client =>
  new EC2Client({
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

const buildEfsClient = (
  region: string,
  tlsInsecure: boolean,
  endpoint: string | undefined,
  credentials?: AwsTemporaryCredentials
): EFSClient =>
  new EFSClient({
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

const buildEcsClient = (
  region: string,
  tlsInsecure: boolean,
  endpoint: string | undefined,
  credentials?: AwsTemporaryCredentials
): ECSClient =>
  new ECSClient({
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

const buildFsxClient = (
  region: string,
  tlsInsecure: boolean,
  endpoint: string | undefined,
  credentials?: AwsTemporaryCredentials
): FSxClient =>
  new FSxClient({
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

const buildS3Client = (
  region: string,
  tlsInsecure: boolean,
  endpoint: string | undefined,
  credentials?: AwsTemporaryCredentials
): S3Client =>
  new S3Client({
    region,
    credentials,
    endpoint,
    forcePathStyle: typeof endpoint === 'string' && endpoint.length > 0,
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
    properties.CreationToken,
    properties.FileSystemId,
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

    if (isUnsupportedLocalstackCloudControlOperation(error)) {
      throw createAppError(
        'AWS_GATEWAY_ERROR',
        'LocalStack nao suporta esta operacao via Cloud Control. Para create/update/delete, use uma conta AWS real ou implemente fallback nativo do servico no backend.',
        501,
        error
      );
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
  if (typeof candidate.message !== 'string') {
    return false;
  }

  const normalizedMessage = candidate.message.toLowerCase();

  return (
    (
      candidate.message.includes("The 'List' operation for the CloudFormation resource type") &&
      candidate.message.includes('CloudControl service in LocalStack')
    ) ||
    (
      normalizedMessage.includes('cloudcontrol.listresources') &&
      normalizedMessage.includes("'nonetype' object has no attribute 'list'")
    )
  );
};

const isUnsupportedLocalstackCloudControlOperation = (error: unknown): boolean => {
  if (typeof error !== 'object' || error === null) {
    return false;
  }

  const candidate = error as { message?: unknown };
  if (typeof candidate.message !== 'string') {
    return false;
  }

  const normalizedMessage = candidate.message.toLowerCase();

  return (
    normalizedMessage.includes('localstack') &&
    normalizedMessage.includes('cloudcontrol service') &&
    (
      normalizedMessage.includes('not currently supported') ||
      normalizedMessage.includes('not yet supported')
    )
  );
};

const isUnsupportedLocalstackServiceOperation = (error: unknown): boolean => {
  if (typeof error !== 'object' || error === null) {
    return false;
  }

  const candidate = error as { message?: unknown };
  if (typeof candidate.message !== 'string') {
    return false;
  }

  const normalizedMessage = candidate.message.toLowerCase();

  return normalizedMessage.includes('not currently supported by localstack');
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

const getEc2DisplayName = (instance: Ec2Instance): string | undefined =>
  instance.Tags?.find(
    (tag) => tag.Key?.trim().toLowerCase() === 'name' && typeof tag.Value === 'string' && tag.Value.trim().length > 0
  )?.Value;

const mapEc2InstanceToSummary = (
  accountId: string,
  region: string,
  instance: Ec2Instance
): ResourceSummary => {
  const identifier = instance.InstanceId?.trim() || 'unknown';

  return asResourceSummaryFromProperties(accountId, region, 'AWS::EC2::Instance', identifier, {
    Name: getEc2DisplayName(instance),
    InstanceId: instance.InstanceId,
    InstanceType: instance.InstanceType,
    ImageId: instance.ImageId,
    PrivateIpAddress: instance.PrivateIpAddress,
    PublicIpAddress: instance.PublicIpAddress,
    State: instance.State?.Name
  });
};

const listEc2Instances = async (
  accountId: string,
  region: string,
  client: EC2Client
): Promise<readonly ResourceSummary[]> => {
  const resources: ResourceSummary[] = [];
  let nextToken: string | undefined;
  let pageCount = 0;

  do {
    const output = await client.send(
      new DescribeInstancesCommand({
        NextToken: nextToken,
        MaxResults: 100
      })
    );

    const normalizedResources = (output.Reservations ?? [])
      .flatMap((reservation: Reservation) => reservation.Instances ?? [])
      .map((instance: Ec2Instance) => mapEc2InstanceToSummary(accountId, region, instance));

    resources.push(...normalizedResources);

    nextToken = output.NextToken;
    pageCount += 1;
  } while (nextToken && pageCount < 10);

  return resources;
};

const mapEfsFileSystemToSummary = (
  accountId: string,
  region: string,
  fileSystem: FileSystemDescription
): ResourceSummary => {
  const identifier =
    fileSystem.FileSystemId?.trim() || fileSystem.FileSystemArn?.trim() || 'unknown';

  return asResourceSummaryFromProperties(accountId, region, 'AWS::EFS::FileSystem', identifier, {
    FileSystemId: fileSystem.FileSystemId,
    Arn: fileSystem.FileSystemArn,
    CreationToken: fileSystem.CreationToken,
    Name: fileSystem.Name,
    LifeCycleState: fileSystem.LifeCycleState,
    Encrypted: fileSystem.Encrypted,
    PerformanceMode: fileSystem.PerformanceMode,
    ThroughputMode: fileSystem.ThroughputMode
  });
};

const listEfsFileSystems = async (
  accountId: string,
  region: string,
  client: EFSClient
): Promise<readonly ResourceSummary[]> => {
  const output = await client.send(new DescribeEfsFileSystemsCommand({}));

  return (output.FileSystems ?? []).map((fileSystem) =>
    mapEfsFileSystemToSummary(accountId, region, fileSystem)
  );
};

const mapEcsClusterToSummary = (
  accountId: string,
  region: string,
  cluster: EcsCluster
): ResourceSummary => {
  const identifier = cluster.clusterArn?.trim() || cluster.clusterName?.trim() || 'unknown';

  return asResourceSummaryFromProperties(accountId, region, 'AWS::ECS::Cluster', identifier, {
    ClusterName: cluster.clusterName,
    Arn: cluster.clusterArn,
    Status: cluster.status,
    ActiveServicesCount: cluster.activeServicesCount,
    RunningTasksCount: cluster.runningTasksCount,
    PendingTasksCount: cluster.pendingTasksCount,
    RegisteredContainerInstancesCount: cluster.registeredContainerInstancesCount
  });
};

const listEcsClusters = async (
  accountId: string,
  region: string,
  client: ECSClient
): Promise<readonly ResourceSummary[]> => {
  const clusterIdentifiers: string[] = [];
  let nextToken: string | undefined;
  let pageCount = 0;

  do {
    const output = await client.send(
      new ListClustersCommand({
        nextToken,
        maxResults: 100
      })
    );

    clusterIdentifiers.push(
      ...(output.clusterArns ?? []).filter(
        (clusterArn): clusterArn is string =>
          typeof clusterArn === 'string' && clusterArn.trim().length > 0
      )
    );

    nextToken = output.nextToken;
    pageCount += 1;
  } while (nextToken && pageCount < 10);

  if (clusterIdentifiers.length === 0) {
    return [];
  }

  const clusterBatches = chunkValues(clusterIdentifiers, 100);
  const describedClusters = await mapWithConcurrency(clusterBatches, 4, async (clusterBatch) => {
    const output = await client.send(
      new DescribeClustersCommand({
        clusters: [...clusterBatch]
      })
    );

    return output.clusters ?? [];
  });

  return describedClusters
    .flat()
    .map((cluster) => mapEcsClusterToSummary(accountId, region, cluster));
};

const mapEcsServiceToSummary = (
  accountId: string,
  region: string,
  service: EcsService
): ResourceSummary => {
  const identifier = service.serviceArn?.trim() || service.serviceName?.trim() || 'unknown';

  return asResourceSummaryFromProperties(accountId, region, 'AWS::ECS::Service', identifier, {
    ServiceName: service.serviceName,
    Arn: service.serviceArn,
    ClusterArn: service.clusterArn,
    Status: service.status,
    DesiredCount: service.desiredCount,
    RunningCount: service.runningCount,
    PendingCount: service.pendingCount,
    TaskDefinition: service.taskDefinition,
    LaunchType: service.launchType
  });
};

const listEcsServices = async (
  accountId: string,
  region: string,
  client: ECSClient
): Promise<readonly ResourceSummary[]> => {
  const clusterIdentifiers = await listEcsClusters(accountId, region, client);

  if (clusterIdentifiers.length === 0) {
    return [];
  }

  const perClusterServices = await mapWithConcurrency(clusterIdentifiers, 4, async (clusterSummary) => {
    const serviceIdentifiers: string[] = [];
    let nextToken: string | undefined;
    let pageCount = 0;

    do {
      const output = await client.send(
        new ListServicesCommand({
          cluster: clusterSummary.identifier,
          nextToken,
          maxResults: 100
        })
      );

      serviceIdentifiers.push(
        ...(output.serviceArns ?? []).filter(
          (serviceArn): serviceArn is string =>
            typeof serviceArn === 'string' && serviceArn.trim().length > 0
        )
      );

      nextToken = output.nextToken;
      pageCount += 1;
    } while (nextToken && pageCount < 10);

    if (serviceIdentifiers.length === 0) {
      return [];
    }

    const serviceBatches = chunkValues(serviceIdentifiers, 10);
    const describedServices = await mapWithConcurrency(serviceBatches, 4, async (serviceBatch) => {
      const output = await client.send(
        new DescribeServicesCommand({
          cluster: clusterSummary.identifier,
          services: [...serviceBatch]
        })
      );

      return output.services ?? [];
    });

    return describedServices
      .flat()
      .map((service) => mapEcsServiceToSummary(accountId, region, service));
  });

  return perClusterServices.flat();
};

const mapFsxFileSystemToSummary = (
  accountId: string,
  region: string,
  fileSystem: FsxFileSystem
): ResourceSummary => {
  const identifier =
    fileSystem.FileSystemId?.trim() || fileSystem.ResourceARN?.trim() || 'unknown';

  return asResourceSummaryFromProperties(accountId, region, 'AWS::FSx::FileSystem', identifier, {
    FileSystemId: fileSystem.FileSystemId,
    Arn: fileSystem.ResourceARN,
    FileSystemType: fileSystem.FileSystemType,
    Lifecycle: fileSystem.Lifecycle,
    StorageCapacity: fileSystem.StorageCapacity
  });
};

const listFsxFileSystems = async (
  accountId: string,
  region: string,
  client: FSxClient
): Promise<readonly ResourceSummary[]> => {
  try {
    const output = await client.send(new DescribeFsxFileSystemsCommand({}));

    return (output.FileSystems ?? []).map((fileSystem) =>
      mapFsxFileSystemToSummary(accountId, region, fileSystem)
    );
  } catch (error: unknown) {
    if (isUnsupportedLocalstackServiceOperation(error)) {
      return [];
    }

    throw error;
  }
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const asNativeSuccessProgressEvent = (
  typeName: string,
  identifier: string,
  operation: 'CREATE' | 'UPDATE' | 'DELETE'
): ProgressEvent =>
  ({
    TypeName: typeName,
    Identifier: identifier,
    Operation: operation,
    OperationStatus: 'SUCCESS',
    StatusMessage: `Operacao ${operation.toLowerCase()} concluida por fallback nativo do servico.`
  }) as ProgressEvent;

const resolveS3BucketName = (payload: UpsertResourcePayload): string => {
  const fromDesiredState = payload.desiredState.BucketName;
  if (typeof fromDesiredState === 'string' && fromDesiredState.trim().length > 0) {
    return fromDesiredState.trim();
  }

  if (typeof payload.identifier === 'string' && payload.identifier.trim().length > 0) {
    return payload.identifier.trim();
  }

  throw createAppError(
    'INVALID_RESOURCE_DATA',
    'BucketName obrigatorio para AWS::S3::Bucket.',
    422
  );
};

const resolveS3BucketVersioningConfiguration = (
  value: unknown
): { Status?: BucketVersioningStatus; MFADelete?: MFADelete } | undefined => {
  if (!isRecord(value)) {
    return undefined;
  }

  const status =
    value.Status === 'Enabled' || value.Status === 'Suspended'
      ? value.Status
      : undefined;
  const mfaDelete =
    value.MFADelete === 'Enabled' || value.MFADelete === 'Disabled'
      ? value.MFADelete
      : undefined;

  if (!status && !mfaDelete) {
    return undefined;
  }

  return {
    ...(status ? { Status: status } : {}),
    ...(mfaDelete ? { MFADelete: mfaDelete } : {})
  };
};

const resolveS3PublicAccessBlockConfiguration = (
  value: unknown
): PublicAccessBlockConfiguration | undefined => {
  if (!isRecord(value)) {
    return undefined;
  }

  const blockPublicAcls =
    typeof value.BlockPublicAcls === 'boolean' ? value.BlockPublicAcls : undefined;
  const ignorePublicAcls =
    typeof value.IgnorePublicAcls === 'boolean' ? value.IgnorePublicAcls : undefined;
  const blockPublicPolicy =
    typeof value.BlockPublicPolicy === 'boolean' ? value.BlockPublicPolicy : undefined;
  const restrictPublicBuckets =
    typeof value.RestrictPublicBuckets === 'boolean' ? value.RestrictPublicBuckets : undefined;

  if (
    blockPublicAcls === undefined &&
    ignorePublicAcls === undefined &&
    blockPublicPolicy === undefined &&
    restrictPublicBuckets === undefined
  ) {
    return undefined;
  }

  return {
    ...(blockPublicAcls !== undefined ? { BlockPublicAcls: blockPublicAcls } : {}),
    ...(ignorePublicAcls !== undefined ? { IgnorePublicAcls: ignorePublicAcls } : {}),
    ...(blockPublicPolicy !== undefined ? { BlockPublicPolicy: blockPublicPolicy } : {}),
    ...(restrictPublicBuckets !== undefined ? { RestrictPublicBuckets: restrictPublicBuckets } : {})
  };
};

const resolveS3BucketEncryptionConfiguration = (
  value: unknown
): { Rules: ServerSideEncryptionRule[] } | undefined => {
  if (!isRecord(value) || !Array.isArray(value.ServerSideEncryptionConfiguration)) {
    return undefined;
  }

  const rules = value.ServerSideEncryptionConfiguration.filter(isRecord);
  if (rules.length === 0) {
    return undefined;
  }

  return {
    Rules: rules as ServerSideEncryptionRule[]
  };
};

const configureS3Bucket = async (
  client: S3Client,
  bucketName: string,
  desiredState: Record<string, unknown>
): Promise<void> => {
  const versioningConfiguration = resolveS3BucketVersioningConfiguration(
    desiredState.VersioningConfiguration
  );
  const publicAccessBlockConfiguration = resolveS3PublicAccessBlockConfiguration(
    desiredState.PublicAccessBlockConfiguration
  );
  const bucketEncryptionConfiguration = resolveS3BucketEncryptionConfiguration(
    desiredState.BucketEncryption
  );

  if (versioningConfiguration) {
    await client.send(
      new PutBucketVersioningCommand({
        Bucket: bucketName,
        VersioningConfiguration: versioningConfiguration
      })
    );
  }

  if (publicAccessBlockConfiguration) {
    await client.send(
      new PutPublicAccessBlockCommand({
        Bucket: bucketName,
        PublicAccessBlockConfiguration: publicAccessBlockConfiguration
      })
    );
  }

  if (bucketEncryptionConfiguration) {
    await client.send(
      new PutBucketEncryptionCommand({
        Bucket: bucketName,
        ServerSideEncryptionConfiguration: bucketEncryptionConfiguration
      })
    );
  }
};

const listS3Buckets = async (
  accountId: string,
  region: string,
  client: S3Client
): Promise<readonly ResourceSummary[]> => {
  const output = await client.send(new ListBucketsCommand({}));

  return (output.Buckets ?? []).map((bucket) =>
    asResourceSummaryFromProperties(
      accountId,
      region,
      'AWS::S3::Bucket',
      bucket.Name?.trim() || 'unknown',
      {
        BucketName: bucket.Name,
        CreationDate: bucket.CreationDate?.toISOString()
      }
    )
  );
};

const assertS3BucketExists = async (client: S3Client, bucketName: string): Promise<void> => {
  try {
    await client.send(
      new HeadBucketCommand({
        Bucket: bucketName
      })
    );
  } catch (error: unknown) {
    if (typeof error === 'object' && error !== null) {
      const namedError = error as { $metadata?: { httpStatusCode?: number } };
      if (namedError.$metadata?.httpStatusCode === 404) {
        throw createAppError('RESOURCE_NOT_FOUND', 'Bucket S3 nao encontrado.', 404, error);
      }
    }

    throw error;
  }
};

const resolveOptionalS3Configuration = async <OutputType>(
  operation: () => Promise<OutputType>
): Promise<OutputType | undefined> => {
  try {
    return await operation();
  } catch (error: unknown) {
    if (typeof error === 'object' && error !== null) {
      const namedError = error as {
        name?: string;
        Code?: string;
        $metadata?: { httpStatusCode?: number };
      };

      if (
        namedError.$metadata?.httpStatusCode === 404 ||
        namedError.name === 'NoSuchPublicAccessBlockConfiguration' ||
        namedError.name === 'ServerSideEncryptionConfigurationNotFoundError' ||
        namedError.name === 'NotFound'
      ) {
        return undefined;
      }
    }

    return undefined;
  }
};

const getS3BucketDetails = async (
  client: S3Client,
  bucketName: string
): Promise<Record<string, unknown>> => {
  await assertS3BucketExists(client, bucketName);

  const [versioning, publicAccessBlock, encryption] = await Promise.all([
    resolveOptionalS3Configuration(() =>
      client.send(
        new GetBucketVersioningCommand({
          Bucket: bucketName
        })
      )
    ),
    resolveOptionalS3Configuration(() =>
      client.send(
        new GetPublicAccessBlockCommand({
          Bucket: bucketName
        })
      )
    ),
    resolveOptionalS3Configuration(() =>
      client.send(
        new GetBucketEncryptionCommand({
          Bucket: bucketName
        })
      )
    )
  ]);

  return {
    BucketName: bucketName,
    ...(versioning && (versioning.Status || versioning.MFADelete)
      ? {
          VersioningConfiguration: {
            ...(versioning.Status ? { Status: versioning.Status } : {}),
            ...(versioning.MFADelete ? { MFADelete: versioning.MFADelete } : {})
          }
        }
      : {}),
    ...(publicAccessBlock?.PublicAccessBlockConfiguration
      ? {
          PublicAccessBlockConfiguration: publicAccessBlock.PublicAccessBlockConfiguration
        }
      : {}),
    ...(encryption?.ServerSideEncryptionConfiguration?.Rules
      ? {
          BucketEncryption: {
            ServerSideEncryptionConfiguration:
              encryption.ServerSideEncryptionConfiguration.Rules
          }
        }
      : {})
  };
};

const chunkValues = <ValueType>(
  values: readonly ValueType[],
  size: number
): readonly (readonly ValueType[])[] => {
  const safeSize = Math.max(1, size);

  return Array.from({ length: Math.ceil(values.length / safeSize) }, (_value, index) =>
    values.slice(index * safeSize, index * safeSize + safeSize)
  );
};

const mapS3ObjectIdentifiers = (
  entries: readonly {
    Key?: string;
    VersionId?: string;
  }[]
): readonly ObjectIdentifier[] =>
  entries.flatMap((entry) =>
    typeof entry.Key === 'string' && entry.Key.trim().length > 0
      ? [
          {
            Key: entry.Key,
            ...(typeof entry.VersionId === 'string' && entry.VersionId.trim().length > 0
              ? { VersionId: entry.VersionId }
              : {})
          }
        ]
      : []
  );

const deleteS3ObjectsInChunks = async (
  client: S3Client,
  bucketName: string,
  objects: readonly ObjectIdentifier[]
): Promise<void> => {
  const objectChunks = chunkValues(objects, 1000);

  await mapWithConcurrency(objectChunks, 1, async (chunk) => {
    if (chunk.length === 0) {
      return;
    }

    await client.send(
      new DeleteObjectsCommand({
        Bucket: bucketName,
        Delete: {
          Objects: [...chunk],
          Quiet: true
        }
      })
    );
  });
};

const emptyUnversionedS3Bucket = async (
  client: S3Client,
  bucketName: string
): Promise<void> => {
  let continuationToken: string | undefined;

  while (true) {
    const output = await client.send(
      new ListObjectsV2Command({
        Bucket: bucketName,
        ContinuationToken: continuationToken,
        MaxKeys: 1000
      })
    );

    const objects = mapS3ObjectIdentifiers(output.Contents ?? []);
    await deleteS3ObjectsInChunks(client, bucketName, objects);

    if (!output.IsTruncated) {
      return;
    }

    continuationToken = output.NextContinuationToken;
  }
};

const emptyVersionedS3Bucket = async (
  client: S3Client,
  bucketName: string
): Promise<void> => {
  let keyMarker: string | undefined;
  let versionIdMarker: string | undefined;

  while (true) {
    const output = await client.send(
      new ListObjectVersionsCommand({
        Bucket: bucketName,
        KeyMarker: keyMarker,
        VersionIdMarker: versionIdMarker,
        MaxKeys: 1000
      })
    );

    const objects = mapS3ObjectIdentifiers([
      ...(output.Versions ?? []),
      ...(output.DeleteMarkers ?? [])
    ]);

    await deleteS3ObjectsInChunks(client, bucketName, objects);

    if (!output.IsTruncated) {
      return;
    }

    keyMarker = output.NextKeyMarker;
    versionIdMarker = output.NextVersionIdMarker;
  }
};

const abortS3MultipartUploads = async (
  client: S3Client,
  bucketName: string
): Promise<void> => {
  let keyMarker: string | undefined;
  let uploadIdMarker: string | undefined;

  while (true) {
    const output = await client.send(
      new ListMultipartUploadsCommand({
        Bucket: bucketName,
        KeyMarker: keyMarker,
        UploadIdMarker: uploadIdMarker,
        MaxUploads: 1000
      })
    );

    const uploads = (output.Uploads ?? []).filter(
      (entry): entry is { Key: string; UploadId: string } =>
        typeof entry.Key === 'string' &&
        entry.Key.trim().length > 0 &&
        typeof entry.UploadId === 'string' &&
        entry.UploadId.trim().length > 0
    );

    await mapWithConcurrency(uploads, 5, (entry) =>
      client.send(
        new AbortMultipartUploadCommand({
          Bucket: bucketName,
          Key: entry.Key,
          UploadId: entry.UploadId
        })
      )
    );

    if (!output.IsTruncated) {
      return;
    }

    keyMarker = output.NextKeyMarker;
    uploadIdMarker = output.NextUploadIdMarker;
  }
};

const emptyS3Bucket = async (client: S3Client, bucketName: string): Promise<void> => {
  const versioning = await resolveOptionalS3Configuration(() =>
    client.send(
      new GetBucketVersioningCommand({
        Bucket: bucketName
      })
    )
  );

  if (versioning?.Status === 'Enabled' || versioning?.Status === 'Suspended') {
    await emptyVersionedS3Bucket(client, bucketName);
  } else {
    await emptyUnversionedS3Bucket(client, bucketName);
  }

  await abortS3MultipartUploads(client, bucketName);
};

const createS3Bucket = async (
  client: S3Client,
  region: string,
  payload: UpsertResourcePayload
): Promise<ProgressEvent> => {
  const bucketName = resolveS3BucketName(payload);

  await client.send(
    new CreateBucketCommand({
      Bucket: bucketName,
      ...(region !== 'us-east-1'
        ? {
            CreateBucketConfiguration: {
              LocationConstraint: region as BucketLocationConstraint
            }
          }
        : {})
    })
  );

  await configureS3Bucket(client, bucketName, payload.desiredState);

  return asNativeSuccessProgressEvent(payload.typeName, bucketName, 'CREATE');
};

const updateS3Bucket = async (
  client: S3Client,
  payload: UpsertResourcePayload
): Promise<ProgressEvent> => {
  const bucketName = resolveS3BucketName(payload);

  if (
    typeof payload.identifier === 'string' &&
    payload.identifier.trim().length > 0 &&
    payload.identifier.trim() !== bucketName
  ) {
    throw createAppError(
      'INVALID_RESOURCE_DATA',
      'Renomear bucket S3 nao e suportado.',
      422
    );
  }

  await assertS3BucketExists(client, bucketName);
  await configureS3Bucket(client, bucketName, payload.desiredState);

  return asNativeSuccessProgressEvent(payload.typeName, bucketName, 'UPDATE');
};

const deleteS3Bucket = async (
  client: S3Client,
  identifier: string
): Promise<ProgressEvent> => {
  await assertS3BucketExists(client, identifier);
  await emptyS3Bucket(client, identifier);
  await client.send(
    new DeleteBucketCommand({
      Bucket: identifier
    })
  );

  return asNativeSuccessProgressEvent('AWS::S3::Bucket', identifier, 'DELETE');
};

const createResourceByNativeFallback = async (
  dependencies: CreateGatewayDependencies,
  region: string,
  payload: UpsertResourcePayload,
  credentials?: AwsTemporaryCredentials
): Promise<ProgressEvent | undefined> => {
  const createS3Client =
    dependencies.createS3Client ??
    ((nextRegion: string, nextCredentials?: AwsTemporaryCredentials) =>
      buildS3Client(nextRegion, dependencies.tlsInsecure ?? false, dependencies.endpoint, nextCredentials));

  switch (payload.typeName) {
    case 'AWS::S3::Bucket':
      return createS3Bucket(createS3Client(region, credentials), region, payload);
    default:
      return undefined;
  }
};

const updateResourceByNativeFallback = async (
  dependencies: CreateGatewayDependencies,
  region: string,
  payload: UpsertResourcePayload,
  credentials?: AwsTemporaryCredentials
): Promise<ProgressEvent | undefined> => {
  const createS3Client =
    dependencies.createS3Client ??
    ((nextRegion: string, nextCredentials?: AwsTemporaryCredentials) =>
      buildS3Client(nextRegion, dependencies.tlsInsecure ?? false, dependencies.endpoint, nextCredentials));

  switch (payload.typeName) {
    case 'AWS::S3::Bucket':
      return updateS3Bucket(createS3Client(region, credentials), payload);
    default:
      return undefined;
  }
};

const deleteResourceByNativeFallback = async (
  dependencies: CreateGatewayDependencies,
  region: string,
  typeName: string,
  identifier: string,
  credentials?: AwsTemporaryCredentials
): Promise<ProgressEvent | undefined> => {
  const createS3Client =
    dependencies.createS3Client ??
    ((nextRegion: string, nextCredentials?: AwsTemporaryCredentials) =>
      buildS3Client(nextRegion, dependencies.tlsInsecure ?? false, dependencies.endpoint, nextCredentials));

  switch (typeName) {
    case 'AWS::S3::Bucket':
      return deleteS3Bucket(createS3Client(region, credentials), identifier);
    default:
      return undefined;
  }
};

const getResourceDetailsByNativeFallback = async (
  dependencies: CreateGatewayDependencies,
  region: string,
  typeName: string,
  identifier: string,
  credentials?: AwsTemporaryCredentials
): Promise<Record<string, unknown> | undefined> => {
  const createEfsClient =
    dependencies.createEfsClient ??
    ((nextRegion: string, nextCredentials?: AwsTemporaryCredentials) =>
      buildEfsClient(nextRegion, dependencies.tlsInsecure ?? false, dependencies.endpoint, nextCredentials));
  const createEcsClient =
    dependencies.createEcsClient ??
    ((nextRegion: string, nextCredentials?: AwsTemporaryCredentials) =>
      buildEcsClient(nextRegion, dependencies.tlsInsecure ?? false, dependencies.endpoint, nextCredentials));
  const createFsxClient =
    dependencies.createFsxClient ??
    ((nextRegion: string, nextCredentials?: AwsTemporaryCredentials) =>
      buildFsxClient(nextRegion, dependencies.tlsInsecure ?? false, dependencies.endpoint, nextCredentials));
  const createS3Client =
    dependencies.createS3Client ??
    ((nextRegion: string, nextCredentials?: AwsTemporaryCredentials) =>
      buildS3Client(nextRegion, dependencies.tlsInsecure ?? false, dependencies.endpoint, nextCredentials));

  switch (typeName) {
    case 'AWS::EFS::FileSystem': {
      const output = await createEfsClient(region, credentials).send(
        new DescribeEfsFileSystemsCommand({
          FileSystemId: identifier
        })
      );
      const fileSystem = output.FileSystems?.[0];

      if (!fileSystem) {
        throw createAppError('RESOURCE_NOT_FOUND', 'File system EFS nao encontrado.', 404);
      }

      return {
        FileSystemId: fileSystem.FileSystemId,
        Arn: fileSystem.FileSystemArn,
        CreationToken: fileSystem.CreationToken,
        Name: fileSystem.Name,
        LifeCycleState: fileSystem.LifeCycleState,
        Encrypted: fileSystem.Encrypted,
        PerformanceMode: fileSystem.PerformanceMode,
        ThroughputMode: fileSystem.ThroughputMode
      };
    }
    case 'AWS::ECS::Cluster': {
      const output = await createEcsClient(region, credentials).send(
        new DescribeClustersCommand({
          clusters: [identifier]
        })
      );
      const cluster = output.clusters?.[0];

      if (!cluster) {
        throw createAppError('RESOURCE_NOT_FOUND', 'Cluster ECS nao encontrado.', 404);
      }

      return {
        ClusterName: cluster.clusterName,
        Arn: cluster.clusterArn,
        Status: cluster.status,
        ActiveServicesCount: cluster.activeServicesCount,
        RunningTasksCount: cluster.runningTasksCount,
        PendingTasksCount: cluster.pendingTasksCount,
        RegisteredContainerInstancesCount: cluster.registeredContainerInstancesCount
      };
    }
    case 'AWS::ECS::Service': {
      const clusterIdentifier = identifier.split('/');
      const clusterName = clusterIdentifier.length >= 2 ? clusterIdentifier[clusterIdentifier.length - 2] : undefined;

      if (!clusterName) {
        throw createAppError(
          'INVALID_RESOURCE_DATA',
          'Nao foi possivel determinar o cluster do servico ECS.',
          422
        );
      }

      const output = await createEcsClient(region, credentials).send(
        new DescribeServicesCommand({
          cluster: clusterName,
          services: [identifier]
        })
      );
      const service = output.services?.[0];

      if (!service) {
        throw createAppError('RESOURCE_NOT_FOUND', 'Servico ECS nao encontrado.', 404);
      }

      return {
        ServiceName: service.serviceName,
        Arn: service.serviceArn,
        ClusterArn: service.clusterArn,
        Status: service.status,
        DesiredCount: service.desiredCount,
        RunningCount: service.runningCount,
        PendingCount: service.pendingCount,
        TaskDefinition: service.taskDefinition,
        LaunchType: service.launchType
      };
    }
    case 'AWS::FSx::FileSystem': {
      try {
        const output = await createFsxClient(region, credentials).send(
          new DescribeFsxFileSystemsCommand({
            FileSystemIds: [identifier]
          })
        );
        const fileSystem = output.FileSystems?.[0];

        if (!fileSystem) {
          throw createAppError('RESOURCE_NOT_FOUND', 'File system FSx nao encontrado.', 404);
        }

        return {
          FileSystemId: fileSystem.FileSystemId,
          Arn: fileSystem.ResourceARN,
          FileSystemType: fileSystem.FileSystemType,
          Lifecycle: fileSystem.Lifecycle,
          StorageCapacity: fileSystem.StorageCapacity
        };
      } catch (error: unknown) {
        if (isUnsupportedLocalstackServiceOperation(error)) {
          return undefined;
        }

        throw error;
      }
    }
    case 'AWS::S3::Bucket':
      return getS3BucketDetails(createS3Client(region, credentials), identifier);
    default:
      return undefined;
  }
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
  const createEc2Client =
    dependencies.createEc2Client ??
    ((nextRegion: string, nextCredentials?: AwsTemporaryCredentials) =>
      buildEc2Client(nextRegion, dependencies.tlsInsecure ?? false, dependencies.endpoint, nextCredentials));
  const createEfsClient =
    dependencies.createEfsClient ??
    ((nextRegion: string, nextCredentials?: AwsTemporaryCredentials) =>
      buildEfsClient(nextRegion, dependencies.tlsInsecure ?? false, dependencies.endpoint, nextCredentials));
  const createEcsClient =
    dependencies.createEcsClient ??
    ((nextRegion: string, nextCredentials?: AwsTemporaryCredentials) =>
      buildEcsClient(nextRegion, dependencies.tlsInsecure ?? false, dependencies.endpoint, nextCredentials));
  const createFsxClient =
    dependencies.createFsxClient ??
    ((nextRegion: string, nextCredentials?: AwsTemporaryCredentials) =>
      buildFsxClient(nextRegion, dependencies.tlsInsecure ?? false, dependencies.endpoint, nextCredentials));
  const createS3Client =
    dependencies.createS3Client ??
    ((nextRegion: string, nextCredentials?: AwsTemporaryCredentials) =>
      buildS3Client(nextRegion, dependencies.tlsInsecure ?? false, dependencies.endpoint, nextCredentials));

  switch (typeName) {
    case 'AWS::EC2::Instance':
      return listEc2Instances(accountId, region, createEc2Client(region, credentials));
    case 'AWS::EFS::FileSystem':
      return listEfsFileSystems(accountId, region, createEfsClient(region, credentials));
    case 'AWS::ECS::Cluster':
      return listEcsClusters(accountId, region, createEcsClient(region, credentials));
    case 'AWS::ECS::Service':
      return listEcsServices(accountId, region, createEcsClient(region, credentials));
    case 'AWS::FSx::FileSystem':
      return listFsxFileSystems(accountId, region, createFsxClient(region, credentials));
    case 'AWS::S3::Bucket':
      return listS3Buckets(accountId, region, createS3Client(region, credentials));
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
    const { cloudControlClient, credentials } = await resolveClients(dependencies, execution);

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
      if (isUnsupportedLocalstackCloudControlOperation(error)) {
        const fallbackResource = await getResourceDetailsByNativeFallback(
          dependencies,
          execution.region,
          typeName,
          identifier,
          credentials
        );

        if (fallbackResource) {
          return {
            identifier,
            typeName,
            properties: fallbackResource
          };
        }
      }

      return normalizeGatewayError(error, 'Falha ao obter detalhes do recurso.');
    }
  },

  createResource: async ({ execution, payload }) => {
    const { cloudControlClient, credentials } = await resolveClients(dependencies, execution);

    try {
      const response = await cloudControlClient.send(
        new CreateResourceCommand({
          TypeName: payload.typeName,
          DesiredState: JSON.stringify(payload.desiredState)
        })
      );

      return waitForProgressEvent(cloudControlClient, response.ProgressEvent, 'create');
    } catch (error: unknown) {
      if (isUnsupportedLocalstackCloudControlOperation(error)) {
        const fallbackProgressEvent = await createResourceByNativeFallback(
          dependencies,
          execution.region,
          payload,
          credentials
        );

        if (fallbackProgressEvent) {
          return fallbackProgressEvent;
        }
      }

      return normalizeGatewayError(error, 'Falha ao criar recurso.');
    }
  },

  updateResource: async ({ execution, payload }) => {
    if (!payload.identifier) {
      throw createAppError('MISSING_IDENTIFIER', 'Identifier obrigatorio para update.', 422);
    }

    const { cloudControlClient, credentials } = await resolveClients(dependencies, execution);

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
      if (isUnsupportedLocalstackCloudControlOperation(error)) {
        const fallbackProgressEvent = await updateResourceByNativeFallback(
          dependencies,
          execution.region,
          payload,
          credentials
        );

        if (fallbackProgressEvent) {
          return fallbackProgressEvent;
        }
      }

      return normalizeGatewayError(error, 'Falha ao atualizar recurso.');
    }
  },

  deleteResource: async ({ execution, typeName, identifier }) => {
    const { cloudControlClient, credentials } = await resolveClients(dependencies, execution);

    try {
      const response = await cloudControlClient.send(
        new DeleteResourceCommand({
          TypeName: typeName,
          Identifier: identifier
        })
      );

      return waitForProgressEvent(cloudControlClient, response.ProgressEvent, 'delete');
    } catch (error: unknown) {
      if (isUnsupportedLocalstackCloudControlOperation(error)) {
        const fallbackProgressEvent = await deleteResourceByNativeFallback(
          dependencies,
          execution.region,
          typeName,
          identifier,
          credentials
        );

        if (fallbackProgressEvent) {
          return fallbackProgressEvent;
        }
      }

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
