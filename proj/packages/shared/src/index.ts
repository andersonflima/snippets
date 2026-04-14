import generatedProviderContracts from './generated/tofu-resource-contracts.generated.js';

export type UserRole = 'admin' | 'operator' | 'viewer';

export type AwsCategory =
  | 'compute'
  | 'storage'
  | 'database'
  | 'network'
  | 'security'
  | 'management';

export type ResourceAction = 'list' | 'get' | 'create' | 'update' | 'delete';
export type ResourceStateAction = Exclude<ResourceAction, 'list' | 'get'>;
export type ResourceStateStatus = 'planned' | 'submitted' | 'applied' | 'failed';
export type PermissionCategory = AwsCategory | '*';

export type ResourceTemplateFieldKind =
  | 'string'
  | 'number'
  | 'boolean'
  | 'json'
  | 'array'
  | 'object'
  | 'enum';

export type ResourceTemplateField = {
  key: string;
  label: string;
  kind: ResourceTemplateFieldKind;
  required: boolean;
  placeholder?: string;
  description?: string;
  defaultValue?: unknown;
  enumValues?: readonly string[];
};

export type ResourceTemplate = {
  typeName: string;
  category: AwsCategory;
  description: string;
  fields: readonly ResourceTemplateField[];
};

export type ResourceUpdateProfileKind = 'field' | 'group' | 'tags';

export type ResourceUpdateProfile = {
  id: string;
  typeName: string;
  label: string;
  description: string;
  kind: ResourceUpdateProfileKind;
  fieldKeys: readonly string[];
  fields: readonly ResourceTemplateField[];
};

export type ProviderContractFieldSource = 'attribute' | 'block';
export type ResourceProviderMappingStrategy = 'direct' | 'partial' | 'composite' | 'variant';
export type ResourceTemplateFieldMappingStatus = 'mapped' | 'compound' | 'unsupported' | 'variant';

export type ProviderContractField = {
  key: string;
  source: ProviderContractFieldSource;
  required: boolean;
  optional: boolean;
  computed: boolean;
  sensitive: boolean;
  deprecated?: boolean;
  kind: ResourceTemplateFieldKind | 'json';
  typeSignature: string;
  description?: string;
  nestingMode?: string;
  minItems?: number;
  maxItems?: number;
  nestedFields?: readonly ProviderContractField[];
};

export type ProviderContractGenerator = {
  tofuImage: string;
  awsProviderVersion: string;
};

export type ProviderResourceContract = {
  providerType: string;
  role: string;
  inputFields: readonly ProviderContractField[];
  computedOnlyFields: readonly ProviderContractField[];
  warnings: readonly string[];
};

export type ResourceProviderBinding = {
  providerType: string;
  fieldKey: string;
};

export type ResourceTemplateFieldMapping = {
  templateKey: string;
  status: ResourceTemplateFieldMappingStatus;
  note?: string;
  providerBindings: readonly ResourceProviderBinding[];
};

export type ResourceProviderContractCatalogEntry = {
  typeName: string;
  category: AwsCategory;
  mappingStrategy: ResourceProviderMappingStrategy;
  generatedAt: string;
  generator: ProviderContractGenerator;
  warnings: readonly string[];
  providerContracts: readonly ProviderResourceContract[];
  templateFieldMappings: readonly ResourceTemplateFieldMapping[];
};

export type ResourceContractCoverage = {
  typeName: string;
  mappingStrategy: ResourceProviderMappingStrategy;
  templateFieldCount: number;
  mappedTemplateFieldCount: number;
  unmappedTemplateFieldKeys: readonly string[];
  providerInputFieldCount: number;
  mappedProviderFieldCount: number;
  unmappedProviderFieldKeys: readonly string[];
  warnings: readonly string[];
  templateFieldMappings: readonly ResourceTemplateFieldMapping[];
};

export type AwsAccount = {
  accountId: string;
  name: string;
  allowedRegions: readonly string[];
};

export type PlatformUser = {
  id: string;
  email: string;
  name: string;
  passwordHash: string;
  role: UserRole;
  accounts: readonly AwsAccount[];
};

export type JwtClaims = {
  sub: string;
  email: string;
  role: UserRole;
};

export type UserContext = {
  accountId: string;
  region: string;
  category: AwsCategory;
};

export type CategoryResourceType = {
  category: AwsCategory;
  resourceTypes: readonly string[];
};

export type ResourceSummary = {
  identifier: string;
  typeName: string;
  displayName: string;
  region: string;
  accountId: string;
};

export type CheckupResult = {
  category: AwsCategory;
  accountId: string;
  region: string;
  resourceCounts: Record<string, number>;
};

export type FinOpsResourceSummary = ResourceSummary & {
  isObsolete: boolean;
  lastStateAt: number | null;
  daysWithoutUpdate: number | null;
  lastStateStatus: ResourceStateRecord['status'] | null;
  lastStateOperation: ResourceStateRecord['operation'] | null;
  obsolescenceReason: string;
};

export type FinOpsOverview = {
  accountId: string;
  region: string;
  category: AwsCategory;
  staleThresholdDays: number;
  totalResources: number;
  obsoleteResources: number;
  resourcesWithoutState: number;
  staleRatePercent: number;
  resourcesByType: Record<string, number>;
  obsoleteByType: Record<string, number>;
  resources: readonly FinOpsResourceSummary[];
  warnings: readonly string[];
};

export const TOP_AWS_RESOURCE_TYPES = [
  'AWS::EC2::Instance',
  'AWS::Lambda::Function',
  'AWS::ECS::Cluster',
  'AWS::ECS::Service',
  'AWS::S3::Bucket',
  'AWS::EFS::FileSystem',
  'AWS::FSx::FileSystem',
  'AWS::RDS::DBInstance',
  'AWS::RDS::DBCluster',
  'AWS::DynamoDB::Table',
  'AWS::EC2::VPC',
  'AWS::EC2::Subnet',
  'AWS::EC2::SecurityGroup',
  'AWS::ElasticLoadBalancingV2::LoadBalancer',
  'AWS::IAM::Role',
  'AWS::KMS::Key',
  'AWS::SecretsManager::Secret',
  'AWS::CloudFormation::Stack',
  'AWS::CloudWatch::Alarm',
  'AWS::Events::Rule'
] as const;

export type DeleteIntent = {
  id: string;
  userId: string;
  accountId: string;
  region: string;
  category: AwsCategory;
  resourceType: string;
  resourceId: string;
  expiresAt: number;
};

export type ResourceStateRecord = {
  id: string;
  userId: string;
  accountId: string;
  region: string;
  category: AwsCategory;
  typeName: string;
  identifier: string;
  operation: ResourceStateAction;
  status: ResourceStateStatus;
  version: number;
  desiredState: Record<string, unknown>;
  patchDocument: readonly Record<string, unknown>[] | null;
  createdAt: number;
  createdBy: string;
};

export type UpsertResourcePayload = {
  typeName: string;
  identifier?: string;
  updateProfileId?: string;
  desiredState: Record<string, unknown>;
  patchDocument?: readonly Record<string, unknown>[];
};

export type PermissionScope = {
  accountId: string;
  category: PermissionCategory;
  resourceType: string;
  action: ResourceAction;
};

export type PermissionRule = PermissionScope & {
  id: string;
  userId: string;
};

export const RESOURCE_TEMPLATES: readonly ResourceTemplate[] = [
  {
    typeName: 'AWS::EC2::Instance',
    category: 'compute',
    description: 'Maquina virtual EC2',
    fields: [
      {
        key: 'InstanceType',
        label: 'InstanceType',
        kind: 'string',
        required: true,
        placeholder: 't3.micro'
      },
      {
        key: 'ImageId',
        label: 'ImageId (AMI)',
        kind: 'string',
        required: true,
        placeholder: 'ami-1234567890abcdef'
      },
      {
        key: 'SubnetId',
        label: 'SubnetId',
        kind: 'string',
        required: false,
        placeholder: 'subnet-xxxxxxxx'
      },
      {
        key: 'SecurityGroupIds',
        label: 'SecurityGroupIds',
        kind: 'json',
        required: false,
        defaultValue: [],
        description: 'Array de SecurityGroup IDs'
      },
      {
        key: 'KeyName',
        label: 'KeyName',
        kind: 'string',
        required: false,
        placeholder: 'my-keypair'
      },
      {
        key: 'Tags',
        label: 'Tags',
        kind: 'json',
        required: false,
        defaultValue: [{ Key: 'managed-by', Value: 'platform' }],
        description: 'Objeto JSON de tags'
      }
    ]
  },
  {
    typeName: 'AWS::Lambda::Function',
    category: 'compute',
    description: 'Funcao Lambda',
    fields: [
      {
        key: 'FunctionName',
        label: 'FunctionName',
        kind: 'string',
        required: true,
        placeholder: 'my-lambda'
      },
      {
        key: 'Runtime',
        label: 'Runtime',
        kind: 'enum',
        required: true,
        enumValues: ['nodejs18.x', 'python3.12', 'java21', 'dotnet8'],
        defaultValue: 'nodejs18.x'
      },
      {
        key: 'Role',
        label: 'Role ARN',
        kind: 'string',
        required: true,
        placeholder: 'arn:aws:iam::123456789012:role/lambda-role'
      },
      {
        key: 'Handler',
        label: 'Handler',
        kind: 'string',
        required: true,
        placeholder: 'index.handler'
      },
      {
        key: 'Code',
        label: 'Code',
        kind: 'json',
        required: false,
        defaultValue: { S3Bucket: '', S3Key: '' },
        description: 'Bucket e chave do código'
      }
    ]
  },
  {
    typeName: 'AWS::ECS::Cluster',
    category: 'compute',
    description: 'Cluster ECS',
    fields: [
      {
        key: 'ClusterName',
        label: 'ClusterName',
        kind: 'string',
        required: true,
        placeholder: 'my-ecs-cluster'
      },
      {
        key: 'CapacityProviders',
        label: 'CapacityProviders',
        kind: 'json',
        required: false,
        defaultValue: [],
        description: 'Array de Capacity Providers'
      }
    ]
  },
  {
    typeName: 'AWS::ECS::Service',
    category: 'compute',
    description: 'Servico ECS',
    fields: [
      {
        key: 'ServiceName',
        label: 'ServiceName',
        kind: 'string',
        required: true,
        placeholder: 'my-ecs-service'
      },
      {
        key: 'Cluster',
        label: 'Cluster',
        kind: 'string',
        required: true,
        placeholder: 'my-ecs-cluster'
      },
      {
        key: 'TaskDefinition',
        label: 'TaskDefinition',
        kind: 'string',
        required: true,
        placeholder: 'my-task-definition'
      },
      {
        key: 'DesiredCount',
        label: 'DesiredCount',
        kind: 'number',
        required: false,
        defaultValue: 1
      },
      {
        key: 'LaunchType',
        label: 'LaunchType',
        kind: 'enum',
        required: false,
        enumValues: ['EC2', 'FARGATE'],
        defaultValue: 'FARGATE'
      }
    ]
  },
  {
    typeName: 'AWS::S3::Bucket',
    category: 'storage',
    description: 'Bucket S3',
    fields: [
      {
        key: 'BucketName',
        label: 'BucketName',
        kind: 'string',
        required: true,
        placeholder: 'meu-bucket'
      },
      {
        key: 'VersioningConfiguration',
        label: 'Versionamento',
        kind: 'json',
        required: false,
        defaultValue: { Status: 'Enabled' },
        description: 'Defina se o bucket mantém versionamento habilitado ou suspenso.'
      },
      {
        key: 'PublicAccessBlockConfiguration',
        label: 'Acesso público',
        kind: 'json',
        required: false,
        defaultValue: { BlockPublicAcls: true, IgnorePublicAcls: true, BlockPublicPolicy: true, RestrictPublicBuckets: true },
        description: 'Controle os bloqueios padrão de acesso público do bucket.'
      },
      {
        key: 'BucketEncryption',
        label: 'Criptografia padrão',
        kind: 'json',
        required: false,
        description: 'Escolha a criptografia padrão do bucket.'
      }
    ]
  },
  {
    typeName: 'AWS::EFS::FileSystem',
    category: 'storage',
    description: 'Sistema de arquivos EFS',
    fields: [
      {
        key: 'CreationToken',
        label: 'CreationToken',
        kind: 'string',
        required: false,
        placeholder: 'minha-creacao'
      },
      {
        key: 'ThroughputMode',
        label: 'ThroughputMode',
        kind: 'enum',
        required: false,
        enumValues: ['bursting', 'provisioned'],
        defaultValue: 'bursting'
      },
      {
        key: 'Encrypted',
        label: 'Encrypted',
        kind: 'boolean',
        required: false,
        defaultValue: true
      }
    ]
  },
  {
    typeName: 'AWS::FSx::FileSystem',
    category: 'storage',
    description: 'Sistema de arquivos FSx',
    fields: [
      {
        key: 'FileSystemType',
        label: 'FileSystemType',
        kind: 'enum',
        required: true,
        enumValues: ['WINDOWS', 'LUSTRE', 'ONTAP'],
        defaultValue: 'WINDOWS'
      },
      {
        key: 'StorageCapacity',
        label: 'StorageCapacity',
        kind: 'number',
        required: true,
        placeholder: '32'
      },
      {
        key: 'SubnetIds',
        label: 'SubnetIds',
        kind: 'json',
        required: true,
        description: 'Lista de subnets',
        defaultValue: []
      },
      {
        key: 'SecurityGroupIds',
        label: 'SecurityGroupIds',
        kind: 'json',
        required: false,
        defaultValue: []
      }
    ]
  },
  {
    typeName: 'AWS::RDS::DBInstance',
    category: 'database',
    description: 'Instância RDS',
    fields: [
      {
        key: 'DBInstanceIdentifier',
        label: 'DBInstanceIdentifier',
        kind: 'string',
        required: true,
        placeholder: 'meu-rds'
      },
      {
        key: 'DBInstanceClass',
        label: 'DBInstanceClass',
        kind: 'string',
        required: true,
        placeholder: 'db.t4g.micro'
      },
      {
        key: 'Engine',
        label: 'Engine',
        kind: 'enum',
        required: true,
        enumValues: ['mysql', 'postgres', 'mariadb', 'sqlserver-ex'],
        defaultValue: 'postgres'
      },
      {
        key: 'MasterUsername',
        label: 'MasterUsername',
        kind: 'string',
        required: true,
        placeholder: 'admin'
      },
      {
        key: 'MasterUserPassword',
        label: 'MasterUserPassword',
        kind: 'string',
        required: true,
        placeholder: 'Senha forte'
      },
      {
        key: 'AllocatedStorage',
        label: 'AllocatedStorage',
        kind: 'number',
        required: false,
        defaultValue: 20
      }
    ]
  },
  {
    typeName: 'AWS::RDS::DBCluster',
    category: 'database',
    description: 'Cluster RDS',
    fields: [
      {
        key: 'DBClusterIdentifier',
        label: 'DBClusterIdentifier',
        kind: 'string',
        required: true,
        placeholder: 'meu-cluster'
      },
      {
        key: 'Engine',
        label: 'Engine',
        kind: 'enum',
        required: true,
        enumValues: ['aurora-mysql', 'aurora-postgresql'],
        defaultValue: 'aurora-postgresql'
      },
      {
        key: 'DatabaseName',
        label: 'DatabaseName',
        kind: 'string',
        required: false,
        placeholder: 'app'
      },
      {
        key: 'EngineMode',
        label: 'EngineMode',
        kind: 'string',
        required: false,
        defaultValue: 'provisioned'
      },
      {
        key: 'MasterUsername',
        label: 'MasterUsername',
        kind: 'string',
        required: true,
        placeholder: 'admin'
      },
      {
        key: 'MasterUserPassword',
        label: 'MasterUserPassword',
        kind: 'string',
        required: true,
        placeholder: 'Senha forte'
      }
    ]
  },
  {
    typeName: 'AWS::DynamoDB::Table',
    category: 'database',
    description: 'Tabela DynamoDB',
    fields: [
      {
        key: 'TableName',
        label: 'TableName',
        kind: 'string',
        required: true,
        placeholder: 'meu-table'
      },
      {
        key: 'BillingMode',
        label: 'BillingMode',
        kind: 'enum',
        required: false,
        enumValues: ['PAY_PER_REQUEST', 'PROVISIONED'],
        defaultValue: 'PAY_PER_REQUEST'
      },
      {
        key: 'AttributeDefinitions',
        label: 'AttributeDefinitions',
        kind: 'json',
        required: true,
        defaultValue: [{ AttributeName: 'pk', AttributeType: 'S' }]
      },
      {
        key: 'KeySchema',
        label: 'KeySchema',
        kind: 'json',
        required: true,
        defaultValue: [{ AttributeName: 'pk', KeyType: 'HASH' }]
      },
      {
        key: 'ProvisionedThroughput',
        label: 'Capacidade provisionada',
        kind: 'json',
        required: false,
        description: 'Somente para BillingMode PROVISIONED',
        defaultValue: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
      }
    ]
  },
  {
    typeName: 'AWS::EC2::VPC',
    category: 'network',
    description: 'VPC',
    fields: [
      {
        key: 'CidrBlock',
        label: 'CidrBlock',
        kind: 'string',
        required: true,
        placeholder: '10.0.0.0/16'
      },
      {
        key: 'EnableDnsSupport',
        label: 'EnableDnsSupport',
        kind: 'boolean',
        required: false,
        defaultValue: true
      },
      {
        key: 'EnableDnsHostnames',
        label: 'EnableDnsHostnames',
        kind: 'boolean',
        required: false,
        defaultValue: true
      },
      {
        key: 'InstanceTenancy',
        label: 'InstanceTenancy',
        kind: 'enum',
        required: false,
        enumValues: ['default', 'default'],
        defaultValue: 'default'
      }
    ]
  },
  {
    typeName: 'AWS::EC2::Subnet',
    category: 'network',
    description: 'Subnet',
    fields: [
      {
        key: 'VpcId',
        label: 'VpcId',
        kind: 'string',
        required: true,
        placeholder: 'vpc-123456'
      },
      {
        key: 'CidrBlock',
        label: 'CidrBlock',
        kind: 'string',
        required: true,
        placeholder: '10.0.1.0/24'
      },
      {
        key: 'AvailabilityZone',
        label: 'AvailabilityZone',
        kind: 'string',
        required: false,
        placeholder: 'us-east-1a'
      },
      {
        key: 'MapPublicIpOnLaunch',
        label: 'MapPublicIpOnLaunch',
        kind: 'boolean',
        required: false,
        defaultValue: false
      }
    ]
  },
  {
    typeName: 'AWS::EC2::SecurityGroup',
    category: 'network',
    description: 'Security Group',
    fields: [
      {
        key: 'GroupName',
        label: 'GroupName',
        kind: 'string',
        required: true,
        placeholder: 'web-sg'
      },
      {
        key: 'Description',
        label: 'Description',
        kind: 'string',
        required: true,
        placeholder: 'Grupo para Web'
      },
      {
        key: 'VpcId',
        label: 'VpcId',
        kind: 'string',
        required: true,
        placeholder: 'vpc-123456'
      },
      {
        key: 'SecurityGroupIngress',
        label: 'SecurityGroupIngress',
        kind: 'json',
        required: false,
        defaultValue: []
      },
      {
        key: 'SecurityGroupEgress',
        label: 'SecurityGroupEgress',
        kind: 'json',
        required: false,
        defaultValue: []
      }
    ]
  },
  {
    typeName: 'AWS::ElasticLoadBalancingV2::LoadBalancer',
    category: 'network',
    description: 'Application Load Balancer',
    fields: [
      {
        key: 'Name',
        label: 'Name',
        kind: 'string',
        required: true,
        placeholder: 'my-alb'
      },
      {
        key: 'Type',
        label: 'Type',
        kind: 'enum',
        required: true,
        enumValues: ['application', 'network', 'gateway'],
        defaultValue: 'application'
      },
      {
        key: 'Subnets',
        label: 'Subnets',
        kind: 'json',
        required: true,
        defaultValue: []
      },
      {
        key: 'Scheme',
        label: 'Scheme',
        kind: 'enum',
        required: false,
        enumValues: ['internet-facing', 'internal'],
        defaultValue: 'internet-facing'
      }
    ]
  },
  {
    typeName: 'AWS::IAM::Role',
    category: 'security',
    description: 'Role IAM',
    fields: [
      {
        key: 'RoleName',
        label: 'RoleName',
        kind: 'string',
        required: true,
        placeholder: 'platform-role'
      },
      {
        key: 'AssumeRolePolicyDocument',
        label: 'AssumeRolePolicyDocument',
        kind: 'json',
        required: true,
        description: 'Doc policy do trust',
        defaultValue: {
          Version: '2012-10-17',
          Statement: [
            {
              Effect: 'Allow',
              Principal: { Service: 'ec2.amazonaws.com' },
              Action: 'sts:AssumeRole'
            }
          ]
        }
      },
      {
        key: 'Description',
        label: 'Description',
        kind: 'string',
        required: false,
        placeholder: 'Role da plataforma'
      }
    ]
  },
  {
    typeName: 'AWS::KMS::Key',
    category: 'security',
    description: 'Chave KMS',
    fields: [
      {
        key: 'Description',
        label: 'Description',
        kind: 'string',
        required: false,
        placeholder: 'Chave gerenciada'
      },
      {
        key: 'Enabled',
        label: 'Enabled',
        kind: 'boolean',
        required: false,
        defaultValue: true
      },
      {
        key: 'EnableKeyRotation',
        label: 'EnableKeyRotation',
        kind: 'boolean',
        required: false,
        defaultValue: true
      },
      {
        key: 'MultiRegion',
        label: 'MultiRegion',
        kind: 'boolean',
        required: false,
        defaultValue: false
      }
    ]
  },
  {
    typeName: 'AWS::SecretsManager::Secret',
    category: 'security',
    description: 'Secret no Secrets Manager',
    fields: [
      {
        key: 'Name',
        label: 'Name',
        kind: 'string',
        required: true,
        placeholder: 'meu-secret'
      },
      {
        key: 'Description',
        label: 'Description',
        kind: 'string',
        required: false,
        placeholder: 'Descricao do segredo'
      },
      {
        key: 'SecretString',
        label: 'SecretString',
        kind: 'string',
        required: true,
        placeholder: 'chave=valor'
      },
      {
        key: 'Tags',
        label: 'Tags',
        kind: 'json',
        required: false,
        defaultValue: [{ Key: 'managed-by', Value: 'platform' }]
      }
    ]
  },
  {
    typeName: 'AWS::CloudFormation::Stack',
    category: 'management',
    description: 'Stack CloudFormation',
    fields: [
      {
        key: 'StackName',
        label: 'StackName',
        kind: 'string',
        required: true,
        placeholder: 'meu-stack'
      },
      {
        key: 'TemplateURL',
        label: 'TemplateURL',
        kind: 'string',
        required: true,
        placeholder: 'https://s3.amazonaws.com/meu-template'
      },
      {
        key: 'Capabilities',
        label: 'Capabilities',
        kind: 'json',
        required: false,
        defaultValue: ['CAPABILITY_NAMED_IAM']
      },
      {
        key: 'Parameters',
        label: 'Parameters',
        kind: 'json',
        required: false,
        defaultValue: {}
      }
    ]
  },
  {
    typeName: 'AWS::CloudWatch::Alarm',
    category: 'management',
    description: 'Alarme CloudWatch',
    fields: [
      {
        key: 'AlarmName',
        label: 'AlarmName',
        kind: 'string',
        required: true,
        placeholder: 'minha-alarme'
      },
      {
        key: 'MetricName',
        label: 'MetricName',
        kind: 'string',
        required: true,
        placeholder: 'CPUUtilization'
      },
      {
        key: 'Namespace',
        label: 'Namespace',
        kind: 'string',
        required: true,
        placeholder: 'AWS/EC2'
      },
      {
        key: 'ComparisonOperator',
        label: 'ComparisonOperator',
        kind: 'enum',
        required: true,
        enumValues: ['GreaterThanThreshold', 'LessThanThreshold', 'GreaterThanOrEqualToThreshold', 'LessThanOrEqualToThreshold'],
        defaultValue: 'GreaterThanThreshold'
      },
      {
        key: 'EvaluationPeriods',
        label: 'EvaluationPeriods',
        kind: 'number',
        required: true,
        defaultValue: 2
      },
      {
        key: 'Threshold',
        label: 'Threshold',
        kind: 'number',
        required: true,
        defaultValue: 70
      },
      {
        key: 'Period',
        label: 'Period',
        kind: 'number',
        required: false,
        defaultValue: 60
      },
      {
        key: 'Statistic',
        label: 'Statistic',
        kind: 'enum',
        required: false,
        enumValues: ['Average', 'Sum', 'Maximum'],
        defaultValue: 'Average'
      }
    ]
  },
  {
    typeName: 'AWS::Events::Rule',
    category: 'management',
    description: 'Regra EventBridge',
    fields: [
      {
        key: 'Name',
        label: 'Name',
        kind: 'string',
        required: true,
        placeholder: 'my-rule'
      },
      {
        key: 'ScheduleExpression',
        label: 'ScheduleExpression',
        kind: 'string',
        required: false,
        placeholder: 'rate(5 minutes)'
      },
      {
        key: 'State',
        label: 'State',
        kind: 'enum',
        required: true,
        enumValues: ['ENABLED', 'DISABLED'],
        defaultValue: 'ENABLED'
      },
      {
        key: 'EventPattern',
        label: 'EventPattern',
        kind: 'json',
        required: false,
        description: 'JSON do padrão de evento'
      },
      {
        key: 'Description',
        label: 'Description',
        kind: 'string',
        required: false,
        placeholder: 'Regra de eventos'
      }
    ]
  }
] as const;

export const getResourceTemplate = (typeName: string): ResourceTemplate | undefined =>
  RESOURCE_TEMPLATES.find((entry) => entry.typeName === typeName);

export const getResourceTemplates = (): readonly ResourceTemplate[] => RESOURCE_TEMPLATES;

const RESOURCE_IDENTIFIER_FIELD_KEYS = new Set([
  'Name',
  'BucketName',
  'CreationToken',
  'DBInstanceIdentifier',
  'DBClusterIdentifier',
  'TableName',
  'FunctionName',
  'ServiceName',
  'ClusterName',
  'RoleName',
  'StackName',
  'AlarmName',
  'GroupName'
]);

const resolveUpdateableFields = (
  template: ResourceTemplate
): readonly ResourceTemplateField[] =>
  template.fields.filter((field) => !RESOURCE_IDENTIFIER_FIELD_KEYS.has(field.key));

export const getResourceUpdateProfiles = (typeName: string): readonly ResourceUpdateProfile[] => {
  const template = getResourceTemplate(typeName);

  if (!template) {
    return [];
  }

  const fields = resolveUpdateableFields(template);

  if (fields.length === 0) {
    return [];
  }

  return [
    {
      id: 'all-mapped-fields',
      typeName,
      label: 'Todos os campos mapeados',
      description:
        'Exibe todos os campos mapeados para update deste recurso. Preencha apenas o que deseja alterar.',
      kind: fields.length > 1 ? 'group' : 'field',
      fieldKeys: fields.map((field) => field.key),
      fields
    }
  ];
};

export const getResourceUpdateProfile = (
  typeName: string,
  profileId: string
): ResourceUpdateProfile | undefined =>
  getResourceUpdateProfiles(typeName).find((profile) => profile.id === profileId);

export const getDefaultResourceUpdateProfile = (
  typeName: string
): ResourceUpdateProfile | undefined => getResourceUpdateProfiles(typeName)[0];

export const RESOURCE_PROVIDER_CONTRACTS =
  generatedProviderContracts as readonly ResourceProviderContractCatalogEntry[];

export const getResourceProviderContracts = (): readonly ResourceProviderContractCatalogEntry[] =>
  RESOURCE_PROVIDER_CONTRACTS;

export const getResourceProviderContract = (
  typeName: string
): ResourceProviderContractCatalogEntry | undefined =>
  RESOURCE_PROVIDER_CONTRACTS.find((entry) => entry.typeName === typeName);

const toProviderFieldRef = (binding: ResourceProviderBinding): string =>
  `${binding.providerType}.${binding.fieldKey}`;

const flattenProviderFieldRefs = (
  contract: ResourceProviderContractCatalogEntry
): readonly string[] =>
  contract.providerContracts.flatMap((providerContract) =>
    providerContract.inputFields.map((field) => `${providerContract.providerType}.${field.key}`)
  );

export const getResourceContractCoverage = (
  typeName: string
): ResourceContractCoverage | undefined => {
  const contract = getResourceProviderContract(typeName);
  const template = getResourceTemplate(typeName);

  if (!contract || !template) {
    return undefined;
  }

  const templateFieldKeys = template.fields.map((field) => field.key);
  const mappedTemplateFieldKeys = new Set(
    contract.templateFieldMappings
      .filter((mapping) => mapping.providerBindings.length > 0)
      .map((mapping) => mapping.templateKey)
  );
  const providerInputFieldRefs = flattenProviderFieldRefs(contract);
  const mappedProviderFieldRefs = new Set(
    contract.templateFieldMappings.flatMap((mapping) => mapping.providerBindings.map(toProviderFieldRef))
  );

  return {
    typeName,
    mappingStrategy: contract.mappingStrategy,
    templateFieldCount: templateFieldKeys.length,
    mappedTemplateFieldCount: templateFieldKeys.filter((fieldKey) =>
      mappedTemplateFieldKeys.has(fieldKey)
    ).length,
    unmappedTemplateFieldKeys: templateFieldKeys.filter(
      (fieldKey) => !mappedTemplateFieldKeys.has(fieldKey)
    ),
    providerInputFieldCount: providerInputFieldRefs.length,
    mappedProviderFieldCount: providerInputFieldRefs.filter((fieldRef) =>
      mappedProviderFieldRefs.has(fieldRef)
    ).length,
    unmappedProviderFieldKeys: providerInputFieldRefs.filter(
      (fieldRef) => !mappedProviderFieldRefs.has(fieldRef)
    ),
    warnings: contract.warnings,
    templateFieldMappings: contract.templateFieldMappings
  };
};
