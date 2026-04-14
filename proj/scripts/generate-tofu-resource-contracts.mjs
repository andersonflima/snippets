import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const repoRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const outputDirectoryPath = join(
  repoRoot,
  'packages',
  'shared',
  'src',
  'generated'
);
const outputJsonFilePath = join(outputDirectoryPath, 'tofu-resource-contracts.generated.json');
const outputTsFilePath = join(outputDirectoryPath, 'tofu-resource-contracts.generated.ts');

const tofuImage = process.env.TOFU_DOCKER_IMAGE ?? 'ghcr.io/opentofu/opentofu:1.9.1';
const awsProviderVersion = process.env.TOFU_AWS_PROVIDER_VERSION ?? '~> 6.0';

const bind = (templateKey, providerType, fieldKey, note) => ({
  templateKey,
  status: 'mapped',
  note,
  providerBindings: [{ providerType, fieldKey }]
});

const compound = (templateKey, bindings, note) => ({
  templateKey,
  status: 'compound',
  note,
  providerBindings: bindings.map(([providerType, fieldKey]) => ({ providerType, fieldKey }))
});

const unsupported = (templateKey, note) => ({
  templateKey,
  status: 'unsupported',
  note,
  providerBindings: []
});

const variant = (templateKey, bindings, note) => ({
  templateKey,
  status: 'variant',
  note,
  providerBindings: bindings.map(([providerType, fieldKey]) => ({ providerType, fieldKey }))
});

const fsxVariants = [
  'aws_fsx_windows_file_system',
  'aws_fsx_lustre_file_system',
  'aws_fsx_ontap_file_system',
  'aws_fsx_openzfs_file_system'
];
const excludedTopLevelAttributeKeys = new Set(['id', 'region', 'tags_all']);
const excludedTopLevelBlockKeys = new Set(['timeouts']);

const resourceMappings = [
  {
    typeName: 'AWS::EC2::Instance',
    category: 'compute',
    mappingStrategy: 'direct',
    warnings: [],
    providers: [{ providerType: 'aws_instance', role: 'primary' }],
    templateFieldMappings: [
      bind('InstanceType', 'aws_instance', 'instance_type'),
      bind('ImageId', 'aws_instance', 'ami'),
      bind('SubnetId', 'aws_instance', 'subnet_id'),
      bind('SecurityGroupIds', 'aws_instance', 'vpc_security_group_ids'),
      bind('KeyName', 'aws_instance', 'key_name'),
      bind('Tags', 'aws_instance', 'tags')
    ]
  },
  {
    typeName: 'AWS::Lambda::Function',
    category: 'compute',
    mappingStrategy: 'partial',
    warnings: [
      'O bloco Code do app agrega multiplas estrategias do provider aws_lambda_function.'
    ],
    providers: [{ providerType: 'aws_lambda_function', role: 'primary' }],
    templateFieldMappings: [
      bind('FunctionName', 'aws_lambda_function', 'function_name'),
      bind('Runtime', 'aws_lambda_function', 'runtime'),
      bind('Role', 'aws_lambda_function', 'role'),
      bind('Handler', 'aws_lambda_function', 'handler'),
      compound(
        'Code',
        [
          ['aws_lambda_function', 'filename'],
          ['aws_lambda_function', 'image_uri'],
          ['aws_lambda_function', 's3_bucket'],
          ['aws_lambda_function', 's3_key']
        ],
        'O provider divide o codigo entre filename, image_uri e origem em S3.'
      )
    ]
  },
  {
    typeName: 'AWS::ECS::Cluster',
    category: 'compute',
    mappingStrategy: 'partial',
    warnings: [
      'CapacityProviders e administrado por recurso separado no provider AWS.'
    ],
    providers: [{ providerType: 'aws_ecs_cluster', role: 'primary' }],
    templateFieldMappings: [
      bind('ClusterName', 'aws_ecs_cluster', 'name'),
      unsupported(
        'CapacityProviders',
        'Use aws_ecs_cluster_capacity_providers para representar capacity providers no provider.'
      )
    ]
  },
  {
    typeName: 'AWS::ECS::Service',
    category: 'compute',
    mappingStrategy: 'direct',
    warnings: [],
    providers: [{ providerType: 'aws_ecs_service', role: 'primary' }],
    templateFieldMappings: [
      bind('ServiceName', 'aws_ecs_service', 'name'),
      bind('Cluster', 'aws_ecs_service', 'cluster'),
      bind('TaskDefinition', 'aws_ecs_service', 'task_definition'),
      bind('DesiredCount', 'aws_ecs_service', 'desired_count'),
      bind('LaunchType', 'aws_ecs_service', 'launch_type')
    ]
  },
  {
    typeName: 'AWS::S3::Bucket',
    category: 'storage',
    mappingStrategy: 'composite',
    warnings: [
      'O contrato do app mistura aws_s3_bucket e aws_s3_bucket_public_access_block.'
    ],
    providers: [
      { providerType: 'aws_s3_bucket', role: 'primary' },
      { providerType: 'aws_s3_bucket_public_access_block', role: 'supporting' }
    ],
    templateFieldMappings: [
      bind('BucketName', 'aws_s3_bucket', 'bucket'),
      compound(
        'VersioningConfiguration',
        [['aws_s3_bucket', 'versioning']],
        'VersioningConfiguration vira bloco aninhado versioning no provider.'
      ),
      compound(
        'PublicAccessBlockConfiguration',
        [
          ['aws_s3_bucket_public_access_block', 'block_public_acls'],
          ['aws_s3_bucket_public_access_block', 'ignore_public_acls'],
          ['aws_s3_bucket_public_access_block', 'block_public_policy'],
          ['aws_s3_bucket_public_access_block', 'restrict_public_buckets']
        ],
        'O provider modela public access block em recurso dedicado.'
      ),
      compound(
        'BucketEncryption',
        [['aws_s3_bucket', 'server_side_encryption_configuration']],
        'BucketEncryption vira bloco aninhado server_side_encryption_configuration no provider.'
      )
    ]
  },
  {
    typeName: 'AWS::EFS::FileSystem',
    category: 'storage',
    mappingStrategy: 'direct',
    warnings: [],
    providers: [{ providerType: 'aws_efs_file_system', role: 'primary' }],
    templateFieldMappings: [
      bind('CreationToken', 'aws_efs_file_system', 'creation_token'),
      bind('ThroughputMode', 'aws_efs_file_system', 'throughput_mode'),
      bind('Encrypted', 'aws_efs_file_system', 'encrypted')
    ]
  },
  {
    typeName: 'AWS::FSx::FileSystem',
    category: 'storage',
    mappingStrategy: 'variant',
    warnings: [
      'AWS::FSx::FileSystem do app representa quatro recursos diferentes do provider AWS.'
    ],
    providers: fsxVariants.map((providerType) => ({ providerType, role: 'variant' })),
    templateFieldMappings: [
      unsupported(
        'FileSystemType',
        'O campo FileSystemType seleciona qual variante aws_fsx_*_file_system sera usada.'
      ),
      variant(
        'StorageCapacity',
        fsxVariants.map((providerType) => [providerType, 'storage_capacity']),
        'StorageCapacity existe em todas as variantes de FSx do provider.'
      ),
      variant(
        'SubnetIds',
        fsxVariants.map((providerType) => [providerType, 'subnet_ids']),
        'SubnetIds existe em todas as variantes de FSx do provider.'
      ),
      variant(
        'SecurityGroupIds',
        fsxVariants.map((providerType) => [providerType, 'security_group_ids']),
        'SecurityGroupIds existe em todas as variantes de FSx do provider.'
      )
    ]
  },
  {
    typeName: 'AWS::RDS::DBInstance',
    category: 'database',
    mappingStrategy: 'direct',
    warnings: [],
    providers: [{ providerType: 'aws_db_instance', role: 'primary' }],
    templateFieldMappings: [
      bind('DBInstanceIdentifier', 'aws_db_instance', 'identifier'),
      bind('DBInstanceClass', 'aws_db_instance', 'instance_class'),
      bind('Engine', 'aws_db_instance', 'engine'),
      bind('MasterUsername', 'aws_db_instance', 'username'),
      bind('MasterUserPassword', 'aws_db_instance', 'password'),
      bind('AllocatedStorage', 'aws_db_instance', 'allocated_storage')
    ]
  },
  {
    typeName: 'AWS::RDS::DBCluster',
    category: 'database',
    mappingStrategy: 'direct',
    warnings: [],
    providers: [{ providerType: 'aws_rds_cluster', role: 'primary' }],
    templateFieldMappings: [
      bind('DBClusterIdentifier', 'aws_rds_cluster', 'cluster_identifier'),
      bind('Engine', 'aws_rds_cluster', 'engine'),
      bind('DatabaseName', 'aws_rds_cluster', 'database_name'),
      bind('EngineMode', 'aws_rds_cluster', 'engine_mode'),
      bind('MasterUsername', 'aws_rds_cluster', 'master_username'),
      bind('MasterUserPassword', 'aws_rds_cluster', 'master_password')
    ]
  },
  {
    typeName: 'AWS::DynamoDB::Table',
    category: 'database',
    mappingStrategy: 'partial',
    warnings: [
      'AttributeDefinitions, KeySchema e ProvisionedThroughput exigem composicao de varios campos do provider.'
    ],
    providers: [{ providerType: 'aws_dynamodb_table', role: 'primary' }],
    templateFieldMappings: [
      bind('TableName', 'aws_dynamodb_table', 'name'),
      bind('BillingMode', 'aws_dynamodb_table', 'billing_mode'),
      compound(
        'AttributeDefinitions',
        [['aws_dynamodb_table', 'attribute']],
        'AttributeDefinitions vira bloco repetivel attribute no provider.'
      ),
      compound(
        'KeySchema',
        [
          ['aws_dynamodb_table', 'hash_key'],
          ['aws_dynamodb_table', 'range_key']
        ],
        'KeySchema do app precisa ser desmembrado em hash_key e range_key.'
      ),
      compound(
        'ProvisionedThroughput',
        [
          ['aws_dynamodb_table', 'read_capacity'],
          ['aws_dynamodb_table', 'write_capacity']
        ],
        'ProvisionedThroughput do app precisa virar read_capacity e write_capacity.'
      )
    ]
  },
  {
    typeName: 'AWS::EC2::VPC',
    category: 'network',
    mappingStrategy: 'direct',
    warnings: [],
    providers: [{ providerType: 'aws_vpc', role: 'primary' }],
    templateFieldMappings: [
      bind('CidrBlock', 'aws_vpc', 'cidr_block'),
      bind('EnableDnsSupport', 'aws_vpc', 'enable_dns_support'),
      bind('EnableDnsHostnames', 'aws_vpc', 'enable_dns_hostnames'),
      bind('InstanceTenancy', 'aws_vpc', 'instance_tenancy')
    ]
  },
  {
    typeName: 'AWS::EC2::Subnet',
    category: 'network',
    mappingStrategy: 'direct',
    warnings: [],
    providers: [{ providerType: 'aws_subnet', role: 'primary' }],
    templateFieldMappings: [
      bind('VpcId', 'aws_subnet', 'vpc_id'),
      bind('CidrBlock', 'aws_subnet', 'cidr_block'),
      bind('AvailabilityZone', 'aws_subnet', 'availability_zone'),
      bind('MapPublicIpOnLaunch', 'aws_subnet', 'map_public_ip_on_launch')
    ]
  },
  {
    typeName: 'AWS::EC2::SecurityGroup',
    category: 'network',
    mappingStrategy: 'partial',
    warnings: [
      'Ingress e egress sao atributos estruturados no provider e exigem serializacao cuidadosa.'
    ],
    providers: [{ providerType: 'aws_security_group', role: 'primary' }],
    templateFieldMappings: [
      bind('GroupName', 'aws_security_group', 'name'),
      bind('Description', 'aws_security_group', 'description'),
      bind('VpcId', 'aws_security_group', 'vpc_id'),
      compound(
        'SecurityGroupIngress',
        [['aws_security_group', 'ingress']],
        'SecurityGroupIngress vira atributo estruturado ingress no provider.'
      ),
      compound(
        'SecurityGroupEgress',
        [['aws_security_group', 'egress']],
        'SecurityGroupEgress vira atributo estruturado egress no provider.'
      )
    ]
  },
  {
    typeName: 'AWS::ElasticLoadBalancingV2::LoadBalancer',
    category: 'network',
    mappingStrategy: 'partial',
    warnings: [
      'Scheme do app precisa ser traduzido para o boolean internal do provider.'
    ],
    providers: [{ providerType: 'aws_lb', role: 'primary' }],
    templateFieldMappings: [
      bind('Name', 'aws_lb', 'name'),
      bind('Type', 'aws_lb', 'load_balancer_type'),
      bind('Subnets', 'aws_lb', 'subnets'),
      compound(
        'Scheme',
        [['aws_lb', 'internal']],
        'Scheme internet-facing/internal do app precisa ser convertido para internal=true/false.'
      )
    ]
  },
  {
    typeName: 'AWS::IAM::Role',
    category: 'security',
    mappingStrategy: 'direct',
    warnings: [],
    providers: [{ providerType: 'aws_iam_role', role: 'primary' }],
    templateFieldMappings: [
      bind('RoleName', 'aws_iam_role', 'name'),
      bind('AssumeRolePolicyDocument', 'aws_iam_role', 'assume_role_policy'),
      bind('Description', 'aws_iam_role', 'description')
    ]
  },
  {
    typeName: 'AWS::KMS::Key',
    category: 'security',
    mappingStrategy: 'direct',
    warnings: [],
    providers: [{ providerType: 'aws_kms_key', role: 'primary' }],
    templateFieldMappings: [
      bind('Description', 'aws_kms_key', 'description'),
      bind('Enabled', 'aws_kms_key', 'is_enabled'),
      bind('EnableKeyRotation', 'aws_kms_key', 'enable_key_rotation'),
      bind('MultiRegion', 'aws_kms_key', 'multi_region')
    ]
  },
  {
    typeName: 'AWS::SecretsManager::Secret',
    category: 'security',
    mappingStrategy: 'composite',
    warnings: [
      'SecretString vive em aws_secretsmanager_secret_version, nao em aws_secretsmanager_secret.'
    ],
    providers: [
      { providerType: 'aws_secretsmanager_secret', role: 'primary' },
      { providerType: 'aws_secretsmanager_secret_version', role: 'supporting' }
    ],
    templateFieldMappings: [
      bind('Name', 'aws_secretsmanager_secret', 'name'),
      bind('Description', 'aws_secretsmanager_secret', 'description'),
      compound(
        'SecretString',
        [['aws_secretsmanager_secret_version', 'secret_string']],
        'SecretString do app precisa ser entregue via recurso de versionamento do provider.'
      ),
      bind('Tags', 'aws_secretsmanager_secret', 'tags')
    ]
  },
  {
    typeName: 'AWS::CloudFormation::Stack',
    category: 'management',
    mappingStrategy: 'direct',
    warnings: [],
    providers: [{ providerType: 'aws_cloudformation_stack', role: 'primary' }],
    templateFieldMappings: [
      bind('StackName', 'aws_cloudformation_stack', 'name'),
      bind('TemplateURL', 'aws_cloudformation_stack', 'template_url'),
      bind('Capabilities', 'aws_cloudformation_stack', 'capabilities'),
      bind('Parameters', 'aws_cloudformation_stack', 'parameters')
    ]
  },
  {
    typeName: 'AWS::CloudWatch::Alarm',
    category: 'management',
    mappingStrategy: 'direct',
    warnings: [],
    providers: [{ providerType: 'aws_cloudwatch_metric_alarm', role: 'primary' }],
    templateFieldMappings: [
      bind('AlarmName', 'aws_cloudwatch_metric_alarm', 'alarm_name'),
      bind('MetricName', 'aws_cloudwatch_metric_alarm', 'metric_name'),
      bind('Namespace', 'aws_cloudwatch_metric_alarm', 'namespace'),
      bind('ComparisonOperator', 'aws_cloudwatch_metric_alarm', 'comparison_operator'),
      bind('EvaluationPeriods', 'aws_cloudwatch_metric_alarm', 'evaluation_periods'),
      bind('Threshold', 'aws_cloudwatch_metric_alarm', 'threshold'),
      bind('Period', 'aws_cloudwatch_metric_alarm', 'period'),
      bind('Statistic', 'aws_cloudwatch_metric_alarm', 'statistic')
    ]
  },
  {
    typeName: 'AWS::Events::Rule',
    category: 'management',
    mappingStrategy: 'direct',
    warnings: [],
    providers: [{ providerType: 'aws_cloudwatch_event_rule', role: 'primary' }],
    templateFieldMappings: [
      bind('Name', 'aws_cloudwatch_event_rule', 'name'),
      bind('ScheduleExpression', 'aws_cloudwatch_event_rule', 'schedule_expression'),
      bind('State', 'aws_cloudwatch_event_rule', 'state'),
      bind('EventPattern', 'aws_cloudwatch_event_rule', 'event_pattern'),
      bind('Description', 'aws_cloudwatch_event_rule', 'description')
    ]
  }
];

const toTerraformTypeSignature = (value) => {
  if (typeof value === 'string') {
    return value === 'bool' ? 'boolean' : value;
  }

  if (!Array.isArray(value) || value.length === 0) {
    return 'dynamic';
  }

  const [kind, payload] = value;

  if (kind === 'list' || kind === 'set' || kind === 'map') {
    return `${kind}<${toTerraformTypeSignature(payload)}>`;
  }

  if (kind === 'tuple' && Array.isArray(payload)) {
    return `tuple<${payload.map(toTerraformTypeSignature).join(', ')}>`;
  }

  if (kind === 'object' && payload && typeof payload === 'object') {
    const entries = Object.entries(payload).map(
      ([fieldKey, fieldType]) => `${fieldKey}:${toTerraformTypeSignature(fieldType)}`
    );
    return `object<{${entries.join(', ')}}>`;
  }

  return JSON.stringify(value);
};

const toFieldKind = (value) => {
  if (typeof value === 'string') {
    if (value === 'string') {
      return 'string';
    }

    if (value === 'number') {
      return 'number';
    }

    if (value === 'bool') {
      return 'boolean';
    }

    return 'json';
  }

  if (!Array.isArray(value) || value.length === 0) {
    return 'json';
  }

  const [kind] = value;

  if (kind === 'list' || kind === 'set' || kind === 'tuple') {
    return 'array';
  }

  if (kind === 'object' || kind === 'map') {
    return 'object';
  }

  return 'json';
};

const sortFields = (fields) =>
  [...fields].sort((left, right) => {
    if (left.required !== right.required) {
      return left.required ? -1 : 1;
    }

    return left.key.localeCompare(right.key);
  });

const toNestedFieldsFromType = (value) => {
  if (typeof value === 'string') {
    return [];
  }

  if (!Array.isArray(value) || value.length === 0) {
    return [];
  }

  const [kind, payload] = value;

  if (kind === 'object' && payload && typeof payload === 'object') {
    return sortFields(
      Object.entries(payload).map(([fieldKey, fieldType]) => {
        const nestedFields = toNestedFieldsFromType(fieldType);
        return {
          key: fieldKey,
          source: 'attribute',
          required: false,
          optional: true,
          computed: false,
          sensitive: false,
          kind: toFieldKind(fieldType),
          typeSignature: toTerraformTypeSignature(fieldType),
          ...(nestedFields.length > 0 ? { nestedFields } : {})
        };
      })
    );
  }

  if (kind === 'list' || kind === 'set' || kind === 'map') {
    return toNestedFieldsFromType(payload);
  }

  if (kind === 'tuple' && Array.isArray(payload)) {
    return sortFields(
      payload.flatMap((fieldType, index) => {
        const nestedFields = toNestedFieldsFromType(fieldType);
        if (nestedFields.length > 0) {
          return [
            {
              key: String(index),
              source: 'attribute',
              required: false,
              optional: true,
              computed: false,
              sensitive: false,
              kind: toFieldKind(fieldType),
              typeSignature: toTerraformTypeSignature(fieldType),
              nestedFields
            }
          ];
        }

        return [];
      })
    );
  }

  return [];
};

const collectBlockFields = (block, isTopLevel = false) => {
  const attributes = Object.entries(block.attributes ?? {});
  const inputFields = attributes
    .filter(([fieldKey]) => !(isTopLevel && excludedTopLevelAttributeKeys.has(fieldKey)))
    .filter(([, attribute]) => Boolean(attribute.required || attribute.optional))
    .map(([fieldKey, attribute]) => {
      const nestedFields = toNestedFieldsFromType(attribute.type);
      return {
        key: fieldKey,
        source: 'attribute',
        required: Boolean(attribute.required),
        optional: Boolean(attribute.optional),
        computed: Boolean(attribute.computed),
        sensitive: Boolean(attribute.sensitive),
        deprecated: Boolean(attribute.deprecated),
        kind: toFieldKind(attribute.type),
        typeSignature: toTerraformTypeSignature(attribute.type),
        ...(attribute.description ? { description: attribute.description } : {}),
        ...(nestedFields.length > 0 ? { nestedFields } : {})
      };
    });

  const computedOnlyFields = attributes
    .filter(([fieldKey]) => !(isTopLevel && excludedTopLevelAttributeKeys.has(fieldKey)))
    .filter(([, attribute]) => !attribute.required && !attribute.optional && Boolean(attribute.computed))
    .map(([fieldKey, attribute]) => {
      const nestedFields = toNestedFieldsFromType(attribute.type);
      return {
        key: fieldKey,
        source: 'attribute',
        required: false,
        optional: false,
        computed: true,
        sensitive: Boolean(attribute.sensitive),
        deprecated: Boolean(attribute.deprecated),
        kind: toFieldKind(attribute.type),
        typeSignature: toTerraformTypeSignature(attribute.type),
        ...(attribute.description ? { description: attribute.description } : {}),
        ...(nestedFields.length > 0 ? { nestedFields } : {})
      };
    });

  const blockFields = Object.entries(block.block_types ?? {})
    .filter(([fieldKey]) => !(isTopLevel && excludedTopLevelBlockKeys.has(fieldKey)))
    .map(([fieldKey, blockType]) => {
      const nestedFields = collectBlockFields(blockType.block, false).inputFields;
      const minItems = Number(blockType.min_items ?? 0);
      const maxItems = typeof blockType.max_items === 'number' ? blockType.max_items : null;

      return {
        key: fieldKey,
        source: 'block',
        required: minItems > 0,
        optional: minItems === 0,
        computed: false,
        sensitive: false,
        deprecated: Boolean(blockType.block.deprecated),
        kind: 'json',
        typeSignature: `${blockType.nesting_mode}<block>`,
        nestingMode: blockType.nesting_mode,
        minItems,
        ...(maxItems !== null ? { maxItems } : {}),
        ...(blockType.block.description ? { description: blockType.block.description } : {}),
        ...(nestedFields.length > 0 ? { nestedFields } : {})
      };
    });

  return {
    inputFields: sortFields([...inputFields, ...blockFields]),
    computedOnlyFields: sortFields(computedOnlyFields)
  };
};

const executeCommand = (command, args, options = {}) => {
  const result = spawnSync(command, args, {
    encoding: 'utf8',
    maxBuffer: 1024 * 1024 * 256,
    ...options
  });

  if (result.status !== 0) {
    throw new Error(result.stderr?.trim() || result.stdout?.trim() || `Falha ao executar ${command}`);
  }

  return result.stdout;
};

const createTofuWorkspace = () => {
  const workspacePath = mkdtempSync(join(tmpdir(), 'tofu-resource-contracts-'));
  const configuration = `terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "${awsProviderVersion}"
    }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_requesting_account_id  = true
  skip_metadata_api_check     = true
  skip_region_validation      = true
}
`;

  writeFileSync(join(workspacePath, 'main.tf'), configuration);
  return workspacePath;
};

const runTofu = (workspacePath, args, stdio = 'pipe') =>
  executeCommand(
    'docker',
    ['run', '--rm', '-v', `${workspacePath}:/workspace`, '-w', '/workspace', tofuImage, ...args],
    { stdio }
  );

const resolveAwsProviderSchema = (schema) => {
  const providerSchemaEntries = Object.entries(schema.provider_schemas ?? {});
  const hit = providerSchemaEntries.find(([providerKey]) => providerKey.endsWith('/hashicorp/aws'));

  if (!hit) {
    throw new Error('Provider AWS nao encontrado no schema gerado pelo OpenTofu.');
  }

  return hit[1];
};

const flattenProviderFieldRefs = (providerContracts) =>
  new Set(
    providerContracts.flatMap((providerContract) =>
      providerContract.inputFields.map((field) => `${providerContract.providerType}.${field.key}`)
    )
  );

const buildProviderContract = (awsProviderSchema, resourceMapping) => {
  const providerContracts = resourceMapping.providers.map(({ providerType, role }) => {
    const providerSchema = awsProviderSchema.resource_schemas?.[providerType];

    if (!providerSchema) {
      return {
        providerType,
        role,
        inputFields: [],
        computedOnlyFields: [],
        warnings: [`Resource schema ${providerType} nao encontrado no provider AWS.`]
      };
    }

    const blockContract = collectBlockFields(providerSchema.block, true);

    return {
      providerType,
      role,
      inputFields: blockContract.inputFields,
      computedOnlyFields: blockContract.computedOnlyFields,
      warnings: []
    };
  });

  const availableProviderFieldRefs = flattenProviderFieldRefs(providerContracts);
  const invalidBindingWarnings = resourceMapping.templateFieldMappings.flatMap((mapping) =>
    mapping.providerBindings
      .filter(
        ({ providerType, fieldKey }) =>
          !availableProviderFieldRefs.has(`${providerType}.${fieldKey}`)
      )
      .map(
        ({ providerType, fieldKey }) =>
          `Template field ${mapping.templateKey} aponta para ${providerType}.${fieldKey}, mas esse campo nao existe no schema gerado.`
      )
  );

  return {
    typeName: resourceMapping.typeName,
    category: resourceMapping.category,
    mappingStrategy: resourceMapping.mappingStrategy,
    generatedAt: new Date().toISOString(),
    generator: {
      tofuImage,
      awsProviderVersion
    },
    warnings: [...resourceMapping.warnings, ...invalidBindingWarnings],
    providerContracts,
    templateFieldMappings: resourceMapping.templateFieldMappings
  };
};

const main = () => {
  const workspacePath = createTofuWorkspace();

  try {
    runTofu(workspacePath, ['init', '-backend=false'], 'inherit');

    const rawSchema = runTofu(workspacePath, ['providers', 'schema', '-json']);
    const parsedSchema = JSON.parse(rawSchema);
    const awsProviderSchema = resolveAwsProviderSchema(parsedSchema);
    const contracts = resourceMappings.map((resourceMapping) =>
      buildProviderContract(awsProviderSchema, resourceMapping)
    );

    mkdirSync(outputDirectoryPath, { recursive: true });
    writeFileSync(outputJsonFilePath, `${JSON.stringify(contracts, null, 2)}\n`);
    writeFileSync(
      outputTsFilePath,
      `const resourceProviderContracts = ${JSON.stringify(contracts, null, 2)} as const;\n\nexport default resourceProviderContracts;\n`
    );

    const summary = contracts.map((contract) => ({
      typeName: contract.typeName,
      providers: contract.providerContracts.length,
      inputFields: contract.providerContracts.reduce(
        (total, providerContract) => total + providerContract.inputFields.length,
        0
      ),
      warnings: contract.warnings.length
    }));

    process.stdout.write(`Contratos gerados em ${outputJsonFilePath} e ${outputTsFilePath}\n`);
    process.stdout.write(`${JSON.stringify(summary, null, 2)}\n`);
  } finally {
    rmSync(workspacePath, { recursive: true, force: true });
  }
};

main();
