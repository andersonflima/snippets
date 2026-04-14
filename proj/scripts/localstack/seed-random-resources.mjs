#!/usr/bin/env node

import { execFileSync } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const currentFilePath = fileURLToPath(import.meta.url);
const repoRoot = resolve(dirname(currentFilePath), '..', '..');
const inventoryPath = resolve(
  repoRoot,
  process.env.PLATFORM_SEED_ACCOUNTS_FILE ?? 'apps/backend/.localstack-organization-accounts.json'
);
const outputPath = resolve(repoRoot, 'apps/backend/.localstack-seeded-resources.json');
const localstackContainerName = process.env.LOCALSTACK_CONTAINER_NAME ?? 'localstack-main';
const primaryRegionOnly = process.env.LOCALSTACK_PRIMARY_REGION_ONLY === 'true';
const runId = `${Date.now().toString(36)}-${randomBytes(3).toString('hex')}`;

const normalizeText = (value) => {
  const normalized = String(value ?? '').trim();
  return normalized === 'None' || normalized === 'null' ? '' : normalized;
};

const slugify = (value) =>
  value
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/-{2,}/g, '-')
    .replace(/^-|-$/g, '')
    .slice(0, 18);

const toBucketName = (value) =>
  slugify(value)
    .replace(/^-+/, '')
    .replace(/-+$/, '')
    .slice(0, 50);

const dockerExec = (args) =>
  execFileSync('docker', args, {
    cwd: repoRoot,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe']
  });

const accountExecPrefix = (accountId) => [
  'exec',
  '-e',
  `AWS_ACCESS_KEY_ID=${accountId}`,
  '-e',
  `AWS_SECRET_ACCESS_KEY=${accountId}`,
  localstackContainerName,
  'awslocal'
];

const awsText = ({ accountId, region, serviceArgs }) =>
  normalizeText(
    dockerExec([...accountExecPrefix(accountId), '--region', region, ...serviceArgs, '--output', 'text'])
  );

const aws = ({ accountId, region, serviceArgs }) =>
  dockerExec([...accountExecPrefix(accountId), '--region', region, ...serviceArgs]);

const dockerShell = (script) =>
  dockerExec(['exec', localstackContainerName, 'sh', '-lc', script]);

const readInventory = () => JSON.parse(readFileSync(inventoryPath, 'utf8'));

const ensureSharedLambdaAsset = () => {
  const zipPath = `/tmp/platform-seed-lambda-${runId}.zip`;

  dockerShell(`cat > /tmp/platform-seed-index.js <<'EOF'
exports.handler = async () => ({
  statusCode: 200,
  body: JSON.stringify({ ok: true, runId: '${runId}' })
});
EOF
cd /tmp && zip -q ${zipPath.split('/').pop()} platform-seed-index.js`);

  return zipPath;
};

const writeStackTemplate = ({ accountId, region, bucketName }) => {
  const templatePath = `/tmp/platform-stack-${accountId}-${region}-${runId}.yaml`;

  dockerShell(`cat > ${templatePath} <<'EOF'
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  SeedBucket:
    Type: AWS::S3::Bucket
    Properties:
      BucketName: ${bucketName}
EOF`);

  return templatePath;
};

const resourcePlan = [
  'AWS::EC2::VPC',
  'AWS::EC2::Subnet',
  'AWS::EC2::SecurityGroup',
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
  'AWS::ElasticLoadBalancingV2::LoadBalancer',
  'AWS::IAM::Role',
  'AWS::KMS::Key',
  'AWS::SecretsManager::Secret',
  'AWS::CloudFormation::Stack',
  'AWS::CloudWatch::Alarm',
  'AWS::Events::Rule'
];

const createRecorder = ({ accountId, accountName, region, resources }) => ({
  ok: (typeName, identifier, metadata = {}) => {
    resources.push({
      accountId,
      accountName,
      region,
      typeName,
      identifier,
      status: 'created',
      ...metadata
    });
  },
  skipped: (typeName, reason) => {
    resources.push({
      accountId,
      accountName,
      region,
      typeName,
      status: 'skipped',
      reason
    });
  },
  failed: (typeName, error) => {
    resources.push({
      accountId,
      accountName,
      region,
      typeName,
      status: 'failed',
      reason: String(error instanceof Error ? error.message : error)
    });
  }
});

const tryCreate = ({ typeName, recorder, create }) => {
  try {
    const identifier = create();
    recorder.ok(typeName, identifier);
    return identifier;
  } catch (error) {
    const reason = String(error instanceof Error ? error.message : error);

    if (
      reason.includes('not currently supported by LocalStack') ||
      reason.includes('operation is not currently supported')
    ) {
      recorder.skipped(typeName, reason);
      return undefined;
    }

    recorder.failed(typeName, reason);
    return undefined;
  }
};

const buildNames = ({ accountName, region }) => {
  const prefix = `${slugify(accountName)}-${slugify(region)}-${runId}`;

  return {
    securityGroupName: `${prefix}-sg`,
    bucketName: toBucketName(`${prefix}-bucket`),
    stackBucketName: toBucketName(`${prefix}-stack-bucket`),
    efsToken: `${prefix}-efs`,
    dbInstanceName: `${prefix}-db`,
    dbClusterName: `${prefix}-clusterdb`,
    tableName: `${prefix}-table`,
    loadBalancerName: prefix.slice(0, 28),
    roleName: `${prefix}-role`.slice(0, 64),
    lambdaRoleName: `${prefix}-lambda-role`.slice(0, 64),
    keyDescription: `${prefix} kms key`,
    secretName: `${prefix}-secret`,
    stackName: `${prefix}-stack`,
    alarmName: `${prefix}-alarm`,
    eventRuleName: `${prefix}-rule`,
    lambdaName: `${prefix}-lambda`,
    clusterName: `${prefix}-cluster`,
    taskFamily: `${prefix}-task`,
    serviceName: `${prefix}-service`
  };
};

const createPrimaryBundle = ({
  account,
  accountIndex,
  region,
  lambdaZipPath,
  resources
}) => {
  const names = buildNames({ accountName: account.name, region });
  const recorder = createRecorder({
    accountId: account.accountId,
    accountName: account.name,
    region,
    resources
  });
  const networkOctet = 40 + accountIndex * 20;
  const vpcCidrBlock = `10.${networkOctet}.0.0/16`;
  const subnetACidrBlock = `10.${networkOctet}.1.0/24`;
  const subnetBCidrBlock = `10.${networkOctet}.2.0/24`;
  const availabilityZoneA = `${region}a`;
  const availabilityZoneB = `${region}b`;

  const vpcId = tryCreate({
    typeName: 'AWS::EC2::VPC',
    recorder,
    create: () =>
      awsText({
        accountId: account.accountId,
        region,
        serviceArgs: ['ec2', 'create-vpc', '--cidr-block', vpcCidrBlock, '--query', 'Vpc.VpcId']
      })
  });

  const subnetAId = vpcId
    ? tryCreate({
        typeName: 'AWS::EC2::Subnet',
        recorder,
        create: () =>
          awsText({
            accountId: account.accountId,
            region,
            serviceArgs: [
              'ec2',
              'create-subnet',
              '--vpc-id',
              vpcId,
              '--cidr-block',
              subnetACidrBlock,
              '--availability-zone',
              availabilityZoneA,
              '--query',
              'Subnet.SubnetId'
            ]
          })
      })
    : undefined;

  const subnetBId = vpcId
    ? tryCreate({
        typeName: 'AWS::EC2::Subnet',
        recorder,
        create: () =>
          awsText({
            accountId: account.accountId,
            region,
            serviceArgs: [
              'ec2',
              'create-subnet',
              '--vpc-id',
              vpcId,
              '--cidr-block',
              subnetBCidrBlock,
              '--availability-zone',
              availabilityZoneB,
              '--query',
              'Subnet.SubnetId'
            ]
          })
      })
    : undefined;

  const securityGroupId = vpcId
    ? tryCreate({
        typeName: 'AWS::EC2::SecurityGroup',
        recorder,
        create: () =>
          awsText({
            accountId: account.accountId,
            region,
            serviceArgs: [
              'ec2',
              'create-security-group',
              '--group-name',
              names.securityGroupName,
              '--description',
              `Security group ${names.securityGroupName}`,
              '--vpc-id',
              vpcId,
              '--query',
              'GroupId'
            ]
          })
      })
    : undefined;

  if (securityGroupId) {
    try {
      aws({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'ec2',
          'authorize-security-group-ingress',
          '--group-id',
          securityGroupId,
          '--protocol',
          'tcp',
          '--port',
          '80',
          '--cidr',
          '0.0.0.0/0'
        ]
      });
    } catch (_error) {
      // Ingress is optional for the seed.
    }
  }

  const imageId = awsText({
    accountId: account.accountId,
    region,
    serviceArgs: ['ec2', 'describe-images', '--query', 'Images[0].ImageId']
  });

  if (subnetAId && securityGroupId && imageId) {
    tryCreate({
      typeName: 'AWS::EC2::Instance',
      recorder,
      create: () =>
        awsText({
          accountId: account.accountId,
          region,
          serviceArgs: [
            'ec2',
            'run-instances',
            '--image-id',
            imageId,
            '--instance-type',
            't3.micro',
            '--subnet-id',
            subnetAId,
            '--security-group-ids',
            securityGroupId,
            '--count',
            '1',
            '--query',
            'Instances[0].InstanceId'
          ]
        })
    });
  } else {
    recorder.skipped('AWS::EC2::Instance', 'Dependencias de rede ou AMI indisponiveis.');
  }

  const lambdaRoleArn = tryCreate({
    typeName: 'AWS::IAM::Role',
    recorder,
    create: () =>
      awsText({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'iam',
          'create-role',
          '--role-name',
          names.lambdaRoleName,
          '--assume-role-policy-document',
          JSON.stringify({
            Version: '2012-10-17',
            Statement: [
              {
                Effect: 'Allow',
                Principal: { Service: 'lambda.amazonaws.com' },
                Action: 'sts:AssumeRole'
              }
            ]
          }),
          '--query',
          'Role.Arn'
        ]
      })
  });

  tryCreate({
    typeName: 'AWS::IAM::Role',
    recorder,
    create: () =>
      awsText({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'iam',
          'create-role',
          '--role-name',
          names.roleName,
          '--assume-role-policy-document',
          JSON.stringify({
            Version: '2012-10-17',
            Statement: [
              {
                Effect: 'Allow',
                Principal: { Service: 'ec2.amazonaws.com' },
                Action: 'sts:AssumeRole'
              }
            ]
          }),
          '--query',
          'Role.Arn'
        ]
      })
  });

  if (lambdaRoleArn) {
    tryCreate({
      typeName: 'AWS::Lambda::Function',
      recorder,
      create: () =>
        awsText({
          accountId: account.accountId,
          region,
          serviceArgs: [
            'lambda',
            'create-function',
            '--function-name',
            names.lambdaName,
            '--runtime',
            'nodejs18.x',
            '--role',
            lambdaRoleArn,
            '--handler',
            'platform-seed-index.handler',
            '--zip-file',
            `fileb://${lambdaZipPath}`,
            '--query',
            'FunctionArn'
          ]
        })
    });
  } else {
    recorder.skipped('AWS::Lambda::Function', 'Role de Lambda nao criada.');
  }

  const clusterArn = tryCreate({
    typeName: 'AWS::ECS::Cluster',
    recorder,
    create: () =>
      awsText({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'ecs',
          'create-cluster',
          '--cluster-name',
          names.clusterName,
          '--query',
          'cluster.clusterArn'
        ]
      })
  });

  const taskDefinitionArn = clusterArn
    ? awsText({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'ecs',
          'register-task-definition',
          '--family',
          names.taskFamily,
          '--network-mode',
          'bridge',
          '--container-definitions',
          JSON.stringify([
            {
              name: 'web',
              image: 'nginx:alpine',
              cpu: 0,
              memory: 128,
              essential: true
            }
          ]),
          '--query',
          'taskDefinition.taskDefinitionArn'
        ]
      })
    : undefined;

  if (clusterArn && taskDefinitionArn) {
    tryCreate({
      typeName: 'AWS::ECS::Service',
      recorder,
      create: () =>
        awsText({
          accountId: account.accountId,
          region,
          serviceArgs: [
            'ecs',
            'create-service',
            '--cluster',
            names.clusterName,
            '--service-name',
            names.serviceName,
            '--task-definition',
            names.taskFamily,
            '--desired-count',
            '0',
            '--launch-type',
            'EC2',
            '--query',
            'service.serviceArn'
          ]
        })
    });
  } else {
    recorder.skipped('AWS::ECS::Service', 'Cluster ECS ou task definition indisponivel.');
  }

  tryCreate({
    typeName: 'AWS::S3::Bucket',
    recorder,
    create: () => {
      aws({
        accountId: account.accountId,
        region,
        serviceArgs: [
          's3api',
          'create-bucket',
          '--bucket',
          names.bucketName,
          ...(region !== 'us-east-1'
            ? ['--create-bucket-configuration', `LocationConstraint=${region}`]
            : [])
        ]
      });

      aws({
        accountId: account.accountId,
        region,
        serviceArgs: [
          's3api',
          'put-bucket-versioning',
          '--bucket',
          names.bucketName,
          '--versioning-configuration',
          'Status=Enabled'
        ]
      });

      return names.bucketName;
    }
  });

  tryCreate({
    typeName: 'AWS::EFS::FileSystem',
    recorder,
    create: () =>
      awsText({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'efs',
          'create-file-system',
          '--creation-token',
          names.efsToken,
          '--encrypted',
          '--query',
          'FileSystemId'
        ]
      })
  });

  if (subnetAId) {
    tryCreate({
      typeName: 'AWS::FSx::FileSystem',
      recorder,
      create: () =>
        awsText({
          accountId: account.accountId,
          region,
          serviceArgs: [
            'fsx',
            'create-file-system',
            '--file-system-type',
            'LUSTRE',
            '--storage-capacity',
            '1200',
            '--subnet-ids',
            subnetAId,
            '--lustre-configuration',
            'DeploymentType=SCRATCH_1,PerUnitStorageThroughput=50',
            '--query',
            'FileSystem.FileSystemId'
          ]
        })
    });
  } else {
    recorder.skipped('AWS::FSx::FileSystem', 'Subnet primaria nao criada.');
  }

  tryCreate({
    typeName: 'AWS::RDS::DBInstance',
    recorder,
    create: () =>
      awsText({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'rds',
          'create-db-instance',
          '--db-instance-identifier',
          names.dbInstanceName,
          '--db-instance-class',
          'db.t3.micro',
          '--engine',
          'postgres',
          '--master-username',
          'admin',
          '--master-user-password',
          'Platform123!',
          '--allocated-storage',
          '20',
          '--query',
          'DBInstance.DBInstanceIdentifier'
        ]
      })
  });

  tryCreate({
    typeName: 'AWS::RDS::DBCluster',
    recorder,
    create: () =>
      awsText({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'rds',
          'create-db-cluster',
          '--db-cluster-identifier',
          names.dbClusterName,
          '--engine',
          'aurora-postgresql',
          '--master-username',
          'admin',
          '--master-user-password',
          'Platform123!',
          '--database-name',
          'app',
          '--query',
          'DBCluster.DBClusterIdentifier'
        ]
      })
  });

  tryCreate({
    typeName: 'AWS::DynamoDB::Table',
    recorder,
    create: () =>
      awsText({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'dynamodb',
          'create-table',
          '--table-name',
          names.tableName,
          '--attribute-definitions',
          'AttributeName=pk,AttributeType=S',
          '--key-schema',
          'AttributeName=pk,KeyType=HASH',
          '--billing-mode',
          'PAY_PER_REQUEST',
          '--query',
          'TableDescription.TableName'
        ]
      })
  });

  if (subnetAId && subnetBId && securityGroupId) {
    tryCreate({
      typeName: 'AWS::ElasticLoadBalancingV2::LoadBalancer',
      recorder,
      create: () =>
        awsText({
          accountId: account.accountId,
          region,
          serviceArgs: [
            'elbv2',
            'create-load-balancer',
            '--name',
            names.loadBalancerName,
            '--type',
            'application',
            '--subnets',
            subnetAId,
            subnetBId,
            '--security-groups',
            securityGroupId,
            '--query',
            'LoadBalancers[0].LoadBalancerArn'
          ]
        })
    });
  } else {
    recorder.skipped(
      'AWS::ElasticLoadBalancingV2::LoadBalancer',
      'Subnets ou security group insuficientes.'
    );
  }

  tryCreate({
    typeName: 'AWS::KMS::Key',
    recorder,
    create: () =>
      awsText({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'kms',
          'create-key',
          '--description',
          names.keyDescription,
          '--query',
          'KeyMetadata.KeyId'
        ]
      })
  });

  tryCreate({
    typeName: 'AWS::SecretsManager::Secret',
    recorder,
    create: () =>
      awsText({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'secretsmanager',
          'create-secret',
          '--name',
          names.secretName,
          '--secret-string',
          JSON.stringify({
            username: 'platform',
            password: `seed-${runId}`
          }),
          '--query',
          'ARN'
        ]
      })
  });

  const stackTemplatePath = writeStackTemplate({
    accountId: account.accountId,
    region,
    bucketName: names.stackBucketName
  });

  tryCreate({
    typeName: 'AWS::CloudFormation::Stack',
    recorder,
    create: () =>
      awsText({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'cloudformation',
          'create-stack',
          '--stack-name',
          names.stackName,
          '--template-body',
          `file://${stackTemplatePath}`,
          '--query',
          'StackId'
        ]
      })
  });

  tryCreate({
    typeName: 'AWS::CloudWatch::Alarm',
    recorder,
    create: () => {
      aws({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'cloudwatch',
          'put-metric-alarm',
          '--alarm-name',
          names.alarmName,
          '--metric-name',
          'CPUUtilization',
          '--namespace',
          'AWS/EC2',
          '--statistic',
          'Average',
          '--period',
          '60',
          '--evaluation-periods',
          '1',
          '--threshold',
          '70',
          '--comparison-operator',
          'GreaterThanThreshold'
        ]
      });

      return names.alarmName;
    }
  });

  tryCreate({
    typeName: 'AWS::Events::Rule',
    recorder,
    create: () =>
      awsText({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'events',
          'put-rule',
          '--name',
          names.eventRuleName,
          '--schedule-expression',
          'rate(5 minutes)',
          '--state',
          'ENABLED',
          '--query',
          'RuleArn'
        ]
      })
  });
};

const createSecondaryRegionBundle = ({ account, region, resources }) => {
  const names = buildNames({ accountName: `${account.name}-extra`, region });
  const recorder = createRecorder({
    accountId: account.accountId,
    accountName: account.name,
    region,
    resources
  });

  tryCreate({
    typeName: 'AWS::S3::Bucket',
    recorder,
    create: () => {
      aws({
        accountId: account.accountId,
        region,
        serviceArgs: [
          's3api',
          'create-bucket',
          '--bucket',
          names.bucketName,
          ...(region !== 'us-east-1'
            ? ['--create-bucket-configuration', `LocationConstraint=${region}`]
            : [])
        ]
      });

      return names.bucketName;
    }
  });

  tryCreate({
    typeName: 'AWS::DynamoDB::Table',
    recorder,
    create: () =>
      awsText({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'dynamodb',
          'create-table',
          '--table-name',
          names.tableName,
          '--attribute-definitions',
          'AttributeName=pk,AttributeType=S',
          '--key-schema',
          'AttributeName=pk,KeyType=HASH',
          '--billing-mode',
          'PAY_PER_REQUEST',
          '--query',
          'TableDescription.TableName'
        ]
      })
  });

  tryCreate({
    typeName: 'AWS::Events::Rule',
    recorder,
    create: () =>
      awsText({
        accountId: account.accountId,
        region,
        serviceArgs: [
          'events',
          'put-rule',
          '--name',
          names.eventRuleName,
          '--schedule-expression',
          'rate(15 minutes)',
          '--state',
          'ENABLED',
          '--query',
          'RuleArn'
        ]
      })
  });
};

const inventory = readInventory();
const seedAccounts = Object.values(inventory.seedAccounts ?? {});
const createdAt = new Date().toISOString();
const resources = [];
const lambdaZipPath = ensureSharedLambdaAsset();

seedAccounts.forEach((account, accountIndex) => {
  const [primaryRegion, ...secondaryRegions] = account.allowedRegions;

  if (!primaryRegion) {
    return;
  }

  createPrimaryBundle({
    account,
    accountIndex,
    region: primaryRegion,
    lambdaZipPath,
    resources
  });

  if (!primaryRegionOnly) {
    secondaryRegions.forEach((region) =>
      createSecondaryRegionBundle({
        account,
        region,
        resources
      })
    );
  }
});

const summary = resourcePlan.map((typeName) => ({
  typeName,
  created: resources.filter((resource) => resource.typeName === typeName && resource.status === 'created')
    .length,
  skipped: resources.filter((resource) => resource.typeName === typeName && resource.status === 'skipped')
    .length,
  failed: resources.filter((resource) => resource.typeName === typeName && resource.status === 'failed')
    .length
}));

mkdirSync(dirname(outputPath), { recursive: true });
writeFileSync(
  outputPath,
  JSON.stringify(
    {
      runId,
      createdAt,
      localstackContainerName,
      primaryRegionOnly,
      resources,
      summary
    },
    null,
    2
  )
);

const totals = resources.reduce(
  (accumulator, resource) => ({
    created: accumulator.created + (resource.status === 'created' ? 1 : 0),
    skipped: accumulator.skipped + (resource.status === 'skipped' ? 1 : 0),
    failed: accumulator.failed + (resource.status === 'failed' ? 1 : 0)
  }),
  { created: 0, skipped: 0, failed: 0 }
);

process.stdout.write(
  JSON.stringify(
    {
      runId,
      createdAt,
      outputPath,
      totals,
      summary
    },
    null,
    2
  )
);
