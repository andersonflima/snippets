import type { AwsCategory } from '@platform/shared';

export const CATEGORY_RESOURCE_TYPES: Record<AwsCategory, readonly string[]> = {
  compute: [
    'AWS::EC2::Instance',
    'AWS::Lambda::Function',
    'AWS::ECS::Cluster',
    'AWS::ECS::Service'
  ],
  storage: ['AWS::S3::Bucket', 'AWS::EFS::FileSystem', 'AWS::FSx::FileSystem'],
  database: ['AWS::RDS::DBInstance', 'AWS::RDS::DBCluster', 'AWS::DynamoDB::Table'],
  network: [
    'AWS::EC2::VPC',
    'AWS::EC2::Subnet',
    'AWS::EC2::SecurityGroup',
    'AWS::ElasticLoadBalancingV2::LoadBalancer'
  ],
  security: ['AWS::IAM::Role', 'AWS::KMS::Key', 'AWS::SecretsManager::Secret'],
  management: ['AWS::CloudFormation::Stack', 'AWS::CloudWatch::Alarm', 'AWS::Events::Rule']
};

export const getCategoryResourceTypes = (category: AwsCategory): readonly string[] =>
  CATEGORY_RESOURCE_TYPES[category];

export const allCategories = (): readonly AwsCategory[] =>
  Object.keys(CATEGORY_RESOURCE_TYPES) as readonly AwsCategory[];
