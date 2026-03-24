import { Agent } from 'node:https';
import { AssumeRoleCommand, STSClient } from '@aws-sdk/client-sts';
import type { AwsAccount } from '@platform/shared';
import { NodeHttpHandler } from '@smithy/node-http-handler';
import { createAppError } from '../../domain/errors.js';

export type AwsTemporaryCredentials = {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken?: string;
};

export type AssumeRoleInput = {
  account: AwsAccount;
  region: string;
  userId: string;
};

export type AssumeRoleFn = (input: AssumeRoleInput) => Promise<AwsTemporaryCredentials | undefined>;

type CreateStsClientInput = {
  region: string;
  credentials?: AwsTemporaryCredentials;
  endpoint?: string;
  tlsInsecure: boolean;
};

type CreateAssumeRoleDependencies = {
  externalId?: string;
  roleArnTemplate?: string;
  baseCredentials?: AwsTemporaryCredentials;
  endpoint?: string;
  tlsInsecure?: boolean;
  useAccountIdForLocalstack?: boolean;
  createStsClient?: (input: CreateStsClientInput) => STSClient;
};

const sanitizeSessionPart = (rawValue: string): string =>
  rawValue
    .replace(/[^a-zA-Z0-9+=,.@-]/g, '-')
    .replace(/-{2,}/g, '-')
    .slice(0, 32);

const defaultCreateStsClient = ({
  region,
  credentials,
  endpoint,
  tlsInsecure
}: CreateStsClientInput): STSClient =>
  new STSClient({
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

const createLocalstackAccountCredentials = (accountId: string): AwsTemporaryCredentials => ({
  accessKeyId: accountId,
  secretAccessKey: accountId
});

export const createAssumeRole = ({
  externalId,
  roleArnTemplate,
  baseCredentials,
  endpoint,
  tlsInsecure = false,
  useAccountIdForLocalstack = false,
  createStsClient = defaultCreateStsClient
}: CreateAssumeRoleDependencies): AssumeRoleFn =>
  async ({ account, region, userId }: AssumeRoleInput) => {
    if (!roleArnTemplate) {
      return baseCredentials;
    }

    const sessionName = `platform-${sanitizeSessionPart(userId)}-${Date.now()}`.slice(0, 64);
    const sourceCredentials =
      useAccountIdForLocalstack && endpoint
        ? createLocalstackAccountCredentials(account.accountId)
        : baseCredentials;
    const stsClient = createStsClient({
      region,
      credentials: sourceCredentials,
      endpoint,
      tlsInsecure
    });
    const roleArn = roleArnTemplate.replaceAll('{account_id}', account.accountId);

    const assumeRoleOutput = await stsClient.send(
      new AssumeRoleCommand({
        RoleArn: roleArn,
        RoleSessionName: sessionName,
        DurationSeconds: 3600,
        ExternalId: externalId
      })
    );

    const credentials = assumeRoleOutput.Credentials;

    if (!credentials || !credentials.AccessKeyId || !credentials.SecretAccessKey) {
      throw createAppError(
        'ASSUME_ROLE_FAILED',
        `Nao foi possivel assumir a role da conta ${account.accountId}.`,
        502,
        assumeRoleOutput
      );
    }

    return {
      accessKeyId: credentials.AccessKeyId,
      secretAccessKey: credentials.SecretAccessKey,
      sessionToken: credentials.SessionToken
    };
  };
