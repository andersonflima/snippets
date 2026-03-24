import { AssumeRoleCommand, STSClient } from '@aws-sdk/client-sts';
import type { AwsAccount } from '@platform/shared';
import { createAppError } from '../../domain/errors.js';

export type AwsTemporaryCredentials = {
  accessKeyId: string;
  secretAccessKey: string;
  sessionToken: string;
};

export type AssumeRoleInput = {
  account: AwsAccount;
  region: string;
  userId: string;
};

export type AssumeRoleFn = (input: AssumeRoleInput) => Promise<AwsTemporaryCredentials>;

type CreateAssumeRoleDependencies = {
  externalId?: string;
  roleArnTemplate: string;
  createStsClient?: (region: string) => STSClient;
};

const sanitizeSessionPart = (rawValue: string): string =>
  rawValue
    .replace(/[^a-zA-Z0-9+=,.@-]/g, '-')
    .replace(/-{2,}/g, '-')
    .slice(0, 32);

export const createAssumeRole = ({
  externalId,
  roleArnTemplate,
  createStsClient = (region: string) => new STSClient({ region })
}: CreateAssumeRoleDependencies): AssumeRoleFn =>
  async ({ account, region, userId }: AssumeRoleInput) => {
    const sessionName = `platform-${sanitizeSessionPart(userId)}-${Date.now()}`.slice(0, 64);
    const stsClient = createStsClient(region);
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

    if (
      !credentials ||
      !credentials.AccessKeyId ||
      !credentials.SecretAccessKey ||
      !credentials.SessionToken
    ) {
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
