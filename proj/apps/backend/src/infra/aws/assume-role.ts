import { Agent } from 'node:https';
import { AssumeRoleCommand, GetCallerIdentityCommand, STSClient } from '@aws-sdk/client-sts';
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
  tlsInsecure: boolean;
};

type CreateAssumeRoleDependencies = {
  externalId?: string;
  roleArnTemplate?: string;
  baseCredentials?: AwsTemporaryCredentials;
  tlsInsecure?: boolean;
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
  tlsInsecure
}: CreateStsClientInput): STSClient =>
  new STSClient({
    region,
    credentials,
    requestHandler: tlsInsecure
      ? new NodeHttpHandler({
          httpsAgent: new Agent({
            rejectUnauthorized: false
          })
        })
      : undefined
  });

const toMessageFromUnknownError = (error: unknown, fallback: string): string => {
  if (typeof error === 'object' && error !== null && 'message' in error) {
    const candidate = error as { message?: unknown };
    if (typeof candidate.message === 'string' && candidate.message.trim().length > 0) {
      return candidate.message;
    }
  }

  return fallback;
};

export const createAssumeRole = ({
  externalId,
  roleArnTemplate,
  baseCredentials,
  tlsInsecure = false,
  createStsClient = defaultCreateStsClient
}: CreateAssumeRoleDependencies): AssumeRoleFn =>
  {
    let cachedCallerAccountId: string | undefined;

    const resolveCallerAccountId = async (region: string): Promise<string | undefined> => {
      if (cachedCallerAccountId) {
        return cachedCallerAccountId;
      }

      const stsClient = createStsClient({
        region,
        credentials: baseCredentials,
        tlsInsecure
      });

      try {
        const identity = await stsClient.send(new GetCallerIdentityCommand({}));
        const resolvedAccountId = identity.Account?.trim();
        cachedCallerAccountId = resolvedAccountId && resolvedAccountId.length > 0 ? resolvedAccountId : undefined;
        return cachedCallerAccountId;
      } catch (error: unknown) {
        throw createAppError(
          'AWS_IDENTITY_CHECK_FAILED',
          toMessageFromUnknownError(
            error,
            'Nao foi possivel validar a conta AWS ativa com GetCallerIdentity.'
          ),
          502,
          error
        );
      }
    };

    return async ({ account, region, userId }: AssumeRoleInput) => {
      if (!roleArnTemplate) {
        const callerAccountId = await resolveCallerAccountId(region);

        if (callerAccountId && callerAccountId !== account.accountId) {
          throw createAppError(
            'AWS_ACCOUNT_MISMATCH',
            `Conta selecionada (${account.accountId}) difere da conta ativa nas credenciais AWS (${callerAccountId}). Configure o contexto para a conta correta ou habilite assume role.`,
            403,
            {
              selectedAccountId: account.accountId,
              resolvedAccountId: callerAccountId
            }
          );
        }

        return baseCredentials;
      }

      const sessionName = `platform-${sanitizeSessionPart(userId)}-${Date.now()}`.slice(0, 64);
      const stsClient = createStsClient({
        region,
        credentials: baseCredentials,
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
  };
