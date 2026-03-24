import { createAdminService } from './application/admin-service.js';
import { createAuthService } from './application/auth-service.js';
import { createContextService } from './application/context-service.js';
import { createResourceService } from './application/resource-service.js';
import { env } from './config/env.js';
import { createAssumeRole } from './infra/aws/assume-role.js';
import { createCloudControlGateway } from './infra/aws/cloud-control.js';
import { prepareDatabase } from './infra/db/bootstrap.js';
import { createDatabaseClient } from './infra/db/postgres.js';
import {
  createContextRepository,
  createDeleteIntentRepository,
  createPermissionRepository,
  createResourceStateRepository,
  createUserRepository
} from './infra/repositories/postgres.js';

export const createContainer = async () => {
  const databaseClient = createDatabaseClient({
    connectionString: env.databaseUrl,
    sslEnabled: env.databaseSsl
  });

  await prepareDatabase(databaseClient);

  const userRepository = createUserRepository(databaseClient);
  const contextRepository = createContextRepository(databaseClient);
  const deleteIntentRepository = createDeleteIntentRepository(databaseClient);
  const permissionRepository = createPermissionRepository(databaseClient);
  const resourceStateRepository = createResourceStateRepository(databaseClient);

  const awsBaseCredentials =
    env.awsAccessKeyId && env.awsSecretAccessKey
      ? {
          accessKeyId: env.awsAccessKeyId,
          secretAccessKey: env.awsSecretAccessKey,
          sessionToken: env.awsSessionToken
        }
      : undefined;

  const assumeRole = createAssumeRole({
    externalId: env.awsExternalId,
    roleArnTemplate: env.awsAssumeRoleArnTemplate,
    baseCredentials: awsBaseCredentials,
    endpoint: env.awsEndpointUrl,
    tlsInsecure: env.awsTlsInsecure,
    useAccountIdForLocalstack: env.awsUseAccountIdForLocalstack
  });

  const resourceGateway = createCloudControlGateway({
    assumeRole,
    endpoint: env.awsEndpointUrl,
    tlsInsecure: env.awsTlsInsecure
  });

  const authService = createAuthService({
    userRepository,
    permissionRepository
  });

  const adminService = createAdminService({
    userRepository,
    permissionRepository,
    deleteIntentRepository
  });

  const contextService = createContextService({
    userRepository,
    contextRepository,
    resourceGateway
  });

  const resourceService = createResourceService({
    userRepository,
    contextRepository,
    deleteIntentRepository,
    permissionRepository,
    resourceStateRepository,
    resourceGateway
  });

  return {
    adminService,
    authService,
    contextService,
    resourceService,
    close: async () => {
      await databaseClient.end();
    }
  };
};

export type AppContainer = Awaited<ReturnType<typeof createContainer>>;
