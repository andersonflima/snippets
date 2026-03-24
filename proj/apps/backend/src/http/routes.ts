import type { AwsCategory, JwtClaims, PermissionScope, UserRole } from '@platform/shared';
import type { FastifyInstance } from 'fastify';
import { z, ZodType } from 'zod';
import type { AppContainer } from '../container.js';
import { allCategories } from '../domain/categories.js';
import { createAppError } from '../domain/errors.js';

const categorySchema = z.custom<AwsCategory>(
  (value) => typeof value === 'string' && allCategories().includes(value as AwsCategory),
  {
    message: 'Categoria invalida.'
  }
);

const parseWithSchema = <T>(schema: ZodType<T>, payload: unknown): T => {
  const parsedPayload = schema.safeParse(payload);

  if (!parsedPayload.success) {
    throw createAppError(
      'VALIDATION_ERROR',
      'Payload invalido.',
      400,
      parsedPayload.error.flatten()
    );
  }

  return parsedPayload.data;
};

const loginSchema = z.object({
  email: z.string().email(),
  password: z.string().min(8)
});

const switchContextSchema = z.object({
  accountId: z.string().regex(/^\d{12}$/),
  region: z.string().min(4),
  category: categorySchema
});

const listResourceSchema = z.object({
  typeName: z.string().min(3).optional()
});

const discoveryResourceSchema = z.object({
  typeName: z.string().min(3)
});

const getResourceSchema = z.object({
  typeName: z.string().min(3),
  identifier: z.string().min(1)
});

const getTemplateSchema = z.object({
  typeName: z.string().min(3)
});

const listResourceStateSchema = z.object({
  typeName: z.string().min(3).optional(),
  identifier: z.string().min(1).optional(),
  limit: z.coerce.number().int().min(1).max(250).optional()
});

const createResourceSchema = z.object({
  typeName: z.string().min(3),
  desiredState: z.record(z.string(), z.unknown())
});

const updateResourceSchema = z.object({
  typeName: z.string().min(3),
  identifier: z.string().min(1),
  desiredState: z.record(z.string(), z.unknown()).default({}),
  patchDocument: z.array(z.record(z.string(), z.unknown())).optional()
});

const deleteIntentSchema = z.object({
  typeName: z.string().min(3),
  resourceId: z.string().min(1)
});

const deleteResourceSchema = z.object({
  intentId: z.string().uuid(),
  typeName: z.string().min(3),
  resourceId: z.string().min(1)
});

const userIdParamsSchema = z.object({
  userId: z.string().min(1)
});

const managedUserRoleSchema = z.custom<UserRole>(
  (value) => value === 'admin' || value === 'operator' || value === 'viewer',
  {
    message: 'Role invalida.'
  }
);

const awsAccountSchema = z.object({
  accountId: z.string().regex(/^\d{12}$/),
  name: z.string().min(1),
  allowedRegions: z.array(z.string().min(4)).min(1)
});

const registerSchema = z.object({
  name: z.string().min(2),
  email: z.string().email(),
  password: z.string().min(8),
  accounts: z.array(awsAccountSchema).min(1)
});

const permissionScopeSchema = z.custom<PermissionScope>((value) => {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  const candidate = value as Partial<PermissionScope>;
  const categoryIsValid =
    candidate.category === '*' ||
    (typeof candidate.category === 'string' &&
      allCategories().includes(candidate.category as AwsCategory));

  const actionIsValid =
    candidate.action === 'list' ||
    candidate.action === 'get' ||
    candidate.action === 'create' ||
    candidate.action === 'update' ||
    candidate.action === 'delete';

  return (
    typeof candidate.accountId === 'string' &&
    candidate.accountId.trim().length > 0 &&
    categoryIsValid &&
    typeof candidate.resourceType === 'string' &&
    candidate.resourceType.trim().length > 0 &&
    actionIsValid
  );
});

const createManagedUserSchema = z.object({
  name: z.string().min(2),
  email: z.string().email(),
  password: z.string().min(8),
  role: managedUserRoleSchema,
  accounts: z.array(awsAccountSchema),
  permissions: z.array(permissionScopeSchema).optional()
});

const updateManagedUserSchema = z
  .object({
    name: z.string().min(2).optional(),
    email: z.string().email().optional(),
    password: z.string().min(8).optional(),
    role: managedUserRoleSchema.optional()
  })
  .refine(
    (value) =>
      value.name !== undefined ||
      value.email !== undefined ||
      value.password !== undefined ||
      value.role !== undefined,
    {
      message: 'Informe ao menos um campo para atualizar.'
    }
  );

const replaceAccountsSchema = z.object({
  accounts: z.array(awsAccountSchema)
});

const replacePermissionsSchema = z.object({
  permissions: z.array(permissionScopeSchema)
});

const adminDeleteUserSchema = z.object({
  intentId: z.string().uuid()
});

const withAuthUserId = (claims: JwtClaims | undefined): string => {
  if (!claims?.sub) {
    throw createAppError('UNAUTHORIZED', 'Token invalido ou ausente.', 401);
  }

  return claims.sub;
};

const assertAdminClaims = (claims: JwtClaims | undefined): JwtClaims => {
  if (!claims?.sub) {
    throw createAppError('UNAUTHORIZED', 'Token invalido ou ausente.', 401);
  }

  if (claims.role !== 'admin') {
    throw createAppError('FORBIDDEN', 'Apenas administradores podem gerenciar ACL e usuarios.', 403);
  }

  return claims;
};

export const registerRoutes = async (
  app: FastifyInstance,
  container: AppContainer
): Promise<void> => {
  app.post('/api/auth/login', async (request, reply) => {
    const payload = parseWithSchema(loginSchema, request.body);

    const authenticatedUser = await container.authService.login(payload.email, payload.password);

    const claims: JwtClaims = {
      sub: authenticatedUser.id,
      email: authenticatedUser.email,
      role: authenticatedUser.role
    };

    const token = app.jwt.sign(claims, {
      expiresIn: '8h'
    });

    return reply.send({
      token,
      user: container.authService.toPublicUser(authenticatedUser)
    });
  });

  app.post('/api/auth/register', async (request, reply) => {
    const payload = parseWithSchema(registerSchema, request.body);

    const registeredUser = await container.authService.register({
      name: payload.name,
      email: payload.email,
      password: payload.password,
      accounts: payload.accounts
    });

    const claims: JwtClaims = {
      sub: registeredUser.id,
      email: registeredUser.email,
      role: registeredUser.role
    };

    const token = app.jwt.sign(claims, {
      expiresIn: '8h'
    });

    return reply.code(201).send({
      token,
      user: container.authService.toPublicUser(registeredUser)
    });
  });

  app.get('/api/auth/me', { preHandler: app.authenticate }, async (request) => {
    const userId = withAuthUserId(request.user as JwtClaims);
    const user = await container.authService.getById(userId);

    return {
      user: container.authService.toPublicUser(user)
    };
  });

  app.get('/api/context/current', { preHandler: app.authenticate }, async (request) => {
    const userId = withAuthUserId(request.user as JwtClaims);
    const context = await container.contextService.getCurrentContext(userId);

    if (!context) {
      return {
        context: null
      };
    }

    return {
      context,
      resourceTypes: await container.resourceService.listTypes(userId)
    };
  });

  app.post('/api/context/switch', { preHandler: app.authenticate }, async (request) => {
    const userId = withAuthUserId(request.user as JwtClaims);
    const payload = parseWithSchema(switchContextSchema, request.body);

    return container.contextService.switchContext({
      userId,
      accountId: payload.accountId,
      region: payload.region,
      category: payload.category
    });
  });

  app.get('/api/resources/types', { preHandler: app.authenticate }, async (request) => {
    const userId = withAuthUserId(request.user as JwtClaims);

    return {
      resourceTypes: await container.resourceService.listTypes(userId)
    };
  });

  app.get('/api/resources', { preHandler: app.authenticate }, async (request) => {
    const userId = withAuthUserId(request.user as JwtClaims);
    const payload = parseWithSchema(listResourceSchema, request.query);

    return {
      resources: await container.resourceService.listResources(userId, payload.typeName)
    };
  });

  app.get('/api/resources/discovery', { preHandler: app.authenticate }, async (request) => {
    const userId = withAuthUserId(request.user as JwtClaims);
    const payload = parseWithSchema(discoveryResourceSchema, request.query);

    return container.resourceService.discoverResources(userId, payload.typeName);
  });

  app.get('/api/resources/details', { preHandler: app.authenticate }, async (request) => {
    const userId = withAuthUserId(request.user as JwtClaims);
    const payload = parseWithSchema(getResourceSchema, request.query);

    return container.resourceService.getResourceDetails(userId, payload.typeName, payload.identifier);
  });

  app.post('/api/resources', { preHandler: app.authenticate }, async (request, reply) => {
    const userId = withAuthUserId(request.user as JwtClaims);
    const payload = parseWithSchema(createResourceSchema, request.body);

    const result = await container.resourceService.createResource(userId, {
      typeName: payload.typeName,
      desiredState: payload.desiredState
    });

    return reply.code(201).send({
      operation: result
    });
  });

  app.get('/api/resources/templates', { preHandler: app.authenticate }, async (request) => {
    return {
      templates: await container.resourceService.listTemplates()
    };
  });

  app.get('/api/resources/templates/:typeName', { preHandler: app.authenticate }, async (request) => {
    const { typeName } = parseWithSchema(
      getTemplateSchema,
      request.params
    );

    const template = await container.resourceService.getTemplateByType(typeName);

    if (!template) {
      throw createAppError(
        'RESOURCE_TEMPLATE_NOT_FOUND',
        `Template para tipo ${typeName} nao foi encontrado.`,
        404
      );
    }

    return {
      template
    };
  });

  app.get('/api/resources/state', { preHandler: app.authenticate }, async (request) => {
    const query = parseWithSchema(listResourceStateSchema, request.query);
    const userId = withAuthUserId(request.user as JwtClaims);

    return {
      history: await container.resourceService.getResourceStateHistory(userId, query)
    };
  });

  app.put('/api/resources', { preHandler: app.authenticate }, async (request) => {
    const userId = withAuthUserId(request.user as JwtClaims);
    const payload = parseWithSchema(updateResourceSchema, request.body);

    const result = await container.resourceService.updateResource(userId, {
      typeName: payload.typeName,
      identifier: payload.identifier,
      desiredState: payload.desiredState,
      patchDocument: payload.patchDocument
    });

    return {
      operation: result
    };
  });

  app.post('/api/resources/delete-intent', { preHandler: app.authenticate }, async (request) => {
    const userId = withAuthUserId(request.user as JwtClaims);
    const payload = parseWithSchema(deleteIntentSchema, request.body);

    const intent = await container.resourceService.requestDeleteIntent(
      userId,
      payload.typeName,
      payload.resourceId
    );

    return {
      intentId: intent.id,
      expiresAt: intent.expiresAt
    };
  });

  app.delete('/api/resources', { preHandler: app.authenticate }, async (request) => {
    const userId = withAuthUserId(request.user as JwtClaims);
    const payload = parseWithSchema(deleteResourceSchema, request.body);

    const result = await container.resourceService.deleteResource(
      userId,
      payload.intentId,
      payload.typeName,
      payload.resourceId
    );

    return {
      operation: result
    };
  });

  app.get('/api/admin/users', { preHandler: app.authenticate }, async (request) => {
    assertAdminClaims(request.user as JwtClaims);

    return {
      users: await container.adminService.listUsers()
    };
  });

  app.post('/api/admin/users', { preHandler: app.authenticate }, async (request, reply) => {
    assertAdminClaims(request.user as JwtClaims);
    const payload = parseWithSchema(createManagedUserSchema, request.body);

    const createdUser = await container.adminService.createUser(payload);

    return reply.code(201).send({
      user: createdUser
    });
  });

  app.patch('/api/admin/users/:userId', { preHandler: app.authenticate }, async (request) => {
    assertAdminClaims(request.user as JwtClaims);

    const params = parseWithSchema(userIdParamsSchema, request.params);
    const payload = parseWithSchema(updateManagedUserSchema, request.body);

    const updatedUser = await container.adminService.updateUser({
      userId: params.userId,
      ...payload
    });

    return {
      user: updatedUser
    };
  });

  app.put('/api/admin/users/:userId/accounts', { preHandler: app.authenticate }, async (request) => {
    assertAdminClaims(request.user as JwtClaims);

    const params = parseWithSchema(userIdParamsSchema, request.params);
    const payload = parseWithSchema(replaceAccountsSchema, request.body);

    const updatedUser = await container.adminService.replaceAccounts(params.userId, payload.accounts);

    return {
      user: updatedUser
    };
  });

  app.get('/api/admin/users/:userId/permissions', { preHandler: app.authenticate }, async (request) => {
    assertAdminClaims(request.user as JwtClaims);

    const params = parseWithSchema(userIdParamsSchema, request.params);
    const permissions = await container.adminService.getPermissions(params.userId);

    return {
      permissions
    };
  });

  app.put(
    '/api/admin/users/:userId/permissions',
    { preHandler: app.authenticate },
    async (request) => {
      assertAdminClaims(request.user as JwtClaims);

      const params = parseWithSchema(userIdParamsSchema, request.params);
      const payload = parseWithSchema(replacePermissionsSchema, request.body);
      const permissions = await container.adminService.replacePermissions(
        params.userId,
        payload.permissions
      );

      return {
        permissions
      };
    }
  );

  app.post(
    '/api/admin/users/:userId/permissions/reset',
    { preHandler: app.authenticate },
    async (request) => {
      assertAdminClaims(request.user as JwtClaims);

      const params = parseWithSchema(userIdParamsSchema, request.params);
      const permissions = await container.adminService.resetPermissionsToRoleDefaults(params.userId);

      return {
        permissions
      };
    }
  );

  app.post(
    '/api/admin/users/:userId/delete-intent',
    { preHandler: app.authenticate },
    async (request) => {
      const claims = assertAdminClaims(request.user as JwtClaims);
      const params = parseWithSchema(userIdParamsSchema, request.params);

      const intent = await container.adminService.requestDeleteUserIntent(claims.sub, params.userId);

      return {
        intentId: intent.id,
        expiresAt: intent.expiresAt
      };
    }
  );

  app.delete('/api/admin/users/:userId', { preHandler: app.authenticate }, async (request, reply) => {
    const claims = assertAdminClaims(request.user as JwtClaims);
    const params = parseWithSchema(userIdParamsSchema, request.params);
    const payload = parseWithSchema(adminDeleteUserSchema, request.body);

    await container.adminService.deleteUser(claims.sub, params.userId, payload.intentId);
    return reply.code(204).send();
  });
};
