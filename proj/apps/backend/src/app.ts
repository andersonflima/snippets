import cors from '@fastify/cors';
import jwt from '@fastify/jwt';
import fastify, { FastifyReply, FastifyRequest } from 'fastify';
import type { JwtClaims } from '@platform/shared';
import { createContainer } from './container.js';
import { env } from './config/env.js';
import { isAppError } from './domain/errors.js';
import { registerRoutes } from './http/routes.js';

declare module 'fastify' {
  interface FastifyInstance {
    authenticate: (request: FastifyRequest, reply: FastifyReply) => Promise<void>;
  }
}

export const createApp = async () => {
  const app = fastify({
    logger: true
  });

  const container = await createContainer();

  await app.register(cors, {
    origin: true
  });

  await app.register(jwt, {
    secret: env.jwtSecret
  });

  app.decorate('authenticate', async (request: FastifyRequest, reply: FastifyReply) => {
    try {
      await request.jwtVerify<JwtClaims>();
    } catch {
      await reply.code(401).send({
        code: 'UNAUTHORIZED',
        message: 'Token invalido ou ausente.'
      });
    }
  });

  app.setErrorHandler((error, request, reply) => {
    if (isAppError(error)) {
      return reply.code(error.statusCode).send({
        code: error.code,
        message: error.message,
        details: error.details ?? null
      });
    }

    request.log.error(error);

    return reply.code(500).send({
      code: 'INTERNAL_SERVER_ERROR',
      message: 'Falha interna no servidor.'
    });
  });

  app.addHook('onClose', async () => {
    await container.close();
  });

  await registerRoutes(app, container);
  return app;
};
