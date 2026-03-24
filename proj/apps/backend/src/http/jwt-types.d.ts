import type { JwtClaims } from '@platform/shared';

declare module '@fastify/jwt' {
  interface FastifyJWT {
    payload: JwtClaims;
    user: JwtClaims;
  }
}
