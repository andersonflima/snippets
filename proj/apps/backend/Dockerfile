FROM node:20-alpine AS build

WORKDIR /app

COPY package.json package-lock.json ./
COPY tsconfig.base.json ./
COPY apps/backend/package.json apps/backend/package.json
COPY packages/shared/package.json packages/shared/package.json

RUN npm ci

COPY apps/backend apps/backend
COPY packages/shared packages/shared

RUN npm --workspace @platform/shared run build \
  && npm --workspace @platform/backend run build

FROM node:20-alpine AS runtime

WORKDIR /app

ENV NODE_ENV=production

COPY --from=build /app /app

WORKDIR /app/apps/backend

EXPOSE 3000

CMD ["node", "dist/index.js"]
