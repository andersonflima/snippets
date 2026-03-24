FROM node:20-alpine AS build

WORKDIR /app

COPY package.json package-lock.json ./
COPY tsconfig.base.json ./
COPY apps/frontend/package.json apps/frontend/package.json
COPY packages/shared/package.json packages/shared/package.json

RUN npm ci

COPY apps/frontend apps/frontend
COPY packages/shared packages/shared

RUN npm --workspace @platform/shared run build \
  && npm --workspace @platform/frontend run build

FROM nginx:1.27-alpine AS runtime

COPY --from=build /app/apps/frontend/dist/frontend/browser /usr/share/nginx/html

EXPOSE 80
