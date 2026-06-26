# --- build do bundle Angular -------------------------------------------------
FROM node:22-alpine AS build

WORKDIR /app

# instala deps com lockfile (cache de camada enquanto package*.json não mudar)
COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build -- --configuration production

# --- runtime: nginx servindo o SPA e fazendo proxy do BFF --------------------
FROM nginx:1.27-alpine

# o frontend usa baseUrl "/bff": o nginx repassa /bff/* ao serviço do BFF.
# Ajustável por ambiente (compose/k8s) via env, sem rebuild.
ENV BFF_UPSTREAM=http://bff:8081
# habilita o script da imagem que popula ${NGINX_LOCAL_RESOLVERS} a partir do
# /etc/resolv.conf (DNS portável Docker/k8s) para o resolver do proxy do BFF.
ENV NGINX_ENTRYPOINT_LOCAL_RESOLVERS=1

# template processado por envsubst no start do nginx (substitui só ${BFF_UPSTREAM}).
COPY nginx.default.conf.template /etc/nginx/templates/default.conf.template
COPY --from=build /app/dist/microservicos-actions-console/browser /usr/share/nginx/html

EXPOSE 80
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
    CMD wget -qO- http://127.0.0.1:80/healthz >/dev/null 2>&1 || exit 1
