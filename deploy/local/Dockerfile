# API Gateway LOCAL — nginx com a config baked na imagem.
#
# Antes a config vinha por bind mount do host (./deploy/local/*.conf), o que
# falha na máquina do serviço quando esses arquivos não estão no caminho do
# compose: o nginx cai no default.conf padrão de /etc/nginx (página welcome).
# Aqui a config viaja dentro da imagem, sem depender do host.
FROM nginx:1.27-alpine

# roteamento por prefixo dos microserviços (substitui o default.conf da imagem)
COPY api-gateway.conf /etc/nginx/conf.d/default.conf
# headers/timeouts comuns, incluídos por cada location via include
COPY proxy_common.conf /etc/nginx/proxy_common.conf

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null 2>&1 || exit 1
