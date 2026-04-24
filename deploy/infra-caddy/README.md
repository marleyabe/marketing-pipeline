# Infra: Caddy compartilhado

Reverse proxy único com HTTPS automático (Let's Encrypt) para múltiplos
projetos no mesmo VPS.

## Setup no VPS

```bash
# 1) Este diretório é só um template. No servidor, copie para /opt/infra/caddy:
mkdir -p /opt/infra/caddy
cp -r deploy/infra-caddy/. /opt/infra/caddy/
cd /opt/infra/caddy

# 2) Edite o Caddyfile com seus domínios/containers
cp Caddyfile.example Caddyfile
vim Caddyfile

# 3) Sobe
docker compose up -d
```

A rede externa `caddy-net` é criada aqui. Outros projetos precisam
declarar essa rede como `external: true` e anexar seus serviços HTTP
nela — o Caddy então os alcança pelo nome do container.

## Adicionando um novo projeto

No `docker-compose.yaml` do projeto:

```yaml
services:
  web:
    # ...
    networks:
      - default
      - caddy-net

networks:
  caddy-net:
    external: true
```

No Caddyfile aqui:

```
novo.dominio.com {
    reverse_proxy <nome-do-container>:<porta>
}
```

Depois: `docker exec infra-caddy-caddy-1 caddy reload --config /etc/caddy/Caddyfile`
(ou `docker compose restart caddy`).
