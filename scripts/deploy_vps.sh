#!/usr/bin/env bash
# Provisiona VPS Ubuntu/Debian para marketing-pipeline.
# Idempotente: pode rodar múltiplas vezes.
#
# Uso (como root, no VPS):
#   bash deploy_vps.sh
#
# Pré-requisitos:
#   - Chave SSH pública instalada em /root/.ssh/authorized_keys
#   - /opt/marketing-pipeline/.env preenchido (ou será criado a partir do template)

set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/marleyabe/marketing-pipeline.git}"
REPO_BRANCH="${REPO_BRANCH:-main}"
APP_DIR="${APP_DIR:-/opt/marketing-pipeline}"
INFRA_CADDY_DIR="${INFRA_CADDY_DIR:-/opt/infra/caddy}"
DEPLOY_USER="${DEPLOY_USER:-deploy}"

log() { echo "[deploy] $*"; }

require_root() {
    if [ "$(id -u)" -ne 0 ]; then
        echo "Rode como root" >&2
        exit 1
    fi
}

install_base_packages() {
    log "instalando pacotes base"
    apt-get update -qq
    apt-get install -y -qq ca-certificates curl gnupg ufw git
}

install_docker() {
    if command -v docker >/dev/null 2>&1; then
        log "docker já instalado"
        return
    fi
    log "instalando docker"
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc 2>/dev/null \
        || curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
    . /etc/os-release
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/${ID} ${VERSION_CODENAME} stable" \
        > /etc/apt/sources.list.d/docker.list
    apt-get update -qq
    apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
}

configure_firewall() {
    log "configurando UFW (22, 80, 443)"
    ufw --force reset >/dev/null
    ufw default deny incoming
    ufw default allow outgoing
    ufw allow 22/tcp
    ufw allow 80/tcp
    ufw allow 443/tcp
    ufw --force enable
}

ensure_deploy_user() {
    if id "$DEPLOY_USER" >/dev/null 2>&1; then
        log "usuário $DEPLOY_USER já existe"
    else
        log "criando usuário $DEPLOY_USER"
        adduser --disabled-password --gecos "" "$DEPLOY_USER"
    fi
    usermod -aG docker "$DEPLOY_USER"
    mkdir -p "/home/$DEPLOY_USER/.ssh"
    if [ -f /root/.ssh/authorized_keys ]; then
        cp /root/.ssh/authorized_keys "/home/$DEPLOY_USER/.ssh/authorized_keys"
        chown -R "$DEPLOY_USER:$DEPLOY_USER" "/home/$DEPLOY_USER/.ssh"
        chmod 700 "/home/$DEPLOY_USER/.ssh"
        chmod 600 "/home/$DEPLOY_USER/.ssh/authorized_keys"
    fi
}

harden_ssh() {
    log "desabilitando login por senha e root via SSH"
    sed -i -E 's/^#?PermitRootLogin.*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config
    sed -i -E 's/^#?PasswordAuthentication.*/PasswordAuthentication no/' /etc/ssh/sshd_config
    sed -i -E 's/^#?ChallengeResponseAuthentication.*/ChallengeResponseAuthentication no/' /etc/ssh/sshd_config
    systemctl reload ssh || systemctl reload sshd
}

clone_or_update_repo() {
    if [ -d "$APP_DIR/.git" ]; then
        log "atualizando repo em $APP_DIR"
        git -C "$APP_DIR" fetch --quiet origin
        git -C "$APP_DIR" checkout "$REPO_BRANCH"
        git -C "$APP_DIR" reset --hard "origin/$REPO_BRANCH"
    else
        log "clonando repo em $APP_DIR"
        git clone --branch "$REPO_BRANCH" "$REPO_URL" "$APP_DIR"
    fi
    chown -R "$DEPLOY_USER:$DEPLOY_USER" "$APP_DIR"
}

ensure_env_file() {
    if [ ! -f "$APP_DIR/.env" ]; then
        log "criando .env a partir do template — PREENCHA antes de subir"
        cp "$APP_DIR/.env.prod.example" "$APP_DIR/.env"
        chmod 600 "$APP_DIR/.env"
        chown "$DEPLOY_USER:$DEPLOY_USER" "$APP_DIR/.env"
    else
        log ".env já existe, mantendo"
    fi
}

ensure_infra_caddy() {
    if [ ! -d "$INFRA_CADDY_DIR" ]; then
        log "provisionando infra Caddy compartilhada em $INFRA_CADDY_DIR"
        mkdir -p "$INFRA_CADDY_DIR"
        cp -r "$APP_DIR/deploy/infra-caddy/." "$INFRA_CADDY_DIR/"
        if [ ! -f "$INFRA_CADDY_DIR/Caddyfile" ]; then
            cp "$INFRA_CADDY_DIR/Caddyfile.example" "$INFRA_CADDY_DIR/Caddyfile"
            log "⚠  edite $INFRA_CADDY_DIR/Caddyfile com seus domínios antes de subir"
        fi
    else
        log "infra Caddy já existe em $INFRA_CADDY_DIR, mantendo"
    fi
}

infra_caddy_up() {
    log "subindo infra Caddy compartilhada"
    cd "$INFRA_CADDY_DIR"
    docker compose up -d
}

compose_up() {
    log "subindo stack (compose)"
    cd "$APP_DIR"
    docker compose -f docker-compose.yaml -f docker-compose.prod.yaml pull || true
    docker compose -f docker-compose.yaml -f docker-compose.prod.yaml up -d --build
}

main() {
    require_root
    install_base_packages
    install_docker
    configure_firewall
    ensure_deploy_user
    clone_or_update_repo
    ensure_env_file
    ensure_infra_caddy
    harden_ssh

    if grep -q '^POSTGRES_PASSWORD=$' "$APP_DIR/.env" 2>/dev/null; then
        log "⚠  .env ainda não foi preenchido — edite $APP_DIR/.env e rode de novo para subir o stack"
        exit 0
    fi

    if grep -q 'seudominio.com' "$INFRA_CADDY_DIR/Caddyfile" 2>/dev/null; then
        log "⚠  Caddyfile ainda tem placeholders — edite $INFRA_CADDY_DIR/Caddyfile e rode de novo"
        exit 0
    fi

    infra_caddy_up
    compose_up
    log "ok — verifique: docker compose ps"
}

main "$@"
