#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# HOW THIS SCRIPT WORKS
#
# 1) If certs are missing, it creates TLS helper files + dummy certs.
# 2) If certs exist, it asks before replacing them via certbot.
# 3) Typical flow:
#    - run once before first deploy (creates dummy cert)
#    - run again after stack is up (fetches Let's Encrypt certs)
# 4) Usage: LETSENCRYPT_STAGING=1 ./scripts/deploy_certbot.sh
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load env in this order:
# 1) project root .env
# 2) scripts/.env (override for local script usage)
set -a
if [[ -f "$ROOT_DIR/.env" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
fi
if [[ -f "$SCRIPT_DIR/.env" ]]; then
  # shellcheck disable=SC1091
  source "$SCRIPT_DIR/.env"
fi
set +a

COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/docker-compose.yml}"
REVERSE_PROXY_SERVICE="${REVERSE_PROXY_SERVICE:-reverse_proxy}"
CERTBOT_SERVICE="${CERTBOT_SERVICE:-certbot}"

RSA_KEY_SIZE="${LETSENCRYPT_RSA_KEY_SIZE:-4096}"
EMAIL="${LETSENCRYPT_EMAIL:-}"
DOMAINS_CSV="${LETSENCRYPT_DOMAINS:-}"
STAGING_FLAG="${LETSENCRYPT_STAGING:-1}" # 1 = staging, 0 = production

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "Compose file not found: $COMPOSE_FILE"
  exit 1
fi

if [[ -z "$DOMAINS_CSV" ]]; then
  echo "LETSENCRYPT_DOMAINS is required (comma-separated), e.g. artham.live,www.artham.live"
  exit 1
fi

IFS=',' read -r -a raw_domains <<< "$DOMAINS_CSV"
domains=()
domain_args=()
for domain in "${raw_domains[@]}"; do
  d="$(echo "$domain" | xargs)"
  if [[ -n "$d" ]]; then
    domains+=("$d")
    domain_args+=("-d" "$d")
  fi
done

if [[ ${#domains[@]} -eq 0 ]]; then
  echo "No valid domains found in LETSENCRYPT_DOMAINS"
  exit 1
fi

primary_domain="${domains[0]}"
live_cert_path="/etc/letsencrypt/live/$primary_domain"

run_compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

run_certbot_sh() {
  local cmd="$1"
  run_compose run --rm --entrypoint sh "$CERTBOT_SERVICE" -lc "$cmd"
}

ensure_tls_params() {
  echo "<--------> Ensuring recommended TLS parameter files <-------->"
  run_certbot_sh '
    set -e
    if [ ! -f /etc/letsencrypt/options-ssl-nginx.conf ]; then
      wget -q -O /etc/letsencrypt/options-ssl-nginx.conf \
        https://raw.githubusercontent.com/certbot/certbot/master/certbot-nginx/certbot_nginx/_internal/tls_configs/options-ssl-nginx.conf
    fi
    if [ ! -f /etc/letsencrypt/ssl-dhparams.pem ]; then
      wget -q -O /etc/letsencrypt/ssl-dhparams.pem \
        https://raw.githubusercontent.com/certbot/certbot/master/certbot/certbot/ssl-dhparams.pem
    fi
  '
}

create_dummy_cert() {
  echo "<--------> Creating dummy certificate for ${domains[*]} <-------->"
  run_certbot_sh "
    set -e
    mkdir -p '$live_cert_path'
    openssl req -x509 -nodes -newkey rsa:$RSA_KEY_SIZE -days 1 \
      -keyout '$live_cert_path/privkey.pem' \
      -out '$live_cert_path/fullchain.pem' \
      -subj '/CN=localhost'
  "
}

has_cert_files() {
  run_certbot_sh "[ -f '$live_cert_path/fullchain.pem' ] && [ -f '$live_cert_path/privkey.pem' ]" >/dev/null 2>&1
}

if ! has_cert_files; then
  echo "<--------> Cert files not found for $primary_domain. Bootstrapping dummy cert. <-------->"
  ensure_tls_params
  create_dummy_cert
  echo "<--------> Bootstrap done. Start/restart stack, then run this script again for Let's Encrypt certs. <-------->"
  exit 0
fi

read -r -p "<--------> Existing cert data found for ${domains[*]}. Replace it? (y/n) <--------> " decision
if [[ "$decision" != "Y" && "$decision" != "y" ]]; then
  echo "Cancelled."
  exit 0
fi

ensure_tls_params

echo "<--------> Starting $REVERSE_PROXY_SERVICE <-------->"
run_compose up --force-recreate -d "$REVERSE_PROXY_SERVICE"

echo "<--------> Removing existing cert files for $primary_domain <-------->"
run_certbot_sh "
  rm -rf /etc/letsencrypt/live/$primary_domain && \
  rm -rf /etc/letsencrypt/archive/$primary_domain && \
  rm -f /etc/letsencrypt/renewal/$primary_domain.conf
"

echo "<--------> Requesting Let's Encrypt cert for ${domains[*]} <-------->"
email_arg=(--register-unsafely-without-email)
if [[ -n "$EMAIL" ]]; then
  email_arg=(--email "$EMAIL")
fi

staging_arg=()
if [[ "$STAGING_FLAG" != "0" ]]; then
  staging_arg=(--staging)
fi

run_compose run --rm --entrypoint certbot "$CERTBOT_SERVICE" certonly \
  --webroot -w /var/www/certbot \
  "${staging_arg[@]}" \
  "${email_arg[@]}" \
  "${domain_args[@]}" \
  --rsa-key-size "$RSA_KEY_SIZE" \
  --agree-tos \
  --force-renewal

echo "<--------> Reloading $REVERSE_PROXY_SERVICE <-------->"
run_compose exec "$REVERSE_PROXY_SERVICE" nginx -s reload

echo "<--------> Done <-------->"
