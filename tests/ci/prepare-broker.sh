#!/usr/bin/env bash
# Configure TLS and two disposable vhosts in the test job's RabbitMQ container.
set -euo pipefail
broker=${1:?pass the disposable RabbitMQ container ID}
certs=${2:?pass a new directory for the test certificates}
mkdir -p "$certs"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj '/CN=RabbitMQ test CA' \
    -keyout "$certs/ca.key" -out "$certs/ca.pem"
openssl req -new -newkey rsa:2048 -nodes -subj '/CN=localhost' \
    -keyout "$certs/server.key" -out "$certs/server.csr"
echo 'subjectAltName=DNS:localhost,DNS:rabbitmq,IP:127.0.0.1' > "$certs/extensions"
openssl x509 -req -days 2 -in "$certs/server.csr" -CA "$certs/ca.pem" -CAkey "$certs/ca.key" \
    -CAcreateserial -extfile "$certs/extensions" -out "$certs/server.pem"

docker exec "$broker" mkdir -p /etc/rabbitmq/test-tls
for file in ca.pem server.pem server.key; do
    docker cp "$certs/$file" "$broker:/etc/rabbitmq/test-tls/$file"
done
docker exec "$broker" chown -R rabbitmq:rabbitmq /etc/rabbitmq/test-tls
docker exec -i "$broker" sh -c 'cat > /etc/rabbitmq/conf.d/90-test-tls.conf' <<'CONF'
listeners.ssl.default = 5671
ssl_options.cacertfile = /etc/rabbitmq/test-tls/ca.pem
ssl_options.certfile = /etc/rabbitmq/test-tls/server.pem
ssl_options.keyfile = /etc/rabbitmq/test-tls/server.key
ssl_options.verify = verify_none
ssl_options.fail_if_no_peer_cert = false
CONF
docker restart "$broker"
deadline=$((SECONDS + 90))
until docker exec "$broker" rabbitmq-diagnostics -q check_running >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
        docker logs --tail 80 "$broker"
        exit 1
    fi
    sleep 2
done
for vhost in a b; do
    docker exec "$broker" rabbitmqctl add_vhost "$vhost"
    docker exec "$broker" rabbitmqctl set_permissions -p "$vhost" rabbit_test '.*' '.*' '.*'
done
