#!/bin/sh
set -eu
: "${RABBITMQ_TEST_URL_A:?Set RABBITMQ_TEST_URL_A to a disposable test vhost}"
fixture=$(cd "$(dirname "$0")" && pwd)
app=$(mktemp -d "${TMPDIR:-/tmp}/yii2-rabbitmq-install.XXXXXXXX")
trap 'rm -rf "$app"' EXIT
cp "$fixture/composer.json" "$fixture/application.php" "$app/"
ln -s "$fixture/../.." "$app/extension"
cd "$app"
if [ "$#" -eq 0 ]; then set -- composer; fi
"$@" install --no-dev --prefer-dist --no-interaction --no-progress
php application.php console
php application.php web
