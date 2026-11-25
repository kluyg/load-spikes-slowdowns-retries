#!/bin/sh
# Runs before nginx starts (the image executes /docker-entrypoint.d/*.sh).
# Without a hostname the rendered config is invalid and nginx's own error does
# not say why.
if [ -z "$SITE_ADDRESS" ]; then
	echo "proxy: SITE_ADDRESS is not set. Put SITE_ADDRESS=<your hostname> in .env; see DEPLOY.md." >&2
	exit 1
fi
