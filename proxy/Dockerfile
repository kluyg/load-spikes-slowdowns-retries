# The public entry point of a deployed instance: nginx terminating HTTPS in
# front of the frontend. The official image ships the ACME module, which gets
# and renews the Let's Encrypt certificate, and renders the template below with
# SITE_ADDRESS from the environment each time the container starts.
FROM nginx:1.31-alpine
COPY nginx.conf.template /etc/nginx/templates/nginx.conf.template
COPY 05-require-site-address.sh /docker-entrypoint.d/
