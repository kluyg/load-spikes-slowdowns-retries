# Deploying a durable public instance (DigitalOcean + Tailscale Funnel)

The laptop-served Funnel URL is only live while your machine is awake. For a
durable public link, run the same compose on a small always-on Droplet. Tailscale Funnel gives free HTTPS and needs **no inbound ports** open
(ingress arrives over Tailscale's own connection), so the box's only exposed
port is SSH.

## 1. Create the Droplet (DigitalOcean web console)

- **Image:** Ubuntu 24.04 (LTS) x64
- **Plan:** Basic → Regular → **$6/mo (1 GB / 1 vCPU)**. 1 GB matters only so the
  in-VM Go build doesn't OOM; the running stack uses far less.
- **Region:** closest to you (or to your users).
- **Auth:** add your SSH key.
- Create, and note the Droplet's IP.

Optionally add a DigitalOcean Cloud Firewall allowing **inbound SSH (22) only**
(leave all outbound open). Funnel needs nothing else inbound.

## 2. Get a Tailscale auth key

Tailscale admin console → **Settings → Keys → Generate auth key**:
- **Reusable:** on (so you can re-run the deploy)
- **Ephemeral:** off (the node should persist)
- Copy the `tskey-auth-...` value.

Funnel is already enabled on your tailnet (the laptop used it), so the new node
inherits it. If Funnel doesn't turn on, check the ACL `nodeAttrs` grant for
`funnel` covers this node.

## 3. Bring it up on the Droplet

```sh
ssh root@<DROPLET_IP>

# Docker Engine + compose plugin
curl -fsSL https://get.docker.com | sh

# The app
git clone https://github.com/kluyg/load-spikes-slowdowns-retries.git
cd load-spikes-slowdowns-retries

# Tailscale auth key (gitignored, never committed)
printf 'TS_AUTHKEY=tskey-auth-XXXXXXXX\n' > .env
chmod 600 .env

# Full stack, public via Funnel
docker compose --profile tailnet up -d --build
```

## 4. Claim the canonical URL (optional but tidy)

To serve at the same `https://frontend.<tailnet>.ts.net` you already tested, free
that hostname from the laptop first:

```sh
# on your laptop:
docker compose --profile tailnet down
```
Then in the Tailscale admin console → **Machines**, remove the old `frontend`
(laptop) node. If the Droplet already registered as `frontend-1`, restart its
sidecar so it reclaims `frontend`:

```sh
docker compose --profile tailnet restart ts-frontend
```

## 5. Verify

```sh
docker compose --profile tailnet exec ts-frontend tailscale funnel status
```
Prints the public URL. Open it from a phone on cellular (off any tailnet) to
confirm it's truly public. `restart: unless-stopped` keeps the stack up across
reboots.

## Teardown

```sh
docker compose --profile tailnet down
```
…and destroy the Droplet to stop billing.
