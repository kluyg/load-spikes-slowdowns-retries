# Deploying a durable public instance (DigitalOcean + Tailscale Funnel)

The laptop-served Funnel URL is only live while your machine is awake. For a
durable public link, run the same compose on a small always-on Droplet. Tailscale Funnel gives free HTTPS and needs **no inbound ports** open
(ingress arrives over Tailscale's own connection), so the box's only exposed
port is SSH. The compose file publishes Redis (6379) and the frontend (8080) on
`127.0.0.1` only, which is what keeps that true: Docker's port publishing
bypasses `ufw`, so a port published on all interfaces would be reachable from
the internet no matter what the host firewall says.

## 1. Create the Droplet (DigitalOcean web console)

- **Image:** Ubuntu 24.04 (LTS) x64
- **Plan:** Basic → Regular → **$6/mo (1 GB / 1 vCPU)**. 1 GB matters only so the
  in-VM Go build doesn't OOM; the running stack uses far less.
- **Region:** closest to you (or to your users). Not every datacenter offers
  the Basic Regular plans; if the $6 plan shows as unavailable (NYC2 did in
  November 2026), pick another datacenter in the same city, such as NYC3.
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

If another machine on your tailnet already holds the name `frontend` (the
laptop you tested from, or an earlier Droplet that has since been destroyed),
the new node registers as `frontend-1` and serves at
`https://frontend-1.<tailnet>.ts.net`. To serve at
`https://frontend.<tailnet>.ts.net` instead:

1. If the old node is your laptop, stop its sidecar first:

   ```sh
   # on your laptop:
   docker compose --profile tailnet down
   ```
2. In the Tailscale admin console → **Machines**, open the old `frontend`
   node's **⋯** menu and choose **Remove**.
3. On the same page, open the **⋯** menu of `frontend-1` → **Edit machine
   name**, turn off **Auto-generate from OS hostname**, and set the name to
   `frontend`.

The rename is needed: once a node has been given `frontend-1`, restarting the
sidecar keeps that name even after `frontend` is free. Funnel follows the new
name within a few seconds, and the `frontend-1` URL stops working. To skip the
rename altogether, do steps 1 and 2 before the first
`docker compose --profile tailnet up` on the Droplet.

## 5. Verify

```sh
docker compose --profile tailnet exec ts-frontend tailscale funnel status
```
Prints the public URL. Open it from a phone on cellular (off any tailnet) to
confirm it's truly public. `restart: unless-stopped` keeps the stack up across
reboots.

## Multiple concurrent users

The public link is safe to share: each visitor gets an isolated room (its own
queue, config, graphs, and a dedicated backend worker pool), so people driving
it at the same time don't interfere. Cost scales with the number of *live*
rooms — each is a handful of goroutines and a few small Redis keys, and idle
rooms are torn down automatically.

The frontend caps the number of live rooms at 20 (`MAX_ROOMS`). A visitor past
the cap sees "all rooms are in use" and is connected automatically once a room
frees up, about 20 seconds after its tab closes. The default is a rough
estimate for the $6 Droplet's single vCPU, where the limit is the request load
of the tabs themselves rather than memory; raise `MAX_ROOMS` in
`docker-compose.yml` on a bigger machine, or set it to 0 to remove the cap.

## Teardown

```sh
docker compose --profile tailnet down
```
…and destroy the Droplet to stop billing.
