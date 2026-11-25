# Deploying a durable public instance (DigitalOcean + nginx + Let's Encrypt)

`docker compose up` serves the demo on `localhost` only. For a public link, run
the same compose file on a small always-on Droplet with the `public` profile.
That adds one container, an nginx reverse proxy that terminates HTTPS on ports
80 and 443 and gets and renews its own Let's Encrypt certificate. The frontend
(8080) and Redis (6379) stay published on `127.0.0.1` only.

The proxy is deliberately a thin one. This demo's traffic is thousands of tiny
polling requests a second, so whatever CPU the entry point uses comes straight
out of the system being demonstrated; see [Why nginx](#why-nginx) below.

## 1. Create the Droplet (DigitalOcean web console)

- **Image:** Ubuntu 24.04 (LTS) x64
- **Plan:** Basic → Regular → **$18/mo (2 GB / 2 vCPU)** is what the numbers
  [below](#what-the-2-vcpu-droplet-delivers) were measured on. The $6 plan
  (1 GB / 1 vCPU) runs the stack, but the retry-storm scenarios already use
  both cores of the larger one.
- **Region:** closest to you (or to your users). Not every datacenter offers
  every Basic Regular plan; if the one you want shows as unavailable, pick
  another datacenter in the same city.
- **Auth:** add your SSH key.
- Create, and note the Droplet's IP.

## 2. Point a hostname at it

Add an `A` record for the name you want (a subdomain is enough) with the
Droplet's IP as its value. If the DNS provider can also proxy traffic
(Cloudflare's orange cloud), turn that off for this record: Let's Encrypt has
to reach nginx directly, and so should the visitors.

## 3. Firewall

Add a DigitalOcean Cloud Firewall to the Droplet allowing **inbound TCP 22, 80
and 443** and all outbound. Port 80 has to stay open: Let's Encrypt validates
the hostname over it, at first issue and at every renewal.

Use the Cloud Firewall rather than `ufw` on the host. Docker's port publishing
bypasses `ufw`, so a port published on all interfaces is reachable from the
internet no matter what the host firewall says. That is also why the compose
file binds Redis and the frontend to `127.0.0.1`.

## 4. Bring it up on the Droplet

```sh
ssh root@<DROPLET_IP>

# Docker Engine + compose plugin
curl -fsSL https://get.docker.com | sh

# The app
git clone https://github.com/kluyg/load-spikes-slowdowns-retries.git
cd load-spikes-slowdowns-retries

# The public hostname (gitignored, never committed)
printf 'SITE_ADDRESS=demo.example.com\n' > .env

# Full stack, public over HTTPS
docker compose --profile public up -d --build
```

The first start requests the certificate, which takes well under a minute.
Until it arrives, HTTPS connections fail. The certificate and the ACME account
key live in the `acme-state` volume; keep it across redeploys, because Let's
Encrypt limits how often it will issue for the same hostname.

## 5. Verify

```sh
curl -sI https://demo.example.com/ | head -1      # HTTP/2 200
docker compose --profile public logs proxy        # if not: issuance errors show here
```

Then open the URL in a browser. `restart: unless-stopped` keeps the stack up
across reboots.

To measure what the deployment can do, run the load generator against it from
another machine and compare with the README's tables:

```sh
cd loadgen && go build -o loadgen . && ./loadgen -url https://demo.example.com
```

## 6. Redeploying

By hand, on the Droplet:

```sh
./deploy.sh
```

It fast-forwards the checkout to `origin/main`, rebuilds and restarts what
changed, and waits for the site to answer. A deploy restarts the frontend, so
open tabs lose their room and reconnect to a fresh one.

### From GitHub Actions

`.github/workflows/deploy.yml` runs the same script over SSH each time CI
passes on `main` (and on demand, from the Actions tab). It stays inactive until
the repository has a `DEPLOY_HOST` variable. To set it up:

1. On the Droplet, create a key pair for deploys only, and allow it to do
   nothing but run the script:

   ```sh
   ssh-keygen -t ed25519 -N '' -C github-actions-deploy -f /root/deploy_key
   printf 'restrict,command="/root/load-spikes-slowdowns-retries/deploy.sh" %s\n' \
     "$(cat /root/deploy_key.pub)" >> /root/.ssh/authorized_keys
   ```

   `restrict` plus a forced command means that whoever holds this key can
   trigger a deploy of a commit that is already on `main`, and nothing else:
   no shell, no port forwarding, no file copy.
2. In the GitHub repository, under **Settings → Secrets and variables →
   Actions**:
   - secret `DEPLOY_SSH_KEY`: the contents of `/root/deploy_key`
   - variable `DEPLOY_HOST`: the hostname from step 2
   - variable `DEPLOY_KNOWN_HOSTS`: the output of
     `ssh-keyscan -t ed25519 <hostname>`, run from a machine that already
     trusts the Droplet
3. Delete `/root/deploy_key` from the Droplet (the `.pub` half can stay).

## Why nginx

Measured with `loadgen -scenario margin-retry` (spike with immediate retry ×5,
Deadline + margin) against the stack pinned to 2 CPUs, with the load generator
on other cores and no network in between:

| Entry point | Total goodput | Goodput in the spike | Its CPU, average / peak |
|---|---|---|---|
| none (plain HTTP to the frontend) | 55% | 73 ops/s | |
| nginx, this configuration | 54% | 72 ops/s | 17% / 39% of a core |
| Caddy, upstream keep-alive pool raised | 51–52% | 64–68 ops/s | 50% / 95% |

Three settings matter more than the choice of proxy, and
`proxy/nginx.conf.template` sets all of them:

- **Reuse connections to the frontend.** A proxy that keeps only a few idle
  upstream connections dials a new one for most polls. Caddy's default of 32
  did exactly that here, and a CPU profile showed 87% of its time inside
  `connect()`.
- **Don't cap requests per client connection.** A tab makes all its requests
  over one HTTP/2 connection; nginx's default limit of 1000 requests would
  force a reconnect and a new TLS handshake every few seconds.
- **Raise the open-file limit.** Docker starts nginx's workers with a soft
  limit of 1024 descriptors. The heaviest scenarios need more, and at the
  limit nginx fails requests instead of passing them to the frontend.

## What the 2-vCPU Droplet delivers

The same scenario against the live Droplet (SFO2, Basic 2 vCPU / 2 GB), with
the load generator on a home connection 30 to 55 ms away:

| Entry point on the Droplet | Total goodput | Goodput in the spike | After the spike |
|---|---|---|---|
| Tailscale Funnel sidecar (the previous setup) | 22–28% | 30–35 ops/s | never recovers |
| nginx, this configuration | 41–43% | 45–50 ops/s | recovers in 4–6 s |

During the storm nginx used 40% of a core on average and about 80% at its
peak, where the sidecar had used 90 to 110% and up to 170%.

The scenarios without a retry storm match the README on this Droplet:
Deadline + margin without retries reaches 72 ops/s in the spike, and load
shedding 76 ops/s. Deadline + margin *with* retry ×5 stops at 45 to 50 ops/s
against the README's 73, because for about 12 seconds the host has no idle CPU
left: roughly 50% user, 29% system and 20% softirq, with under 1% stolen by
the hypervisor. The README's figure for that scenario needs more CPU than this
plan has.

## Multiple concurrent users

The public link is safe to share: each visitor gets an isolated room (its own
queue, config, graphs, and a dedicated backend worker pool), so people driving
it at the same time don't interfere. Cost scales with the number of *live*
rooms — each is a handful of goroutines and a few small Redis keys, and idle
rooms are torn down automatically.

The frontend caps the number of live rooms at 20 (`MAX_ROOMS`). A visitor past
the cap sees "all rooms are in use" and is connected automatically once a room
frees up, about 20 seconds after its tab closes. The default is a rough
estimate for a single shared vCPU, where the limit is the request load of the
tabs themselves rather than memory; raise `MAX_ROOMS` in
`docker-compose.yml` on a bigger machine, or set it to 0 to remove the cap.

## Teardown

```sh
docker compose --profile public down
```
…and destroy the Droplet to stop billing. Remove the DNS record too, so the
name does not keep pointing at an address someone else may be given.
