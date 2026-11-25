#!/usr/bin/env bash
# Deploys a commit of main on the machine this checkout lives on. See DEPLOY.md.
#
#   ./deploy.sh            deploy the tip of origin/main
#   ./deploy.sh <sha>      deploy that commit, which must be on origin/main
#
# The GitHub Actions deploy job reaches this script through an SSH key that is
# only allowed to run it (a forced command in authorized_keys); the commit then
# arrives in SSH_ORIGINAL_COMMAND instead of as an argument.
set -euo pipefail

# Everything is inside main() so that bash has read the whole file before the
# `git merge` below replaces it with a newer version.
main() {
	local sha="${SSH_ORIGINAL_COMMAND:-${1:-}}"
	cd "$(dirname "$(readlink -f "$0")")"

	git fetch --quiet origin main
	if [[ -z "$sha" ]]; then
		sha="$(git rev-parse origin/main)"
	elif [[ ! "$sha" =~ ^[0-9a-f]{40}$ ]]; then
		echo "deploy: expected a full commit sha, got: $sha" >&2
		exit 2
	elif ! git merge-base --is-ancestor "$sha" origin/main; then
		echo "deploy: $sha is not on origin/main" >&2
		exit 2
	fi

	# Fast-forward only: a checkout with local commits or conflicting edits
	# stops here instead of being overwritten.
	git merge --ff-only "$sha"
	echo "deploy: now at $(git log -1 --format='%h %s')"

	docker compose --profile public up --detach --build --remove-orphans
	docker image prune --force >/dev/null

	local site
	site="$(sed -n 's/^SITE_ADDRESS=//p' .env)"
	for _ in $(seq 30); do
		if curl --fail --silent --show-error --output /dev/null --max-time 5 \
			--resolve "$site:443:127.0.0.1" "https://$site/" 2>/dev/null; then
			echo "deploy: https://$site/ is serving"
			return 0
		fi
		sleep 2
	done
	echo "deploy: https://$site/ did not answer within a minute" >&2
	docker compose --profile public ps >&2
	exit 1
}

main "$@"
