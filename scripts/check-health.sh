#!/bin/bash
kill_descendant_processes() {
    local pid="$1"
    local and_self="${2:-false}"
    if children="$(pgrep -P "$pid")"; then
        for child in $children; do
            kill_descendant_processes "$child" true
        done
    fi
    if [[ "$and_self" == true ]]; then
        echo "Killing $pid"
        kill "$pid"
    fi
    sleep 5
}

# This script starts up the container and hits the /health endpoint until it sees
# a healthy response, failing if it never sees one in 5 tries.

trap 'kill_descendant_processes $$' EXIT

# Launch the container with sabotaged dns to mimic no external internet, which causes startup
# failures if assets are missing in the container image. Using --network none doesn't work because
# we still want to send requests into the container for the health check.
if [ "$1" = "llm" ]; then
  make docker-llm-run env_file=docs/no-auth.env llm_image_name=$2 it="--dns=192.168.0.0" &
elif [ "$1" = "main" ]; then
  make docker-run env_file=docs/no-auth.env image_name=$2 it="--dns=192.168.0.0" &
elif [ "$1" = "policy_validator" ]; then
  make docker-policy-validator-run policy_validator_name=$2 it="--dns=198.168.0.0" &
else
  exit 1 
fi

for i in {1..5}; do
  if curl --fail -s http://localhost:8000/health >/dev/null ; then
    echo "Server started up healthy"
    exit 0
  fi
  echo "Server not healthy yet, sleeping"
  sleep 10
done

echo "Server never started up healthy, exiting"
exit 1
