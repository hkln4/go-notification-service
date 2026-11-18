#!/usr/bin/env bash
# Runs the burst scenario against the docker-compose stack and reports how
# the system drained it. Needs the stack up: docker compose up -d --scale worker=3
set -euo pipefail
cd "$(dirname "$0")/.."

NETWORK=$(docker network ls --format '{{.Name}}' | grep -m1 'notification-service_default')
psql() { docker compose exec -T postgres psql -U notification -d notification -tA -c "$1"; }

echo "== reset"
psql "TRUNCATE outbox, delivery_attempts, notifications, batches CASCADE" >/dev/null
curl -fsS -X POST localhost:9090/stats/reset
echo "workers: $(docker compose ps -q worker | wc -l | tr -d ' ')"

echo "== load (k6)"
start=$(date +%s)
docker run --rm -i --network "$NETWORK" -e API_URL=http://api:8080 -e DURATION="${DURATION:-30s}" \
  grafana/k6:2.4.0 run --quiet - < loadtest/burst.js
load_end=$(date +%s)

echo "== draining"
while :; do
  left=$(psql "SELECT count(*) FROM notifications WHERE status IN ('pending','processing','retrying')")
  [[ "$left" == 0 ]] && break
  printf '\r%6s left' "$left"; sleep 1
done
end=$(date +%s)
printf '\rload %ss, drained %ss after the load stopped (%ss total)\n' \
  $((load_end - start)) $((end - load_end)) $((end - start))

echo "== status"
psql "SELECT status, count(*) FROM notifications GROUP BY 1 ORDER BY 1"

echo "== end-to-end latency by priority (created → sent), seconds"
psql "SELECT CASE priority WHEN 2 THEN 'high' WHEN 1 THEN 'normal' ELSE 'low' END,
             count(*),
             round(percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM updated_at - created_at))::numeric, 2) AS p50,
             round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM updated_at - created_at))::numeric, 2) AS p95
      FROM notifications WHERE status = 'sent' GROUP BY priority ORDER BY priority DESC"

echo "== provider view (mock /stats)"
curl -fsS localhost:9090/stats; echo
