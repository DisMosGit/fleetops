# dashboards/ — Grafana dashboards

Grafana dashboard JSON for the FleetOps stack (provisioned through Grafana's dashboard
providers wired in `deploy/`): canary wave progress, per-region health, heartbeat ingest rate,
and activity latency. Every panel must read a Prometheus metric that has a real source — no
dashboard-only metrics.

**Not implemented yet** — dashboards arrive at stage 4 together with the Prometheus metrics
they read.
