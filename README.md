## Monitoring & Observability
The trading engine exposes high-resolution telemetry data via a native Prometheus instrumentation layer (`prom-client`). 

### Core Telemetry:
- **Custom Trading Metrics**: `nexus_circuit_breaker_trips_total` tracking real-time 15% volatility halts on a per-ticker/per-side basis.
- **Runtime Health**: Low-overhead monitoring of Node.js event loop lag and heap allocation structures.

Available out-of-the-box at the `/metrics` endpoint for easy ingestion into standard infrastructure monitoring setups.