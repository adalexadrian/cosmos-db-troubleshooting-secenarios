# Azure Cosmos DB Troubleshooting Demo (Python)

A single Python script that connects to Azure Cosmos DB (NoSQL API), creates a database and a container with **40,000 RU/s**, bulk loads data, and runs hands-on scenarios for performance troubleshooting, connectivity and configuration, and best practices.

## Prerequisites

- Python 3.9+ (tested with 3.12)
- An Azure Cosmos DB account with **Provisioned throughput** (not Serverless)
- The account's **total throughput limit** must allow at least 40,000 RU/s (Portal → Settings → Features)

```bash
pip install "azure-cosmos>=4.7" azure-identity
```

## Configuration

| Variable | Required | Description |
|---|---|---|
| `COSMOS_ENDPOINT` | Yes | Account URI, e.g. `https://myaccount.documents.azure.com:443/` |
| `COSMOS_KEY` | Yes* | Primary key (*or set `COSMOS_USE_AAD=1` to use Entra ID) |
| `COSMOS_THROUGHPUT` | No | RU/s for the main container (default `40000`) |
| `COSMOS_DB` / `COSMOS_CONTAINER` | No | Names (default `perf-demo-db` / `orders`) |
| `COSMOS_PREFERRED_LOCATIONS` | No | Comma-separated regions, e.g. `West Europe,North Europe` |

**PowerShell**
```powershell
$env:COSMOS_ENDPOINT = "https://myaccount.documents.azure.com:443/"
$env:COSMOS_KEY = "<primary-key>"
```

**Bash**
```bash
export COSMOS_ENDPOINT="https://myaccount.documents.azure.com:443/"
export COSMOS_KEY="<primary-key>"
```

## Quick start

```bash
python cosmos_demo.py setup                 # create database + container (40,000 RU/s)
python cosmos_demo.py load --docs 50000     # insert high-volume data
python cosmos_demo.py perf                  # performance scenarios
python cosmos_demo.py connectivity          # connectivity scenarios
python cosmos_demo.py best-practices        # best-practice scenarios
python cosmos_demo.py cleanup               # delete the database (stops billing)
```

Or run everything in order: `python cosmos_demo.py all`

## Commands

| Command | Purpose |
|---|---|
| `setup` | Creates the database and container (add `--autoscale` for autoscale up to 40,000 RU/s) |
| `load` | Multi-threaded bulk insert (`--docs`, `--workers`, `--customers`) |
| `perf` | Runs the 3 performance scenarios |
| `connectivity` | Runs the 2 connectivity scenarios |
| `best-practices` | Runs the 3 best-practice scenarios |
| `failover-watch` | Live routing monitor for manual failover tests (`--watch-seconds`) |
| `all` | Setup, load and all scenarios |
| `cleanup` | Deletes the demo database |

## Scenarios

### 1. Performance troubleshooting (`perf`)

| # | Scenario | What it demonstrates | What to look for |
|---|---|---|---|
| P1 | **High latency** | Compares a point read, a single-partition query, a cross-partition query, and a full scan on an unindexed field | RU and latency rise at each step; prints query metrics and activity id |
| P2 | **Throughput optimization** | Blasts writes at a 400 RU/s container with SDK retries off, then scales to 4,000 RU/s and repeats | High 429 rate before scaling, low after; playbook for autoscale and hot partitions |
| P3 | **Indexing problems** | Default vs tuned indexing policy: write cost, unindexed filter, multi-field `ORDER BY` | Tuned policy writes cheaper; `ORDER BY` fails (400) without a composite index |

### 2. Connectivity and configuration (`connectivity`, `failover-watch`)

| # | Scenario | What it demonstrates | What to look for |
|---|---|---|---|
| C1 | **Authentication and connection errors** | Wrong key (401), missing database/container (404), unreachable endpoint, Entra ID/RBAC failure (403) | Each error is printed with a troubleshooting hint |
| C2 | **Regions and failover** | Lists write/read regions and shows how `preferred_locations` changes routing | Endpoint changes with region order; a bogus region falls back gracefully |
| C3 | **Live failover test** (`failover-watch`) | Point read every second while you trigger a manual failover | Short error/latency blip, then reads and writes move to the new region |

### 3. Best practices (`best-practices`)

| # | Scenario | What it demonstrates | What to look for |
|---|---|---|---|
| B1 | **Schema and partition key design** | Point read vs query, embedded vs referenced items, document size, partition key skew | Embedded model is cheaper; skew ratio shows hot-key risk |
| B2 | **SDK configuration** | `SELECT *` vs projection, paging with continuation tokens, effective client settings | Projection costs fewer RU; single shared client, retry and location settings |
| B3 | **Monitoring and alerting** | Client-side p50/p95/p99 latency and RU/op, current throughput, Azure Monitor metrics and KQL | Ready-to-use alert command and Log Analytics queries |

## Notes

- **Cost:** 40,000 RU/s is billed hourly (roughly $3.20/hour at standard manual rates; check your region). Run `cleanup` when finished.
- **Extra containers:** The `perf` scenarios temporarily create small 400 RU/s containers, so the account throughput limit must have headroom.
- **Gateway mode:** The Python SDK uses Gateway mode only, so parallelism (threads) is how you reach high throughput. Run from an Azure VM in the same region for best results.
- **RBAC:** Entra ID data-plane roles can't create databases or containers. Use the account key (or ARM/CLI) for `setup`.
- **Never commit keys:** Use environment variables, Key Vault or Managed Identity.

## Common errors

| Error | Likely cause |
|---|---|
| `400 sub_status=1028` on setup | Account throughput limit is lower than requested RU/s |
| `401 Unauthorized` | Wrong or rotated key, wrong account, or clock skew |
| `403 Forbidden` | Firewall/VNet/Private Endpoint or missing RBAC role |
| `404 Not Found` | Wrong database/container name, or id and partition key don't match |
| `429 Too Many Requests` | RU/s exhausted or hot partition |
