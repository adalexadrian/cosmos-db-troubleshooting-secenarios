"""
Azure Cosmos DB (NoSQL API) - setup, high-volume load and troubleshooting scenarios.

Install:   pip install "azure-cosmos>=4.7" azure-identity
Env vars:  COSMOS_ENDPOINT   https://<account>.documents.azure.com:443/
           COSMOS_KEY        primary key (needed for create database/container)
           COSMOS_USE_AAD=1  (optional) use Entra ID / RBAC for data-plane calls
           COSMOS_PREFERRED_LOCATIONS="West Europe,North Europe"   (optional)

Usage:     python cosmos_demo.py setup                 # DB + container @ 40,000 RU/s
           python cosmos_demo.py load --docs 200000    # high-volume insert
           python cosmos_demo.py perf                  # latency / throttling / indexing
           python cosmos_demo.py connectivity          # auth errors, regions, failover
           python cosmos_demo.py best-practices        # schema, SDK config, monitoring
           python cosmos_demo.py failover-watch --watch-seconds 120
           python cosmos_demo.py all | cleanup

Note: the Python SDK is Gateway-mode (HTTPS) only. Expect slightly higher latency than
the .NET/Java Direct-mode SDKs; parallelism (threads) is how you get throughput.
Note: Entra ID/RBAC data-plane roles cannot create databases/containers - use the key
(or ARM/Bicep/CLI) for `setup`

////

Setup

bash
pip install "azure-cosmos>=4.7" azure-identity
export COSMOS_ENDPOINT="https://<account>.documents.azure.com:443/"
export COSMOS_KEY="<primary-key>"

python cosmos_demo.py setup                # database + container at 40,000 RU/s (add --autoscale for autoscale)
python cosmos_demo.py load --docs 200000   # multi-threaded insert with RU and latency stats
python cosmos_demo.py all                  # everything in sequence

Scenarios

Area	Command	What it demonstrates
Performance	perf	Compares point read, single-partition query, cross-partition fan-out and a full scan on an unindexed path, and prints server-side query metrics.
		Forces 429s on a 400 RU/s container, then scales it to 4,000 RU/s and reruns.
		Compares default and tuned indexing: write RU, an unindexed filter, and a multi-field ORDER BY that fails without a composite index.
Connectivity	connectivity	Triggers a wrong key (401), a missing database or container (404), an unreachable endpoint, and an Entra ID/RBAC error (403), each with a troubleshooting hint.
		Shows the account's write and read regions and how preferred_locations changes routing.
	failover-watch	Runs a point read every second and prints the routing endpoint, so you can watch a manual failover happen.
Best practices	best-practices	Compares point read vs query, embedded vs referenced items, document size and partition key skew.
		Compares SELECT * with a projection, shows paging, and prints the effective client config.
		Collects p50/p95/p99 and RU/op, then prints Azure Monitor metrics, an alert command and KQL queries.

Use python cosmos_demo.py cleanup to delete the demo database when you're done.

Gateway mode: The Python SDK only supports Gateway mode, so it has no Direct-mode or built-in bulk executor like .NET/Java. That's why the load uses a thread pool.
Partition key: At 40,000 RU/s the container has at least four physical partitions, each capped at 10,000 RU/s. /customerId is a high-cardinality key, so writes spread across them.
RBAC: Entra ID data-plane roles can't create databases or containers. Use the account key (or ARM/CLI) for setup, and RBAC afterwards if you like (COSMOS_USE_AAD=1).
Cost: The scenarios create and delete small temporary containers at 400 RU/s, so expect a small extra charge while they run.
Failover: failover-watch can't trigger a failover itself. Use the portal's manual failover or az cosmosdb failover-priority-change.


///

Run the commands in order:
powershell
   py cosmos_demo.py setup
   py cosmos_demo.py load --docs 50000
   py cosmos_demo.py perf
   py cosmos_demo.py connectivity
   py cosmos_demo.py best-practices
