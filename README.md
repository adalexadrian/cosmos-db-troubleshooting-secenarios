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
(or A
