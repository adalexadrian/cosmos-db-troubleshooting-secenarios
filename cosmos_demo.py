#!/usr/bin/env python3
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
(or ARM/Bicep/CLI) for `setup`.
"""
import argparse, base64, json, logging, os, random, sys, threading, time, uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter
from azure.core.pipeline.transport import RequestsTransport
from azure.cosmos import CosmosClient, PartitionKey, ThroughputProperties, documents, exceptions
from azure.core.exceptions import ServiceRequestError, ServiceResponseError

# --------------------------------------------------------------------------- config
ENDPOINT = os.getenv("COSMOS_ENDPOINT")
KEY = os.getenv("COSMOS_KEY")
USE_AAD = os.getenv("COSMOS_USE_AAD") == "1"
DB_NAME = os.getenv("COSMOS_DB", "perf-demo-db")
CONTAINER_NAME = os.getenv("COSMOS_CONTAINER", "orders")
PK_PATH = "/customerId"          # high cardinality, appears in most queries
THROUGHPUT = int(os.getenv("COSMOS_THROUGHPUT", "40000"))   # RU/s. 40k => >= 4 physical partitions

# Tuned indexing policy: don't index the big payload, add a composite index for a known query.
MAIN_INDEXING = {
    "indexingMode": "consistent",
    "automatic": True,
    "includedPaths": [{"path": "/*"}],
    "excludedPaths": [{"path": "/payload/?"}, {"path": '/"_etag"/?'}],
    "compositeIndexes": [[{"path": "/status", "order": "ascending"},
                          {"path": "/orderDate", "order": "descending"}]],
}

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger("cosmos-demo")


# --------------------------------------------------------------------------- helpers
def banner(title):
    print("\n" + "=" * 90 + f"\n{title}\n" + "=" * 90)


def credential():
    if USE_AAD:
        from azure.identity import DefaultAzureCredential
        return DefaultAzureCredential()
    if not KEY:
        sys.exit("Set COSMOS_KEY (or COSMOS_USE_AAD=1).")
    return KEY


POOL_SIZE = int(os.getenv("COSMOS_POOL_SIZE", "100"))


def make_transport(pool_size=POOL_SIZE):
    """HTTP transport with a bigger connection pool (default is 10 -> 'Connection pool is full'
    warnings as soon as more than 10 threads share one client)."""
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return RequestsTransport(session=session)


def make_client(*, endpoint=None, cred=None, max_429_retries=9, max_429_wait=30,
                preferred_locations=None, **kwargs):
    """Single place where every client is built. Reuse ONE client per process (singleton)."""
    if not (endpoint or ENDPOINT):
        sys.exit("Set COSMOS_ENDPOINT.")
    policy = documents.ConnectionPolicy()
    policy.RetryOptions = documents.RetryOptions(
        max_retry_attempt_count=max_429_retries,        # SDK-side retry on HTTP 429
        max_wait_time_in_seconds=max_429_wait)
    opts = dict(connection_policy=policy, user_agent="cosmos-troubleshooting-demo",
                transport=make_transport())
    if preferred_locations:
        opts["preferred_locations"] = preferred_locations
    opts.update(kwargs)
    return CosmosClient(endpoint or ENDPOINT, credential=cred or credential(), **opts)


def create_best_practice_client():
    prefs = [r.strip() for r in os.getenv("COSMOS_PREFERRED_LOCATIONS", "").split(",") if r.strip()]
    extra = {"connection_timeout": 10}
    if os.getenv("COSMOS_CONSISTENCY"):     # can only be equal or weaker than account default
        extra["consistency_level"] = os.getenv("COSMOS_CONSISTENCY")
    return make_client(preferred_locations=prefs or None, **extra)


class Metrics:
    """Thread-safe latency / RU / status collector (used by load and monitoring demos)."""
    def __init__(self):
        self._lock = threading.Lock()
        self.lat, self.ru, self.ok, self.throttled, self.failed = [], 0.0, 0, 0, 0

    def add(self, ms=None, ru=0.0, status=200):
        with self._lock:
            if status == 429:
                self.throttled += 1
            elif status >= 400:
                self.failed += 1
            else:
                self.ok += 1
                self.lat.append(ms or 0.0)
            self.ru += ru

    def pct(self, p):
        if not self.lat:
            return 0.0
        s = sorted(self.lat)
        return s[min(len(s) - 1, int(len(s) * p / 100))]

    def report(self, label, wall):
        print(f"{label}: ok={self.ok:,} throttled(429)={self.throttled:,} failed={self.failed:,} "
              f"wall={wall:.1f}s  {self.ok / wall:,.0f} ops/s  ~{self.ru / wall:,.0f} RU/s")
        print(f"   latency ms  p50={self.pct(50):.1f}  p95={self.pct(95):.1f}  p99={self.pct(99):.1f}  "
              f"avg RU/op={self.ru / max(self.ok, 1):.2f}")


def _ru(headers):
    return float(headers.get("x-ms-request-charge", 0))


def point_read(container, item_id, pk):
    box = {}
    t = time.perf_counter()
    item = container.read_item(item_id, partition_key=pk, response_hook=lambda h, _b: box.update(h))
    return item, (time.perf_counter() - t) * 1000, _ru(box)


def run_query(container, query, parameters=None, **kw):
    """Runs a query to completion, returns (items, latency_ms, total_RU across pages)."""
    ru = [0.0]
    def hook(h, _b):
        ru[0] += _ru(h)
    t = time.perf_counter()
    items = list(container.query_items(query=query, parameters=parameters,
                                       response_hook=hook, **kw))
    return items, (time.perf_counter() - t) * 1000, ru[0]


def last_headers(container):
    return getattr(container.client_connection, "last_response_headers", {}) or {}


def get_sample(container):
    rows, _, _ = run_query(container, "SELECT TOP 1 c.id, c.customerId, c.orderId FROM c "
                           "WHERE c.type = 'order'", enable_cross_partition_query=True)
    if not rows:
        sys.exit("Container is empty - run `load` first.")
    return rows[0]


def show(label, ms, ru, extra=""):
    print(f"  {label:<58} {ms:9.1f} ms {ru:9.2f} RU {extra}")


HINTS = {
    400: "Bad request: malformed query/JSON, ORDER BY without composite index, wrong partition key value.",
    401: "Unauthorized: wrong/rotated key, key from another account, or client clock skew > 15 min.",
    403: "Forbidden: firewall/VNet/Private Endpoint blocks this IP, missing RBAC role (sub-status 5301), "
         "read-only key used for a write, or write sent to a read-only region (sub-status 3).",
    404: "Not found: wrong database/container name, or id + partition key don't match (sub-status 1002 = "
         "session token not yet replicated to that region).",
    408: "Timeout: check client CPU/threads, network path, and cross-partition fan-out.",
    409: "Conflict: an item with this id already exists in this partition key.",
    412: "Precondition failed: ETag mismatch (optimistic concurrency) - re-read and retry.",
    413: "Request too large: item >2 MB.",
    429: "Throttled: RU/s exhausted or hot partition. Honour x-ms-retry-after-ms, raise RU or fix skew.",
    449: "Retry-with: transient write conflict - SDK retries; retry at app level if it surfaces.",
    503: "Service unavailable: transient/regional issue - retry with back-off, check Service Health.",
}


def explain(e):
    code = getattr(e, "status_code", None)
    sub = getattr(e, "sub_status", None)
    msg = (getattr(e, "message", None) or str(e)).replace("\n", " ")[:170]
    print(f"    -> {type(e).__name__} status={code} sub_status={sub}\n       {msg}")
    if code in HINTS:
        print(f"    HINT: {HINTS[code]}")
    elif isinstance(e, (ServiceRequestError, ServiceResponseError)):
        print("    HINT: Network-level failure (DNS, TLS, proxy, firewall, endpoint typo, no route/timeout).")


# --------------------------------------------------------------------------- 1. setup
def setup(client, autoscale=False):
    banner("SETUP - database + container")
    db = client.create_database_if_not_exists(id=DB_NAME)
    throughput = (ThroughputProperties(auto_scale_max_throughput=THROUGHPUT)
                  if autoscale else THROUGHPUT)
    print(f"Database '{DB_NAME}' ready. Creating container '{CONTAINER_NAME}' at {THROUGHPUT:,} RU/s ...")
    try:
        container = db.create_container_if_not_exists(
            id=CONTAINER_NAME,
            partition_key=PartitionKey(path=PK_PATH),
            indexing_policy=MAIN_INDEXING,
            offer_throughput=throughput)
    except exceptions.CosmosHttpResponseError as e:
        print("\nCONTAINER CREATION FAILED")
        explain(e)
        print("""
    Most common reasons:
     * Serverless account: it does not support provisioned RU/s. Create a 'Provisioned throughput' account.
     * Account-level throughput cap: Azure portal -> your account -> Settings -> Features ->
       'Account level throughput' (or Free/Learn tier default limits, often 1000-4000 RU/s).
       Raise/disable the cap, or run with a lower value:  $env:COSMOS_THROUGHPUT = "4000"
     * Subscription/region quota for RU/s: request an increase in the portal (Help + support).
""")
        sys.exit(1)
    tp = container.get_throughput()
    print(f"Database '{DB_NAME}', container '{CONTAINER_NAME}', pk={PK_PATH}")
    print("Throughput:", getattr(tp, "auto_scale_max_throughput", None) and
          f"autoscale max {tp.auto_scale_max_throughput}" or f"manual {tp.offer_throughput} RU/s")
    return db, container


# --------------------------------------------------------------------------- 2. bulk load
STATUSES = ["Created", "Paid", "Shipped", "Delivered", "Cancelled"]
REGIONS = ["EMEA", "NA", "APAC", "LATAM"]


def make_order(customers):
    """~1 KB order document with embedded items (schema designed for point reads by customer)."""
    date = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 365))
    items = [{"sku": f"SKU-{random.randint(1, 9999):05d}", "qty": random.randint(1, 5),
              "price": round(random.uniform(1, 200), 2)} for _ in range(random.randint(1, 5))]
    return {
        "id": str(uuid.uuid4()),
        "type": "order",
        "customerId": f"cust-{random.randint(1, customers):06d}",
        "orderId": f"ORD-{uuid.uuid4().hex[:12]}",
        "status": random.choice(STATUSES),
        "region": random.choice(REGIONS),
        "orderDate": date.isoformat(),
        "totalAmount": round(sum(i["qty"] * i["price"] for i in items), 2),
        "items": items,
        "shipping": {"city": random.choice(["Bucharest", "Berlin", "Paris", "Madrid"]),
                     "zip": f"{random.randint(10000, 99999)}"},
        "payload": "x" * random.randint(300, 600),      # excluded from index (see MAIN_INDEXING)
    }


def bulk_load(container, total, workers, customers):
    banner(f"BULK LOAD - {total:,} documents with {workers} threads")
    m = Metrics()

    def worker(_):
        doc = make_order(customers)
        for _attempt in range(6):                        # app-level retry AFTER SDK retries gave up
            box = {}
            t = time.perf_counter()
            try:
                container.upsert_item(doc, response_hook=lambda h, _b: box.update(h))
                m.add((time.perf_counter() - t) * 1000, _ru(box))
                return
            except exceptions.CosmosHttpResponseError as e:
                if e.status_code == 429:
                    m.add(status=429)
                    wait_ms = float((getattr(e, "headers", None) or {}).get("x-ms-retry-after-ms", 200))
                    time.sleep(wait_ms / 1000)
                else:
                    m.add(status=e.status_code or 500)
                    return

    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for i, _ in enumerate(pool.map(worker, range(total)), 1):
            if i % 10000 == 0:
                print(f"  ... {i:,} processed ({i / (time.perf_counter() - t0):,.0f} docs/s)")
    m.report("Load complete", time.perf_counter() - t0)
    print(f"  Theoretical ceiling at {THROUGHPUT:,} RU/s and {m.ru / max(m.ok, 1):.1f} RU/doc "
          f"= ~{THROUGHPUT / max(m.ru / max(m.ok, 1), 1):,.0f} docs/s. If you are far below it, add "
          f"threads (--workers) or run from an Azure VM in the same region.")


# =========================================================================== PERFORMANCE
def scenario_latency(container):
    banner("PERFORMANCE 1 - High latency: where does the time go?")
    s = get_sample(container)
    print(f"Sample item: id={s['id'][:8]}... customerId={s['customerId']}")

    reads = [point_read(container, s["id"], s["customerId"]) for _ in range(5)]
    show("Point read (id + partition key), avg of 5",
         sum(r[1] for r in reads) / 5, sum(r[2] for r in reads) / 5, "<- cheapest, ~1 RU per KB")

    _, ms, ru = run_query(container, "SELECT * FROM c WHERE c.customerId = @c",
                          [{"name": "@c", "value": s["customerId"]}], partition_key=s["customerId"])
    show("Single-partition query (partition_key supplied)", ms, ru)

    q_indexed = "SELECT * FROM c WHERE c.orderId = @o"
    p = [{"name": "@o", "value": s["orderId"]}]
    _, ms, ru = run_query(container, q_indexed, p, enable_cross_partition_query=True)
    show("Cross-partition query on indexed field (fan-out)", ms, ru, "<- pays per physical partition")

    _, ms, ru = run_query(container, "SELECT VALUE COUNT(1) FROM c WHERE c.payload = @x",
                          [{"name": "@x", "value": "nope"}], enable_cross_partition_query=True)
    show("Cross-partition filter on EXCLUDED path (full scan)", ms, ru, "<- RU + latency explode")

    print("\n  Server-side diagnostics for the fan-out query:")
    try:
        try:
            list(container.query_items(q_indexed, parameters=p, enable_cross_partition_query=True,
                                       populate_query_metrics=True, populate_index_metrics=True))
        except TypeError:   # older SDK without populate_index_metrics
            list(container.query_items(q_indexed, parameters=p, enable_cross_partition_query=True,
                                       populate_query_metrics=True))
        h = last_headers(container)
        for k in ("x-ms-activity-id", "x-ms-request-charge", "x-ms-documentdb-query-metrics",
                  "x-ms-cosmos-index-utilization"):
            if k in h:
                print(f"    {k}: {str(h[k])[:200]}")
    except Exception as e:  # noqa
        explain(e)

    print("""
  Diagnosis checklist:
   * Client far from the account region?  -> use preferred_locations, run in the same region.
   * Point-read vs query on the same item -> use read_item whenever you have id + pk.
   * Cross-partition fan-out              -> put the pk in the WHERE clause / re-think the pk.
   * Scan on excluded/unindexed path      -> index it or stop filtering on it.
   * Client-side: new CosmosClient per request, too few threads, CPU-bound host (Gateway mode).
   * Server-side latency (Azure Monitor 'ServerSideLatency') < 10 ms but app sees 100+ ms
     => the problem is network/client, not Cosmos DB.""")


def scenario_throttling(db, seconds=120):
    banner(f"PERFORMANCE 2 - Throughput optimisation: sustained 429s for {seconds}s, then scale-up")
    name = "throttle-demo"
    cont = db.create_container_if_not_exists(id=name, partition_key=PartitionKey(path="/pk"),
                                             offer_throughput=400)
    # Client with SDK 429-retries disabled, so we can SEE the throttling.
    raw = make_client(max_429_retries=0)
    tcont = raw.get_database_client(DB_NAME).get_container_client(name)

    def blast(label, duration, threads=24):
        m, stop = Metrics(), time.time() + duration

        def w():
            while time.time() < stop:
                box, t = {}, time.perf_counter()
                try:
                    tcont.upsert_item({"id": str(uuid.uuid4()), "pk": str(uuid.uuid4()),
                                       "data": "y" * 800}, response_hook=lambda h, _b: box.update(h))
                    m.add((time.perf_counter() - t) * 1000, _ru(box))
                except exceptions.CosmosHttpResponseError as e:
                    m.add(status=e.status_code or 500)
                    if e.status_code == 429:
                        time.sleep(0.02)          # avoid burning client CPU in a tight 429 loop

        print(f"\n  {label} - {duration}s, {threads} threads")
        start = last_t = time.time()
        last = (0, 0, 0.0)
        with ThreadPoolExecutor(threads) as ex:
            for _ in range(threads):
                ex.submit(w)
            while time.time() < stop:                     # progress line every 10 s
                time.sleep(max(0.0, min(10, stop - time.time())))
                now = time.time()
                ok, thr, ru = m.ok, m.throttled, m.ru
                d_ok, d_thr, d_ru = ok - last[0], thr - last[1], ru - last[2]
                dt = max(now - last_t, 1e-6)
                pct = 100 * d_thr / max(d_ok + d_thr, 1)
                print(f"    t+{now - start:4.0f}s  ok={d_ok:6,}  429={d_thr:7,} ({pct:3.0f}% throttled)"
                      f"  ~{d_ru / dt:6,.0f} RU/s consumed")
                last, last_t = (ok, thr, ru), now
        total = m.ok + m.throttled
        print(f"  => {label}: {total:,} requests, {m.throttled:,} throttled "
              f"({100 * m.throttled / max(total, 1):.0f}% 429), "
              f"avg ~{m.ru / duration:,.0f} RU/s consumed")

    try:
        blast("A) 400 RU/s, SDK retries OFF", seconds)
        try:
            cont.replace_throughput(4000)
            print("\n  -> scaled container to 4,000 RU/s (replace_throughput), waiting 5 s ...")
            time.sleep(5)
            blast("B) 4,000 RU/s, SDK retries OFF", min(30, seconds))
        except exceptions.CosmosHttpResponseError as e:
            print("\n  Could not scale to 4,000 RU/s (account throughput limit?) - skipping phase B")
            explain(e)
    finally:
        db.delete_container(name)

    print("""
  Optimisation playbook:
   * Keep SDK 429 retries ON in production (default 9 attempts / 30 s) and add app-level back-off
     using x-ms-retry-after-ms.
   * Some 429s at ~100% utilisation are normal and healthy; sustained 429 (>1-5%) is not.
   * Autoscale (--autoscale) for spiky workloads: pays 10-100% of max RU/s.
   * Look at 'Normalized RU Consumption' PER PARTITION KEY RANGE: one range at 100% while the
     others idle = hot partition -> change pk / add synthetic pk, don't just add RU.
   * Lower RU per operation: projections, smaller docs, fewer indexed paths, point reads.
   * Each physical partition is capped at 10,000 RU/s and 50 GB - 40k RU/s only helps if load
     is spread over >= 4 logical-key ranges.
   * Many threads on one client? Size the HTTP pool (COSMOS_POOL_SIZE) >= thread count.""")


def scenario_indexing(db):
    banner("PERFORMANCE 3 - Indexing problems")
    tuned = {"indexingMode": "consistent", "automatic": True,
             "includedPaths": [{"path": "/customerId/?"}, {"path": "/status/?"}, {"path": "/orderDate/?"}],
             "excludedPaths": [{"path": "/*"}],
             "compositeIndexes": [[{"path": "/status", "order": "ascending"},
                                   {"path": "/orderDate", "order": "descending"}]]}
    conts = {"idx-default": db.create_container_if_not_exists(
                 id="idx-default", partition_key=PartitionKey(path=PK_PATH), offer_throughput=400),
             "idx-tuned": db.create_container_if_not_exists(
                 id="idx-tuned", partition_key=PartitionKey(path=PK_PATH),
                 indexing_policy=tuned, offer_throughput=400)}
    try:
        docs = [make_order(50) for _ in range(150)]
        print("Write cost (150 docs each):")
        for name, c in conts.items():
            total = 0.0
            for d in docs:
                box = {}
                c.upsert_item(d, response_hook=lambda h, _b: box.update(h))
                total += _ru(box)
            print(f"  {name:<12} avg {total / len(docs):5.2f} RU/write")

        tests = [("Filter on indexed field  (status = 'Paid')", "SELECT * FROM c WHERE c.status = 'Paid'"),
                 ("Filter on unindexed field (region = 'EMEA')", "SELECT * FROM c WHERE c.region = 'EMEA'")]
        print("\nRead cost:")
        for label, q in tests:
            for name, c in conts.items():
                _, ms, ru = run_query(c, q, enable_cross_partition_query=True)
                show(f"{label} [{name}]", ms, ru)

        print("\nMulti-property ORDER BY needs a composite index:")
        q = "SELECT c.id FROM c ORDER BY c.status ASC, c.orderDate DESC"
        for name, c in conts.items():
            try:
                _, ms, ru = run_query(c, q, enable_cross_partition_query=True)
                show(f"ORDER BY status, orderDate [{name}]", ms, ru, "OK")
            except exceptions.CosmosHttpResponseError as e:
                print(f"  [{name}] FAILED")
                explain(e)
    finally:
        for name in conts:
            db.delete_container(name)

    print("""
  Indexing rules of thumb:
   * Default = index every path: simple, but the most expensive writes. Exclude big/unqueried paths.
   * Query slow but RU low?  -> not indexing. Query RU high + 'Index hit doc count' << 'Retrieved doc count'
     -> missing index / composite index / range vs. equality mismatch.
   * Policy changes re-index in the background; track progress via the
     'x-ms-documentdb-collection-index-transformation-progress' response header.""")


# =========================================================================== CONNECTIVITY
def scenario_auth_errors():
    banner("CONNECTIVITY 1 - Authentication / configuration errors")
    fake_key = base64.b64encode(os.urandom(64)).decode()

    print("Case 1: valid-looking but WRONG account key")
    try:
        c = make_client(cred=fake_key, retry_total=1)
        c.get_database_client(DB_NAME).read()
    except Exception as e:  # noqa
        explain(e)

    print("\nCase 2: database/container that does not exist")
    try:
        make_client().get_database_client("no-such-db").get_container_client("nope").read()
    except Exception as e:  # noqa
        explain(e)

    print("\nCase 3: unreachable endpoint (typo / DNS / firewall)")
    try:
        make_client(endpoint="https://no-such-account-xyz123.documents.azure.com:443/",
                    connection_timeout=3, retry_total=1, retry_connect=1)
    except Exception as e:  # noqa
        explain(e)

    print("\nCase 4: Entra ID (RBAC) data-plane access")
    try:
        from azure.identity import DefaultAzureCredential
        c = make_client(cred=DefaultAzureCredential())
        c.get_database_client(DB_NAME).get_container_client(CONTAINER_NAME).read()
        print("    RBAC access OK")
    except ImportError:
        print("    azure-identity not installed - skipped")
    except Exception as e:  # noqa
        explain(e)
        print("    Fix: az cosmosdb sql role assignment create --account-name <acct> -g <rg> "
              "--role-definition-name 'Cosmos DB Built-in Data Contributor' "
              "--principal-id <oid> --scope /")

    print("""
  Checklist for 'it worked yesterday':
   * 401 -> key rotated? (regenerate keys invalidates old) / VM clock drift / wrong account.
   * 403 -> Networking blade: selected networks, Private Endpoint DNS (privatelink.documents.azure.com),
            'Accept connections from within public Azure datacenters'; RBAC role assignment.
   * Timeouts/DNS -> corporate proxy, NSG/UDR, outbound 443, and use the account's regional endpoint.
   * Never hard-code keys: use Key Vault or Managed Identity + Entra ID (disable local auth in prod).""")


def _endpoints(client):
    cc = client.client_connection
    return getattr(cc, "ReadEndpoint", "?"), getattr(cc, "WriteEndpoint", "?")


def _host(url):
    return str(url).split("//")[-1].split(".")[0]


def scenario_regions(client):
    banner("CONNECTIVITY 2 - Regions, routing and failover behaviour")
    acct = client.get_database_account()
    writable = [l["name"] for l in acct.WritableLocations]
    readable = [l["name"] for l in acct.ReadableLocations]
    print(f"Write regions : {writable}")
    print(f"Read regions  : {readable}")
    print(f"Multi-write   : {getattr(acct, 'EnableMultipleWritableLocations', False)}")
    print(f"Default consistency: {getattr(acct, 'ConsistencyPolicy', {}).get('defaultConsistencyLevel')}")
    r, w = _endpoints(client)
    print(f"This client -> reads: {_host(r)}  writes: {_host(w)}")

    print("\nEffect of preferred_locations on read routing:")
    for order in [readable, list(reversed(readable)), ["Nowhere Land"] + readable]:
        try:
            c = make_client(preferred_locations=order)
            print(f"  preferred={order} -> reads from {_host(_endpoints(c)[0])}")
        except Exception as e:  # noqa
            explain(e)

    print("""
  How failover works with the SDK:
   * enable_endpoint_discovery=True (default): the SDK reads the account's region list at startup,
     refreshes it periodically and on 403/3 (WriteForbidden), 403/1008 (region unavailable),
     503 and connection errors, then retries against the next preferred region.
   * Set preferred_locations to your app's region first, then the paired region(s).
   * Enable 'Service-managed failover' on the account and configure failover priorities.
   * Test it: `python cosmos_demo.py failover-watch` and trigger a manual failover:
       az cosmosdb failover-priority-change -n <acct> -g <rg> --failover-policies <r1>=0 <r2>=1
   * Expect a short blip of errors/latency; apps must retry idempotently (use upsert / ETags).""")


def failover_watch(client, container, seconds):
    banner(f"FAILOVER WATCH - point reads every second for {seconds}s")
    s = get_sample(container)
    print("Trigger a manual failover / disable a region now and watch the routing change.\n")
    end = time.time() + seconds
    while time.time() < end:
        t = time.perf_counter()
        try:
            _, ms, _ru_ = point_read(container, s["id"], s["customerId"])
            status = "OK"
        except Exception as e:  # noqa
            ms, status = (time.perf_counter() - t) * 1000, f"ERR {getattr(e, 'status_code', type(e).__name__)}"
        r, w = _endpoints(client)
        print(f"  {time.strftime('%H:%M:%S')}  {status:<10} {ms:8.1f} ms   read->{_host(r)}  write->{_host(w)}")
        time.sleep(1)


# =========================================================================== BEST PRACTICES
def scenario_schema(container):
    banner("BEST PRACTICES 1 - Schema design & partition key")
    s = get_sample(container)
    cust = s["customerId"]

    order, ms_r, ru_r = point_read(container, s["id"], cust)
    _, ms_q, ru_q = run_query(container, "SELECT * FROM c WHERE c.id = @i",
                              [{"name": "@i", "value": s["id"]}], partition_key=cust)
    show("Point read of one order", ms_r, ru_r)
    show("Same item through a query", ms_q, ru_q, "<- queries cost more than reads")

    print("\n  Embedded items (1 read) vs. referenced items (1 read + 1 query):")
    ref_ids = []
    try:
        for i in range(5):
            doc = {"id": str(uuid.uuid4()), "type": "orderItem", "customerId": cust,
                   "orderId": order["orderId"], "sku": f"SKU-{i}", "qty": 1}
            container.upsert_item(doc)
            ref_ids.append(doc["id"])
        _, ms_i, ru_i = run_query(container, "SELECT * FROM c WHERE c.type='orderItem' AND c.orderId=@o",
                                  [{"name": "@o", "value": order["orderId"]}], partition_key=cust)
        show("Embedded model: 1 point read", ms_r, ru_r)
        show("Referenced model: point read + item query", ms_r + ms_i, ru_r + ru_i)
    finally:
        for i in ref_ids:
            container.delete_item(i, partition_key=cust)

    sizes = []
    rows, _, _ = run_query(container, "SELECT TOP 100 * FROM c WHERE c.type = 'order'",
                           enable_cross_partition_query=True)
    sizes = [len(json.dumps(r)) for r in rows]
    print(f"\n  Avg document size (sample of {len(sizes)}): {sum(sizes) / max(len(sizes), 1) / 1024:.2f} KB "
          f"(limit 2 MB; every extra KB costs RU on every read/write)")

    rows, ms, ru = run_query(container, "SELECT c.customerId, COUNT(1) AS n FROM c "
                             "WHERE c.type='order' GROUP BY c.customerId", enable_cross_partition_query=True)
    if rows:
        counts = sorted((r["n"] for r in rows), reverse=True)
        avg = sum(counts) / len(counts)
        print(f"  Partition key check: {len(rows):,} distinct customerId values, "
              f"max={counts[0]} docs, avg={avg:.1f}, skew={counts[0] / avg:.1f}x   ({ru:.0f} RU to compute)")
        print("  -> " + ("healthy distribution" if counts[0] / avg < 5 else
                         "SKEWED: a few keys dominate - consider a synthetic/hierarchical key"))
    print("""
  Schema rules:
   * Model for your queries: embed data read together, reference data that is unbounded or changes often.
   * Partition key: high cardinality, even writes AND reads, present in most WHERE clauses; never a timestamp
     or a low-cardinality value like status. Consider hierarchical partition keys for multi-tenant data.
   * Avoid unbounded arrays and huge documents; use a 'type' discriminator to co-locate entities.
   * id + partition key = point read (cheapest operation in Cosmos DB). Use TTL for expiring data.""")


def scenario_sdk(container, client):
    banner("BEST PRACTICES 2 - SDK usage & configuration")
    s = get_sample(container)
    cust = s["customerId"]
    p = [{"name": "@c", "value": cust}]

    _, ms, ru = run_query(container, "SELECT * FROM c WHERE c.customerId = @c", p, partition_key=cust)
    show("SELECT *", ms, ru)
    _, ms, ru = run_query(container, "SELECT c.id, c.status, c.totalAmount FROM c WHERE c.customerId = @c",
                          p, partition_key=cust)
    show("Projection (only needed fields)", ms, ru, "<- cheaper, less network")

    pager = container.query_items("SELECT c.id FROM c WHERE c.customerId = @c", parameters=p,
                                  partition_key=cust, max_item_count=5).by_page()
    first = list(next(pager))
    print(f"  Paging: page 1 has {len(first)} items; continuation token = "
          f"{'present' if pager.continuation_token else 'none (last page)'}")

    cp = client.client_connection.connection_policy
    print("\n  Effective client configuration:")
    print(f"    PreferredLocations       : {getattr(cp, 'PreferredLocations', None)}")
    print(f"    EnableEndpointDiscovery  : {getattr(cp, 'EnableEndpointDiscovery', None)}")
    print(f"    429 retries / max wait   : {cp.RetryOptions.MaxRetryAttemptCount} / "
          f"{cp.RetryOptions.MaxWaitTimeInSeconds}s")
    print("""
  SDK checklist:
   * ONE CosmosClient for the whole process (it caches account/partition metadata; don't create per request).
   * Set preferred_locations, a user_agent (shows up in diagnostics), sane timeouts and 429 retry options.
   * Always pass partition_key to queries when known; avoid enable_cross_partition_query unless required.
   * Parameterise queries (@param) - safer and lets the service cache plans. SELECT only needed fields.
   * Use max_item_count + continuation tokens for paging; use read_item for id+pk lookups.
   * Bulk work: thread pool (sync) or azure.cosmos.aio (async) + transactional execute_item_batch
     (<=100 ops within one partition key). Keep the SDK current: azure-cosmos 4.x.
   * Choose the weakest consistency that is correct (Session is the default and usually right).""")


def scenario_monitoring(container):
    banner("BEST PRACTICES 3 - Monitoring & alerting")
    s = get_sample(container)
    m, t0 = Metrics(), time.perf_counter()
    for i in range(150):                                    # small synthetic 'production' mix
        try:
            t = time.perf_counter()
            if i % 5:
                _, ms, ru = point_read(container, s["id"], s["customerId"])
            else:
                _, ms, ru = run_query(container, "SELECT c.id FROM c WHERE c.customerId=@c",
                                      [{"name": "@c", "value": s["customerId"]}],
                                      partition_key=s["customerId"])
            m.add(ms, ru)
        except exceptions.CosmosHttpResponseError as e:
            m.add(status=e.status_code or 500)
    m.report("Client-side telemetry (wrap every call like this and ship to App Insights/Prometheus)",
             time.perf_counter() - t0)
    if m.pct(99) > 50:
        print("  ALERT: p99 latency above 50 ms SLO")

    tp = container.get_throughput()
    print(f"  Provisioned: {getattr(tp, 'offer_throughput', None) or 'autoscale max ' + str(tp.auto_scale_max_throughput)}"
          f"  | scale operation pending: {last_headers(container).get('x-ms-offer-replace-pending', 'false')}")

    print("""
  Azure Monitor metrics to chart + alert on (per container / partition key range):
    NormalizedRUConsumption (> 70-80% sustained)   TotalRequests split by StatusCode (429, 5xx)
    ServerSideLatency (p99)   ProvisionedThroughput   DataUsage / IndexUsage   ReplicationLatency

  Example alert:
    az monitor metrics alert create -n cosmos-ru-high -g <rg> --scopes <cosmos-resource-id> \\
       --condition "max NormalizedRUConsumption > 80" --window-size 5m --evaluation-frequency 1m \\
       --action <action-group-id>

  Enable Diagnostic settings -> Log Analytics (resource-specific tables), then use KQL:
    // Who is throttled, and on which partition range?
    CDBDataPlaneRequests | where TimeGenerated > ago(1h) and StatusCode == 429
    | summarize count() by bin(TimeGenerated, 1m), PartitionKeyRangeId

    // Most expensive partition keys (hot-key detection)
    CDBPartitionKeyRUConsumption | where TimeGenerated > ago(1h)
    | summarize RU = sum(RequestCharge) by PartitionKey, PartitionKeyRangeId | top 10 by RU

    // Slowest operations
    CDBDataPlaneRequests | summarize p99 = percentile(DurationMs, 99) by OperationName, bin(TimeGenerated, 5m)

  Client-side: log the activity id (x-ms-activity-id) for every failed/slow call - support needs it.
  Turn on SDK logging when debugging:  logging.getLogger('azure.cosmos').setLevel(logging.DEBUG)""")


# --------------------------------------------------------------------------- main
def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["setup", "load", "perf", "connectivity", "best-practices",
                                        "failover-watch", "all", "cleanup"])
    ap.add_argument("--docs", type=int, default=100_000)
    ap.add_argument("--workers", type=int, default=64)
    ap.add_argument("--customers", type=int, default=5000, help="distinct partition key values")
    ap.add_argument("--autoscale", action="store_true", help="autoscale max 40k instead of manual 40k")
    ap.add_argument("--watch-seconds", type=int, default=60)
    ap.add_argument("--throttle-seconds", type=int, default=120,
                    help="how long the sustained-429 phase of `perf` runs (default 120)")
    a = ap.parse_args()

    if a.command == "connectivity":                 # scenario 1 builds its own clients
        scenario_auth_errors()
    client = create_best_practice_client()

    if a.command == "cleanup":
        client.delete_database(DB_NAME)
        print(f"Deleted database {DB_NAME}")
        return
    if a.command in ("setup", "all"):
        db, container = setup(client, a.autoscale)
    else:
        db = client.get_database_client(DB_NAME)
        container = db.get_container_client(CONTAINER_NAME)

    if a.command in ("load", "all"):
        bulk_load(container, a.docs, a.workers, a.customers)
    if a.command in ("perf", "all"):
        scenario_latency(container)
        scenario_throttling(db, a.throttle_seconds)
        scenario_indexing(db)
    if a.command in ("connectivity", "all"):
        if a.command == "all":
            scenario_auth_errors()
        scenario_regions(client)
    if a.command == "failover-watch":
        failover_watch(client, container, a.watch_seconds)
    if a.command in ("best-practices", "all"):
        scenario_schema(container)
        scenario_sdk(container, client)
        scenario_monitoring(container)


if __name__ == "__main__":
    main()
