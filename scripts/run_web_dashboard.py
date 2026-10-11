
"""ChronosMatch browser dashboard connected to the real matching engine."""

import json
import sys
import time
import threading
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from chronosmatch.engine.matching_engine import MatchingEngine
from chronosmatch.simulator.order_generator import OrderGenerator

HOST = "127.0.0.1"
PORT = 8765
TOTAL_ORDERS = 100_000
UPDATE_SECONDS = 0.20

engine = MatchingEngine()
lock = threading.Lock()
started = time.perf_counter()

generator = OrderGenerator(
    seed=42,
    min_price=100_000,
    max_price=110_000,
    min_quantity=1,
    max_quantity=20,
    order_count=TOTAL_ORDERS,
)
orders = iter(generator.generate_batch())

stats = {
    "processed": 0,
    "accepted": 0,
    "rejected": 0,
    "status": "RUNNING",
    "last_update": time.time(),
}


def market_worker():
    """Feed generated orders through the project's real matching engine."""
    while stats["processed"] < TOTAL_ORDERS:
        try:
            order = next(orders)
        except StopIteration:
            stats["status"] = "COMPLETED"
            break

        try:
            with lock:
                engine.process_order(order)
                stats["accepted"] += 1
                stats["processed"] += 1
                stats["last_update"] = time.time()
        except ValueError:
            with lock:
                stats["rejected"] += 1
                stats["processed"] += 1
                stats["last_update"] = time.time()

        time.sleep(UPDATE_SECONDS)

    stats["status"] = "COMPLETED"


def snapshot():
    with lock:
        book = engine.get_order_book()
        trades = engine.get_trades()
        elapsed = max(time.perf_counter() - started, 0.001)

        bids = [
            {"price": price, "quantity": quantity}
            for price, quantity in book["bids"][:10]
        ]
        asks = [
            {"price": price, "quantity": quantity}
            for price, quantity in book["asks"][:10]
        ]

        recent_trades = [
            {
                "id": f"TRD-{t.trade_id:06d}",
                "price": t.price,
                "quantity": t.quantity,
                "buy_order": f"ORD-{t.buy_order_id:06d}",
                "sell_order": f"ORD-{t.sell_order_id:06d}",
            }
            for t in reversed(trades[-30:])
        ]

        return {
            "status": stats["status"],
            "mode": "REAL PROJECT MATCHING ENGINE + SIMULATED INPUT ORDERS",
            "processed": stats["processed"],
            "accepted": stats["accepted"],
            "rejected": stats["rejected"],
            "trades": len(trades),
            "throughput": round(stats["processed"] / elapsed, 2),
            "elapsed": round(elapsed, 1),
            "best_bid": bids[0]["price"] if bids else None,
            "best_ask": asks[0]["price"] if asks else None,
            "spread": (
                asks[0]["price"] - bids[0]["price"]
                if bids and asks else None
            ),
            "bids": bids,
            "asks": asks,
            "recent_trades": recent_trades,
            "updated": time.strftime("%H:%M:%S"),
        }


PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>ChronosMatch | Live Market</title>
<style>
:root{color-scheme:dark;--bg:#0a0f1b;--panel:#121a2a;--line:#26334a;
--text:#e7edf8;--muted:#91a0b8;--green:#35d49a;--red:#ff6b79;--blue:#70a7ff}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);
font:14px system-ui,Segoe UI,sans-serif}
main{max-width:1250px;margin:auto;padding:26px}
header{display:flex;justify-content:space-between;align-items:center;gap:15px;
flex-wrap:wrap;margin-bottom:22px}
h1{font-size:26px;margin:0}h2{font-size:16px;margin:0 0 16px}
p{color:var(--muted);margin:6px 0}
.status{color:var(--green);font-weight:700}
.notice{background:#17233a;border:1px solid var(--line);padding:12px;
border-radius:10px;color:#c6d6f3;margin-bottom:18px}
.metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));
gap:12px;margin-bottom:18px}
.card,.panel{background:var(--panel);border:1px solid var(--line);
border-radius:12px;padding:17px;min-width:0}
.metric-label{color:var(--muted);font-size:12px}
.metric{font-size:25px;font-weight:750;margin-top:8px;overflow-wrap:anywhere}
.columns{display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{text-align:right;padding:10px 7px;border-bottom:1px solid var(--line)}
th{color:var(--muted);font-weight:600}
th:first-child,td:first-child{text-align:left}
.bid{color:var(--green)}.ask{color:var(--red)}
.table-wrap{overflow-x:auto;max-height:390px;overflow-y:auto}
.full{margin-bottom:16px}
footer{color:var(--muted);font-size:12px;padding:12px 0}
@media(max-width:760px){main{padding:14px}.metrics{grid-template-columns:1fr 1fr}
.columns{grid-template-columns:1fr}h1{font-size:22px}}
</style>
</head>
<body><main>
<header>
<div><h1>ChronosMatch</h1><p>Live Order Book &amp; Market Simulator</p></div>
<div><span class="status" id="status">● CONNECTING</span><p id="updated">Waiting for data...</p></div>
</header>
<div class="notice" id="mode">Connecting to the matching engine...</div>
<section class="metrics">
<div class="card"><div class="metric-label">Processed Orders</div><div class="metric" id="processed">0</div></div>
<div class="card"><div class="metric-label">Executed Trades</div><div class="metric" id="trades">0</div></div>
<div class="card"><div class="metric-label">Throughput (orders/sec)</div><div class="metric" id="throughput">0</div></div>
<div class="card"><div class="metric-label">Simulation Time</div><div class="metric" id="elapsed">0 s</div></div>
</section>
<section class="metrics">
<div class="card"><div class="metric-label">Best Bid</div><div class="metric bid" id="bestBid">—</div></div>
<div class="card"><div class="metric-label">Best Ask</div><div class="metric ask" id="bestAsk">—</div></div>
<div class="card"><div class="metric-label">Spread</div><div class="metric" id="spread">—</div></div>
<div class="card"><div class="metric-label">Rejected Orders</div><div class="metric" id="rejected">0</div></div>
</section>
<section class="columns">
<div class="panel"><h2 class="bid">BUY ORDERS — BIDS</h2>
<div class="table-wrap"><table><thead><tr><th>Price</th><th>Quantity</th></tr></thead>
<tbody id="bids"></tbody></table></div></div>
<div class="panel"><h2 class="ask">SELL ORDERS — ASKS</h2>
<div class="table-wrap"><table><thead><tr><th>Price</th><th>Quantity</th></tr></thead>
<tbody id="asks"></tbody></table></div></div>
</section>
<section class="panel full"><h2>Recent Executed Trades</h2>
<div class="table-wrap"><table><thead><tr><th>Trade ID</th><th>Price</th>
<th>Qty</th><th>Buy Order</th><th>Sell Order</th></tr></thead>
<tbody id="recent"></tbody></table></div></section>
<footer>ChronosMatch educational prototype · Refreshes every 500 ms · Ctrl+C to stop</footer>
</main>
<script>
const el=id=>document.getElementById(id);
const fmt=v=>v==null?'—':Number(v).toLocaleString();
function rows(id,data,columns,cls=''){
 const body=el(id);
 body.innerHTML='';
 if(!data.length){body.innerHTML='<tr><td colspan="'+columns.length+
 '">No open orders</td></tr>';return;}
 for(const item of data){
  const tr=document.createElement('tr');
  if(cls)tr.className=cls;
  for(const key of columns){
   const td=document.createElement('td');
   td.textContent=fmt(item[key]);
   tr.appendChild(td);
  }
  body.appendChild(tr);
 }
}
async function refresh(){
 try{
  const response=await fetch('/api/state',{cache:'no-store'});
  if(!response.ok)throw Error('HTTP '+response.status);
  const d=await response.json();
  el('status').textContent='● '+d.status;
  el('mode').textContent=d.mode;
  for(const key of ['processed','trades','throughput','rejected'])
   el(key).textContent=fmt(d[key]);
  el('elapsed').textContent=d.elapsed+' s';
  el('bestBid').textContent=fmt(d.best_bid);
  el('bestAsk').textContent=fmt(d.best_ask);
  el('spread').textContent=fmt(d.spread);
  el('updated').textContent='Last update: '+d.updated;
  rows('bids',d.bids,['price','quantity'],'bid');
  rows('asks',d.asks,['price','quantity'],'ask');
  rows('recent',d.recent_trades,
       ['id','price','quantity','buy_order','sell_order']);
 }catch(e){el('status').textContent='● CONNECTION ERROR';
  el('updated').textContent=e.message;}
}
refresh();setInterval(refresh,500);
</script>
</body></html>"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/state":
            body = json.dumps(snapshot()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path in ("/", "/index.html"):
            body = PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_error(404)

    def log_message(self, fmt, *args):
        pass


def main():
    worker = threading.Thread(target=market_worker, daemon=True)
    worker.start()

    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print("=" * 55)
    print("  CHRONOSMATCH — MATCHING ENGINE DASHBOARD")
    print(f"  Open: http://{HOST}:{PORT}")
    print("  Uses MatchingEngine + OrderGenerator from your project.")
    print("  Press Ctrl+C to stop.")
    print("=" * 55)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping ChronosMatch dashboard...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
