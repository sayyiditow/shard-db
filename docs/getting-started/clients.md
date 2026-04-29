# Client examples

shard-db speaks newline-delimited JSON over TCP. Any TCP-capable language can talk to it — no client library required. Below: terminal one-liners (socat / nc), and reference clients in Python, Java, and Node.js.

For the full wire format (request/response framing, pipelining, errors), see [Query protocol → Overview](../query-protocol/overview.md). For TLS-wrapped variants, see [Operations → Deployment](../operations/deployment.md).

## Terminal: socat / nc

```bash
# Using socat
echo '{"mode":"get","dir":"default","object":"users","key":"user1"}' \
  | socat - TCP:localhost:9199

# Using nc/ncat
echo '{"mode":"insert","dir":"default","object":"users","key":"u1","value":{"name":"Bob"}}' \
  | nc -q1 localhost 9199

# Pipeline multiple requests over one connection
printf '%s\n%s\n%s\n' \
  '{"mode":"insert","dir":"default","object":"users","key":"u1","value":{"name":"Alice"}}' \
  '{"mode":"insert","dir":"default","object":"users","key":"u2","value":{"name":"Bob"}}' \
  '{"mode":"get","dir":"default","object":"users","key":"u1"}' \
  | socat - TCP:localhost:9199
```

## Python

```python
import socket
import json

def query(request, host="localhost", port=9199):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    sock.sendall((json.dumps(request) + "\n").encode())
    sock.shutdown(socket.SHUT_WR)
    data = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
    sock.close()
    # Strip trailing null byte
    text = data.decode().strip().rstrip("\x00").strip()
    return json.loads(text) if text else None

# Create an object
query({
    "mode": "create-object",
    "dir": "default",
    "object": "products",
    "splits": 16,
    "max_key": 128,
    "fields": ["name:varchar:100", "price:numeric:10,2", "stock:int", "active:bool"]
})

# Insert
query({
    "mode": "insert",
    "dir": "default",
    "object": "products",
    "key": "prod-001",
    "value": {"name": "Widget", "price": "29.99", "stock": 150, "active": True}
})

# Insert only if key doesn't exist
query({
    "mode": "insert",
    "dir": "default",
    "object": "products",
    "key": "prod-001",
    "value": {"name": "Widget", "price": "29.99", "stock": 150, "active": True},
    "if_not_exists": True
})

# Update with compare-and-swap
query({
    "mode": "update",
    "dir": "default",
    "object": "products",
    "key": "prod-001",
    "value": {"stock": 149},
    "if": [{"field": "stock", "op": "eq", "value": "150"}]
})

# Get
result = query({"mode": "get", "dir": "default", "object": "products", "key": "prod-001"})

# Get with field projection
result = query({
    "mode": "get",
    "dir": "default",
    "object": "products",
    "key": "prod-001",
    "fields": "name,price"
})

# Multi-get
results = query({
    "mode": "get",
    "dir": "default",
    "object": "products",
    "keys": ["prod-001", "prod-002", "prod-003"]
})

# Find with criteria
results = query({
    "mode": "find",
    "dir": "default",
    "object": "products",
    "criteria": [
        {"field": "price", "op": "lt", "value": "50.00"},
        {"field": "active", "op": "eq", "value": "true"}
    ],
    "offset": 0,
    "limit": 20
})

# Count matching records
result = query({
    "mode": "count",
    "dir": "default",
    "object": "products",
    "criteria": [{"field": "active", "op": "eq", "value": "true"}]
})

# Bulk insert
query({
    "mode": "bulk-insert",
    "dir": "default",
    "object": "products",
    "records": [
        {"id": "prod-100", "data": {"name": "Gadget A", "price": "9.99", "stock": 500}},
        {"id": "prod-101", "data": {"name": "Gadget B", "price": "19.99", "stock": 250}}
    ]
})

# Bulk update by criteria
query({
    "mode": "bulk-update",
    "dir": "default",
    "object": "products",
    "criteria": [{"field": "stock", "op": "eq", "value": "0"}],
    "value": {"active": False},
    "limit": 1000
})

# Bulk delete by criteria (dry run first)
query({
    "mode": "bulk-delete",
    "dir": "default",
    "object": "products",
    "criteria": [{"field": "active", "op": "eq", "value": "false"}],
    "dry_run": True
})
```

### Pipelining (Python)

```python
def pipeline(requests, host="localhost", port=9199):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    payload = "".join(json.dumps(r) + "\n" for r in requests)
    sock.sendall(payload.encode())
    sock.shutdown(socket.SHUT_WR)
    data = b""
    while True:
        chunk = sock.recv(65536)
        if not chunk:
            break
        data += chunk
    sock.close()
    results = []
    for line in data.decode().split("\x00"):
        line = line.strip()
        if line:
            results.append(json.loads(line))
    return results

# Send 1000 inserts in one TCP connection
records = [
    {"mode": "insert", "dir": "default", "object": "products",
     "key": f"bulk-{i}", "value": {"name": f"Item {i}", "price": "1.00", "stock": i}}
    for i in range(1000)
]
results = pipeline(records)
```

### TLS variant (Python)

```python
import ssl

def query_tls(request, host="your-server", port=9200):
    ctx = ssl.create_default_context()
    # For self-signed certs during dev:
    # ctx.check_hostname = False
    # ctx.verify_mode = ssl.CERT_NONE
    raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock = ctx.wrap_socket(raw, server_hostname=host)
    sock.connect((host, port))
    sock.sendall((json.dumps(request) + "\n").encode())
    sock.shutdown(socket.SHUT_WR)
    data = b""
    while True:
        chunk = sock.recv(4096)
        if not chunk: break
        data += chunk
    sock.close()
    return json.loads(data.decode().strip().rstrip("\x00").strip())
```

## Java

```java
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class ShardDbClient {

    private final String host;
    private final int port;

    public ShardDbClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String query(String json) throws IOException {
        try (Socket socket = new Socket(host, port)) {
            OutputStream out = socket.getOutputStream();
            out.write((json + "\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            socket.shutdownOutput();

            InputStream in = socket.getInputStream();
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] tmp = new byte[4096];
            int n;
            while ((n = in.read(tmp)) != -1) {
                buf.write(tmp, 0, n);
            }
            String result = buf.toString(StandardCharsets.UTF_8).trim();
            if (result.endsWith("\0")) {
                result = result.substring(0, result.length() - 1).trim();
            }
            return result;
        }
    }

    /** Pipeline multiple requests over one connection */
    public String[] pipeline(String[] requests) throws IOException {
        try (Socket socket = new Socket(host, port)) {
            OutputStream out = socket.getOutputStream();
            StringBuilder sb = new StringBuilder();
            for (String req : requests) {
                sb.append(req).append("\n");
            }
            out.write(sb.toString().getBytes(StandardCharsets.UTF_8));
            out.flush();
            socket.shutdownOutput();

            InputStream in = socket.getInputStream();
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            byte[] tmp = new byte[65536];
            int n;
            while ((n = in.read(tmp)) != -1) {
                buf.write(tmp, 0, n);
            }
            return buf.toString(StandardCharsets.UTF_8).split("\0");
        }
    }

    public static void main(String[] args) throws IOException {
        ShardDbClient db = new ShardDbClient("localhost", 9199);

        // Create object
        db.query("{\"mode\":\"create-object\",\"dir\":\"default\",\"object\":\"orders\","
               + "\"splits\":16,\"max_key\":128,"
               + "\"fields\":[\"customer:varchar:100\",\"total:numeric:12,2\",\"status:varchar:20\",\"created:datetime\"],"
               + "\"indexes\":[\"customer\",\"status\"]}");

        // Insert
        System.out.println(db.query(
            "{\"mode\":\"insert\",\"dir\":\"default\",\"object\":\"orders\","
          + "\"key\":\"ord-001\","
          + "\"value\":{\"customer\":\"Alice\",\"total\":\"249.99\",\"status\":\"pending\"}}"));

        // Update with CAS
        System.out.println(db.query(
            "{\"mode\":\"update\",\"dir\":\"default\",\"object\":\"orders\","
          + "\"key\":\"ord-001\","
          + "\"value\":{\"status\":\"shipped\"},"
          + "\"if\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"pending\"}]}"));

        // Find
        System.out.println(db.query(
            "{\"mode\":\"find\",\"dir\":\"default\",\"object\":\"orders\","
          + "\"criteria\":[{\"field\":\"status\",\"op\":\"eq\",\"value\":\"shipped\"}],"
          + "\"limit\":50}"));

        // Pipeline
        String[] results = db.pipeline(new String[]{
            "{\"mode\":\"get\",\"dir\":\"default\",\"object\":\"orders\",\"key\":\"ord-001\"}",
            "{\"mode\":\"size\",\"dir\":\"default\",\"object\":\"orders\"}"
        });
        for (String r : results) System.out.println(r.trim());
    }
}
```

### TLS variant (Java)

```java
import javax.net.ssl.SSLSocketFactory;
SSLSocketFactory sf = (SSLSocketFactory) SSLSocketFactory.getDefault();
Socket socket = sf.createSocket("your-server", 9200);
// ... same read/write logic as plain TCP client above
```

## Node.js

```javascript
const net = require("net");

function query(request, host = "localhost", port = 9199) {
  return new Promise((resolve, reject) => {
    const socket = new net.Socket();
    let data = "";

    socket.connect(port, host, () => {
      socket.write(JSON.stringify(request) + "\n");
      socket.end();
    });

    socket.on("data", (chunk) => (data += chunk.toString()));
    socket.on("end", () => {
      const text = data.replace(/\0/g, "").trim();
      try { resolve(JSON.parse(text)); }
      catch { resolve(text); }
    });
    socket.on("error", reject);
  });
}

function pipeline(requests, host = "localhost", port = 9199) {
  return new Promise((resolve, reject) => {
    const socket = new net.Socket();
    let data = "";

    socket.connect(port, host, () => {
      const payload = requests.map((r) => JSON.stringify(r)).join("\n") + "\n";
      socket.write(payload);
      socket.end();
    });

    socket.on("data", (chunk) => (data += chunk.toString()));
    socket.on("end", () => {
      const results = data
        .split("\0")
        .map((s) => s.trim())
        .filter(Boolean)
        .map((s) => { try { return JSON.parse(s); } catch { return s; } });
      resolve(results);
    });
    socket.on("error", reject);
  });
}

(async () => {
  // Create object
  await query({
    mode: "create-object", dir: "default", object: "events",
    splits: 16, max_key: 128,
    fields: [
      "type:varchar:50", "source:varchar:100",
      "payload:varchar:2000", "timestamp:datetime", "severity:short",
    ],
    indexes: ["type", "source"],
  });

  // Insert
  await query({
    mode: "insert", dir: "default", object: "events", key: "evt-001",
    value: { type: "login", source: "web", payload: '{"ip":"1.2.3.4"}', severity: 1 },
  });

  // Find
  console.log(await query({
    mode: "find", dir: "default", object: "events",
    criteria: [{ field: "type", op: "eq", value: "login" }],
    limit: 100,
  }));

  // Pipeline 1000 inserts
  const reqs = Array.from({ length: 1000 }, (_, i) => ({
    mode: "insert", dir: "default", object: "events",
    key: `batch-${i}`,
    value: { type: "batch", source: "script", severity: 1 },
  }));
  const results = await pipeline(reqs);
  console.log(`Pipelined ${results.length} responses`);
})();
```

### TLS variant (Node.js)

```javascript
const tls = require("tls");
function queryTls(request, host = "your-server", port = 9200) {
  return new Promise((resolve, reject) => {
    const socket = tls.connect(port, host, () => {
      socket.write(JSON.stringify(request) + "\n");
      socket.end();
    });
    let data = "";
    socket.on("data", (chunk) => (data += chunk.toString()));
    socket.on("end", () => resolve(JSON.parse(data.replace(/\0/g, "").trim())));
    socket.on("error", reject);
  });
}
```
