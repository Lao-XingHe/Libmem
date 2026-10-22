# -*- coding: utf-8 -*-
r"""LibMem-AML · 本地轮询代理（多进程服务的入口）

## 为什么需要它（2026-10-22 第二次评测失败后加的）

平台要求 **16 并发**。实测单进程服务：单次搜索 **1.7s**，8 并发 p50 就变 **6.4s**，
16 并发 **12.4s** —— 那是**一个 Python 进程里 8 个线程抢一把 GIL**，
CPU 密集的检索逻辑只能排队。数据量越大越明显（42 万条库）。

**加线程没用**（GIL 挡着），**加进程才有用**。于是：
  8080 = 本代理（隧道指向它，公网地址不变）
  8081..808N = N 个真实服务进程，共享同一个 SQLite（WAL 支持并发读）

代价是每个进程各有自己的路由缓存（内存 ~250MB/进程）。

## 设计要点

* **只做转发**：不改 body、不改状态码，`/health`、`/add`、`/search` 一视同仁；
* **轮询**（round-robin）+ 失败**换一个后端重试一次**（写请求重试靠平台的
  `request_id` 幂等兜底，我们这边 `insert_memory` 也是 UPSERT）；
* **不缓冲整个流**：读完整响应再回（我们响应最大 ~100KB，可接受）；
* **超时 120s**：远大于我们实测的 2s，但小于平台上限。

用法::
    python proxy.py --listen 8080 --backends 8081,8082,8083,8084,8085,8086
"""
from __future__ import annotations

import argparse
import http.client
import itertools
import socket
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

BACKENDS: list = []
_rr = None
_rr_lock = threading.Lock()

HOP = {"host", "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
       "te", "trailers", "transfer-encoding", "upgrade", "content-length"}


def pick() -> tuple:
    """轮询取一个后端；每次都推进，所以重试会换到别的进程。"""
    with _rr_lock:
        return next(_rr)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "LibMem-AML-proxy/1.0"

    def log_message(self, fmt, *args):          # 安静一点：只留错误
        pass

    def _relay(self) -> None:
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        body = self.rfile.read(length) if length > 0 else b""
        headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP}

        last_error = ""
        for attempt in range(2):               # 失败换后端重试一次
            host, port = pick()
            try:
                conn = http.client.HTTPConnection(host, port, timeout=120)
                conn.request(self.command, self.path, body=body or None, headers=headers)
                resp = conn.getresponse()
                data = resp.read()
                conn.close()
                self.send_response(resp.status)
                for key, value in resp.getheaders():
                    if key.lower() not in ("transfer-encoding", "connection", "content-length"):
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(data)
                return
            except (ConnectionRefusedError, socket.timeout, OSError, http.client.HTTPException) as error:
                last_error = f"{type(error).__name__}: {error}"
                continue
        sys.stderr.write(f"[proxy] {self.command} {self.path} 全部后端失败: {last_error}\n")
        try:
            self.send_response(502)
            self.send_header("Content-Length", "0")
            self.end_headers()
        except OSError:
            pass

    do_GET = _relay
    do_POST = _relay
    do_HEAD = _relay


def main() -> int:
    global _rr
    ap = argparse.ArgumentParser()
    ap.add_argument("--listen", type=int, default=8080)
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--backends", required=True, help="如 8081,8082,8083")
    args = ap.parse_args()

    for item in args.backends.split(","):
        item = item.strip()
        if item:
            BACKENDS.append(("127.0.0.1", int(item)))
    if not BACKENDS:
        print("没有可用后端", file=sys.stderr)
        return 2
    _rr = itertools.cycle(BACKENDS)

    httpd = ThreadingHTTPServer((args.host, args.listen), Handler)
    httpd.daemon_threads = True
    print(f"[proxy] {args.host}:{args.listen} → {len(BACKENDS)} 个后端 "
          f"({', '.join(str(p) for _, p in BACKENDS)})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
