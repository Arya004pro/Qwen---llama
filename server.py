"""server.py — serves dashboard.html locally.

Run from the project root:
    python server.py

Then open: http://localhost:8080/dashboard.html
(The browser opens automatically.)
"""

import http.server
import socketserver
import os
import webbrowser
import threading

PORT = 8080
DIR  = os.path.dirname(os.path.abspath(__file__))


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=DIR, **kwargs)

    def log_message(self, fmt, *args):
        # Clean log format
        print(f"  [{self.address_string()}] {fmt % args}")


def open_browser():
    import time
    time.sleep(1.0)
    webbrowser.open(f"http://localhost:{PORT}/dashboard.html")


print()
print("=" * 50)
print(f"  Dashboard → http://localhost:{PORT}/dashboard.html")
print(f"  Serving files from: {DIR}")
print("  Press Ctrl+C to stop.")
print("=" * 50)
print()

threading.Thread(target=open_browser, daemon=True).start()

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")