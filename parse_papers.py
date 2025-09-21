# parse_pdfs.py
import os
import time
import json
import requests
from pathlib import Path
from grobid_client.grobid_client import GrobidClient
from tqdm import tqdm

# Config file path (adjust if needed)
CONFIG_PATH = "grobid_config.json"

def wait_for_grobid(server_url, timeout=120):
    """Wait until GROBID responds positively to /api/isalive or admin port."""
    alive_url = server_url.rstrip("/") + "/api/isalive"
    admin_url = server_url.replace(":8070", ":8071")
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(alive_url, timeout=5)
            if r.status_code == 200 and r.text.strip().lower() in ("true", "ok"):
                print(f"GROBID alive at {alive_url}")
                return True
        except Exception:
            pass
        try:
            r2 = requests.get(admin_url, timeout=5)
            if r2.status_code == 200:
                print(f"GROBID admin reachable at {admin_url}")
                return True
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError(f"GROBID not responding after {timeout}s (checked {alive_url} and {admin_url})")

def main():
    with open(CONFIG_PATH, "r") as fh:
        cfg = json.load(fh)

    server = cfg.get("grobid_server", "http://grobid:8070")
    outdir = Path(cfg.get("output", "./grobid_output"))
    outdir.mkdir(parents=True, exist_ok=True)
    input_dir = Path("./pdfs")
    if not input_dir.exists():
        raise FileNotFoundError("Please create a ./pdfs directory with PDFs to process")

    # 1) wait for server
    print("Waiting for GROBID server...")
    wait_for_grobid(server)

    # 2) instantiate client
    client = GrobidClient(config_path=CONFIG_PATH)

    # 3) process all PDFs -- the client provides concurrent processing via `n`
    # Set n to a reasonable value. Lower if you run into memory or HTTP 503.
    concurrency = 8
    print(f"Processing PDFs from {input_dir} with concurrency n={concurrency} ...")

    # The grobid-client-python 'process' method writes TEI outputs in the output dir.
    client.process(cfg["process"]["service"], str(input_dir), n=concurrency)

    print("Done. Results are in:", outdir)

if __name__ == "__main__":
    main()
