import os
import sys
import re
import time
import requests
from urllib.parse import urljoin

TARGET_SIZE_GB = 7
DATA_DIR        = os.path.join(os.path.dirname(__file__), "..", "data")
CHUNKS_DIR      = os.path.join(DATA_DIR, "enwiki_chunks")
DUMP_INDEX_URL  = "https://dumps.wikimedia.org/enwiki/latest/"

MAX_WAIT_SECONDS = 300
RETRY_INTERVAL   = 30

def wait_for_internet(total_waited):
    """Wait for network connectivity restoration before retrying."""
    if total_waited >= MAX_WAIT_SECONDS:
        print(f"\n  [!] Connection failed. Terminating download session.")
        return False
    
    print(f"\n  [!] Network issue detected. Retrying in {RETRY_INTERVAL}s... "
          f"({total_waited}/{MAX_WAIT_SECONDS}s elapsed)")
    time.sleep(RETRY_INTERVAL)
    return True

def download_file_robust(url, filepath):
    """Download single chunk from Wikimedia dump server with auto-resume logic."""
    filename = os.path.basename(filepath)
    total_waited = 0
    
    while True:
        try:
            response = requests.get(url, stream=True, timeout=20)
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))

            if os.path.exists(filepath):
                local_size = os.path.getsize(filepath)
                if local_size == total_size:
                    print(f"  [Match] {filename} exists. Skipping.")
                    return True
                else:
                    print(f"  [Partial] {filename} is incomplete. Re-downloading...")
                    os.remove(filepath)

            print(f"  Downloading: {filename}")
            downloaded = 0
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=1024*1024):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        percent = (downloaded / total_size) * 100
                        mb_done = downloaded // 1024 // 1024
                        total_mb = total_size // 1024 // 1024
                        print(f"\r  Progress: {percent:5.1f}%  ({mb_done}/{total_mb} MB)", end="")
            
            print(f"\n  [Done] {filename} saved.")
            return True

        except (requests.exceptions.RequestException, Exception):
            if not wait_for_internet(total_waited):
                return False
            total_waited += RETRY_INTERVAL

def get_chunk_urls():
    """Extract list of latest compressed bz2 Wikipedia chunk URLs from Wikimedia registry."""
    print("  Connecting to Wikimedia Dump Server...")
    try:
        r = requests.get(DUMP_INDEX_URL, timeout=30)
        r.raise_for_status()
        html = r.text
        pattern = r'(enwiki-latest-pages-articles\d+\.xml-p\d+p\d+\.bz2)'
        chunks = sorted(list(set(re.findall(pattern, html))))
        return [urljoin(DUMP_INDEX_URL, c) for c in chunks]
    except Exception as e:
        print(f"  ERROR: Could not fetch dump index: {e}")
        sys.exit(1)

def main():
    print()
    print("=" * 65)
    print("  ROBUST WIKIPEDIA DOWNLOADER (English Chunks)")
    print(f"  Target Size: {TARGET_SIZE_GB} GB | Retry Limit: 5 Mins")
    print("=" * 65)
    print()

    os.makedirs(CHUNKS_DIR, exist_ok=True)
    urls = get_chunk_urls()
    
    target_bytes = TARGET_SIZE_GB * 1024 * 1024 * 1024
    total_bytes_on_disk = 0
    count = 0

    for f in os.listdir(CHUNKS_DIR):
        if f.endswith(".bz2"):
            total_bytes_on_disk += os.path.getsize(os.path.join(CHUNKS_DIR, f))

    for url in urls:
        if total_bytes_on_disk >= target_bytes:
            break
        
        filename = url.split("/")[-1]
        filepath = os.path.join(CHUNKS_DIR, filename)
        count += 1
        
        print(f"\n  [File {count}] Target: {total_bytes_on_disk/1024**3:.2f}/{TARGET_SIZE_GB} GB")
        
        if not download_file_robust(url, filepath):
            print("\n  [Stopped] Download aborted due to connection issues.")
            sys.exit(1)
            
        total_bytes_on_disk += os.path.getsize(filepath)

    print()
    print("=" * 65)
    print(f"  SUCCESS! Total Data: {total_bytes_on_disk/1024**3:.2f} GB")
    print(f"  Location: {CHUNKS_DIR}")
    print("=" * 65)

if __name__ == "__main__":
    main()
