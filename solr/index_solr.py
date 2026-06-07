import json
import os
import requests

DATA_DIR      = os.path.join(os.path.dirname(__file__), "..", "data")
SOLR_HOST  = "http://localhost:8983"
CORE_NAME  = "wikipedia"
BATCH_SIZE = 100

def check_solr():
    """Verify Solr node availability."""
    print("  Checking Solr connection at:", SOLR_HOST)
    try:
        response = requests.get(f"{SOLR_HOST}/solr/admin/cores", timeout=10)
        if response.status_code == 200:
            print("  Solr is running!")
            data = response.json()
            cores = list(data.get("status", {}).keys())
            print(f"  Available cores: {cores}")
            return True
    except Exception as e:
        print(f"  ERROR: Cannot connect to Solr: {e}")
        print("  Make sure Docker containers are running!")
        return False

def check_core():
    """Verify core registry."""
    print(f"\n  Checking Solr core '{CORE_NAME}'...")
    url = f"{SOLR_HOST}/solr/admin/cores?action=STATUS&core={CORE_NAME}"
    response = requests.get(url, timeout=10)
    data = response.json()

    if CORE_NAME in data.get("status", {}):
        core_info = data["status"][CORE_NAME]
        num_docs = core_info.get("index", {}).get("numDocs", 0)
        print(f"  Core '{CORE_NAME}' found! Current documents: {num_docs}")
        return True
    else:
        print(f"  ERROR: Core '{CORE_NAME}' not found!")
        return False

def clear_core():
    """Clear all documents in the active core."""
    print(f"\n  Clearing existing documents from core '{CORE_NAME}'...")
    url = f"{SOLR_HOST}/solr/{CORE_NAME}/update?commit=true"
    payload = {"delete": {"query": "*:*"}}

    response = requests.post(
        url,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=30
    )
    if response.status_code == 200:
        print("  All existing documents deleted.")
    else:
        print(f"  Warning: Could not clear core. Status: {response.status_code}")

def index_articles():
    """Stream clean articles to Solr in bulk batches."""
    path1 = os.path.join(DATA_DIR, "articles.jsonl")
    path2 = os.path.join(DATA_DIR, "articles.jsonll")
    ARTICLES_JSONL = path2 if os.path.exists(path2) else path1

    if not os.path.exists(ARTICLES_JSONL):
        print(f"  ERROR: No articles file found (checked .jsonl and .jsonll)")
        return

    print(f"\n  Indexing articles into Solr (Streaming)...")
    print(f"  Batch size: {BATCH_SIZE}\n")

    batch = []
    total_indexed = 0

    with open(ARTICLES_JSONL, "r", encoding="utf-8") as f:
        for line in f:
            article = json.loads(line)
            batch.append({
                "id":      article["id"],
                "title":   article["title"],
                "content": article["content"]
            })

            if len(batch) >= BATCH_SIZE:
                url = f"{SOLR_HOST}/solr/{CORE_NAME}/update?commit=false"
                requests.post(url, json=batch, timeout=30)
                total_indexed += len(batch)
                batch = []
                
                if total_indexed % 5000 == 0:
                    print(f"  Processed: {total_indexed} articles...")

    if batch:
        url = f"{SOLR_HOST}/solr/{CORE_NAME}/update?commit=false"
        requests.post(url, json=batch, timeout=30)
        total_indexed += len(batch)

    print("\n  Committing changes to Solr...")
    requests.get(f"{SOLR_HOST}/solr/{CORE_NAME}/update?commit=true", timeout=30)
    print(f"  Indexing complete! Total: {total_indexed} articles")
    return total_indexed

def verify_with_search():
    """Run baseline query to verify index integrity."""
    print("\n  Running verification search in Solr...")
    print("-" * 50)

    url = f"{SOLR_HOST}/solr/{CORE_NAME}/select"
    params = {
        "q":    "content:computer",
        "fl":   "id,title,score",
        "rows": 3,
        "wt":   "json"
    }

    response = requests.get(url, params=params, timeout=10)
    data = response.json()
    total = data["response"]["numFound"]
    print(f"  Search 'computer' → {total} articles found\n")

    for doc in data["response"]["docs"]:
        title = doc.get("title", "N/A")
        score = doc.get("score", 0)
        print(f"  Score: {score:.4f} | Title: {title}")

    print("\n  Solr indexing verified!")

if __name__ == "__main__":
    if not check_solr():
        exit(1)
    if not check_core():
        exit(1)
    clear_core()
    index_articles()
    verify_with_search()
