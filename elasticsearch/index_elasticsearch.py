import json
import os
import time
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

DATA_DIR    = os.path.join(os.path.dirname(__file__), "..", "data")
ES_HOST  = "http://localhost:9200"
INDEX_NAME = "wikipedia"
BATCH_SIZE = 100

def connect_elasticsearch():
    """Establish connection to the Elasticsearch cluster."""
    print("  Connecting to Elasticsearch at:", ES_HOST)
    es = Elasticsearch(ES_HOST, request_timeout=60, retry_on_timeout=True)

    for attempt in range(10):
        try:
            if es.ping():
                print("  Connected successfully!")
                info = es.info()
                print(f"  Elasticsearch version: {info['version']['number']}")
                return es
        except Exception:
            pass
        print(f"  Waiting for Elasticsearch... attempt {attempt + 1}/10")
        time.sleep(5)

    raise ConnectionError("Could not connect to Elasticsearch after 10 attempts.")

def create_index(es):
    """Define index mapping configurations with customized BM25 similarity."""
    print("\n  Creating Elasticsearch index...")

    if es.indices.exists(index=INDEX_NAME):
        print(f"  Index '{INDEX_NAME}' already exists. Deleting...")
        es.indices.delete(index=INDEX_NAME)

    mapping = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0,
            "similarity": {
                "default": {
                    "type": "BM25",
                    "b": 0.75,
                    "k1": 1.2
                }
            }
        },
        "mappings": {
            "properties": {
                "id": {
                    "type": "keyword"
                },
                "title": {
                    "type": "text",
                    "analyzer": "english",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "content": {
                    "type": "text",
                    "analyzer": "english"
                }
            }
        }
    }

    es.indices.create(index=INDEX_NAME, body=mapping)
    print(f"  Index '{INDEX_NAME}' created successfully!")

def index_articles(es):
    """Stream clean articles to Elasticsearch using parallel bulk requests."""
    print("\n  Indexing articles into Elasticsearch (Streaming)...")
    
    path1 = os.path.join(DATA_DIR, "articles.jsonl")
    path2 = os.path.join(DATA_DIR, "articles.jsonll")
    ARTICLES_JSONL = path2 if os.path.exists(path2) else path1
    
    if not os.path.exists(ARTICLES_JSONL):
        print(f"  ERROR: No articles file found (checked .jsonl and .jsonll)")
        return

    def generate_actions():
        with open(ARTICLES_JSONL, "r", encoding="utf-8") as f:
            for line in f:
                article = json.loads(line)
                yield {
                    "_index": INDEX_NAME,
                    "_id":    article["id"],
                    "_source": {
                        "id":      article["id"],
                        "title":   article["title"],
                        "content": article["content"]
                    }
                }

    print(f"  Starting bulk index... (Batch size: {BATCH_SIZE})")
    
    success = 0
    failed = 0
    
    for ok, result in streaming_bulk(
        es, 
        generate_actions(), 
        chunk_size=BATCH_SIZE,
        request_timeout=60,
        raise_on_error=False
    ):
        if ok:
            success += 1
        else:
            failed += 1
        
        if (success + failed) % 500 == 0:
            print(f"  Indexed: {success + failed} articles...")

    print(f"\n  Indexing complete!")
    print(f"  Successfully indexed: {success} articles")
    print(f"  Failed: {failed} articles")

    es.indices.refresh(index=INDEX_NAME)
    print("  Index refreshed - articles are now searchable!")

def verify_with_search(es):
    """Confirm index readability with a baseline test query."""
    print("\n  Running verification search...")
    print("-" * 50)

    query = {
        "query": {
            "match": {
                "content": "computer"
            }
        },
        "size": 3
    }

    response = es.search(index=INDEX_NAME, body=query)
    total = response["hits"]["total"]["value"]
    print(f"  Search 'computer' → {total} articles found\n")

    for hit in response["hits"]["hits"]:
        title = hit["_source"]["title"]
        score = hit["_score"]
        print(f"  Score: {score:.4f} | Title: {title}")

    print("\n  Elasticsearch indexing verified!")

if __name__ == "__main__":
    es = connect_elasticsearch()
    create_index(es)
    index_articles(es)
    verify_with_search(es)
