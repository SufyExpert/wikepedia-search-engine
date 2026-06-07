import re
import time
import requests
from flask import Flask, render_template, request, jsonify
from elasticsearch import Elasticsearch

app = Flask(__name__)

# Constants and connections
ES_HOST    = "http://localhost:9200"
SOLR_HOST  = "http://localhost:8983"
INDEX_NAME = "wikipedia"
CORE_NAME  = "wikipedia"
TOP_N      = 10

es = Elasticsearch(ES_HOST, request_timeout=30)

def clean_wiki(text, max_len=300):
    """Normalize and clean raw Wikipedia text markup for UI display."""
    if not text:
        return ""
    # Strip double brace nested templates (up to 6 levels deep)
    for _ in range(6):
        text = re.sub(r'\{\{[^{}]*\}\}', '', text)
    text = re.sub(r'\{[^{}]*\}', '', text)
    text = re.sub(r'\{+|\}+', '', text)
    
    # Resolve links and remove styling
    text = re.sub(r'\[\[(?:[^|\]]*\|)?([^\]]+)\]\]', r'\1', text)
    text = re.sub(r"'{2,}", '', text)
    
    # Parse headers and clean URLs
    text = re.sub(r'===+\s*([^=]+?)\s*===+', r'\1: ', text)
    text = re.sub(r'==\s*([^=]+?)\s*==', r'\1: ', text)
    text = re.sub(r'\[https?://[^\] ]*\s*([^\]]*)\]', r'\1', text)
    text = re.sub(r'https?://\S+', '', text)
    
    # Strip HTML tags, tables, and internal template tags
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'(scope|class|style|valign|align|width|cellpadding|cellspacing|bgcolor|border)="[^"]*"', '', text, flags=re.IGNORECASE)
    text = re.sub(r'(scope|class|style|valign|align|width)="[^"]*"', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\w+="[^"]*"', '', text)
    text = re.sub(r'\|\|', ' ', text)
    text = re.sub(r'^\|.*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'^!.*$', '', text, flags=re.MULTILINE)
    text = re.sub(r'\|[^\n|]{0,100}', '', text)
    
    # Strip file metadata, infobox definitions, and bullet symbols
    text = re.sub(r'(?m)^\s*(Category|File|Image|thumb|right|left|px)\s*:.*$', '', text)
    text = re.sub(r'\b(Category|File|Image):[^\s\]]+', '', text)
    text = re.sub(r'\bInbox\s+\w+', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\bInfobox\s+\w+', '', text, flags=re.IGNORECASE)
    text = re.sub(r'^[*#:;]+\s*', '', text, flags=re.MULTILINE)
    
    # Normalize whitespaces
    text = re.sub(r'\s+', ' ', text).strip()
    
    # Prune initial non-alphabetic noise
    m = re.search(r'[A-Za-z]', text)
    if m:
        text = text[m.start():]
    return text[:max_len] if max_len else text

def clean_content(text):
    return clean_wiki(text, max_len=280)

def search_elasticsearch(query_text, size=TOP_N):
    """Execute match-phrase queries in Elasticsearch with BM25 ranking."""
    start = time.perf_counter()
    try:
        query = {
            "query": {
                "multi_match": {
                    "query":  query_text,
                    "fields": ["title^3", "content"],
                    "type":   "best_fields"
                }
            },
            "size": size,
            "track_total_hits": True
        }
        response = es.search(index=INDEX_NAME, body=query)
        elapsed = round((time.perf_counter() - start) * 1000, 2)

        results = []
        for hit in response["hits"]["hits"]:
            results.append({
                "title":   hit["_source"].get("title", "N/A"),
                "score":   round(hit["_score"], 4),
                "snippet": clean_content(hit["_source"].get("content", ""))
            })

        return {
            "results":       results,
            "total":         response["hits"]["total"]["value"],
            "time_ms":       elapsed,
            "top_score":     results[0]["score"] if results else 0,
            "status":        "ok"
        }
    except Exception as e:
        elapsed = round((time.perf_counter() - start) * 1000, 2)
        return {"results": [], "total": 0, "time_ms": elapsed, "top_score": 0, "status": "error", "error": str(e)}

def search_solr(query_text, size=TOP_N):
    """Query Solr core with edismax and highlighting configurations."""
    start = time.perf_counter()
    try:
        url    = f"{SOLR_HOST}/solr/{CORE_NAME}/select"
        params = {
            "defType": "edismax",
            "q":       query_text,
            "qf":      "title^3 content",
            "fl":      "id,title,score,content",
            "rows":    size,
            "wt":      "json",
            "hl":         "true",
            "hl.fl":      "content",
            "hl.snippets": 1,
            "hl.fragsize": 250
        }
        response = requests.get(url, params=params, timeout=15)
        data     = response.json()
        elapsed  = round((time.perf_counter() - start) * 1000, 2)

        highlights = data.get("highlighting", {})
        results    = []
        for doc in data["response"]["docs"]:
            doc_id  = doc.get("id", "")
            title   = doc.get("title", "N/A")
            if isinstance(title, list):
                title = title[0]
            score   = round(doc.get("score", 0), 4)
            hl_data = highlights.get(doc_id, {})
            snippets= hl_data.get("content", [])
            content = doc.get("content", "")
            if isinstance(content, list):
                content = content[0]
            raw_snip = snippets[0] if snippets else content
            snippet  = clean_content(raw_snip)

            results.append({"title": title, "score": score, "snippet": snippet})

        return {
            "results":   results,
            "total":     data["response"]["numFound"],
            "time_ms":   elapsed,
            "top_score": results[0]["score"] if results else 0,
            "status":    "ok"
        }
    except Exception as e:
        elapsed = round((time.perf_counter() - start) * 1000, 2)
        return {"results": [], "total": 0, "time_ms": elapsed, "top_score": 0, "status": "error", "error": str(e)}

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/search")
def search():
    """Rank-fusion intersection search route."""
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify({"error": "Empty query"}), 400

    es_comp   = search_elasticsearch(query, size=100)
    solr_comp = search_solr(query, size=100)

    es_map   = {r["title"]: r["score"] for r in es_comp["results"]}
    solr_map = {r["title"]: r["score"] for r in solr_comp["results"]}
    
    common_titles = set(es_map.keys()) & set(solr_map.keys())
    
    common_results = []
    for title in common_titles:
        combined_score = round(es_map[title] + solr_map[title], 4)
        common_results.append({"title": title, "combined_score": combined_score})
    
    common_results.sort(key=lambda x: x["combined_score"], reverse=True)
    sorted_common_titles = [r["title"] for r in common_results]
    
    es_data   = {"results": es_comp["results"][:10], "total": es_comp["total"], "time_ms": es_comp["time_ms"], "top_score": es_comp["top_score"]}
    solr_data = {"results": solr_comp["results"][:10], "total": solr_comp["total"], "time_ms": solr_comp["time_ms"], "top_score": solr_comp["top_score"]}

    return jsonify({
        "query":      query,
        "elasticsearch": es_data,
        "solr":          solr_data,
        "common_titles": sorted_common_titles[:10],
        "common_count":  len(common_titles)
    })

@app.route("/article")
def get_article():
    """Retrieve full clean text from Elasticsearch using exact-phrase matching."""
    title = request.args.get("title", "").strip()
    if not title:
        return jsonify({"content": "No title provided."})
    try:
        res = es.search(index=INDEX_NAME, body={
            "query": {"bool": {"should": [
                {"match_phrase": {"title": {"query": title, "boost": 3}}},
                {"match":        {"title": title}}
            ]}},
            "size": 1
        })
        hits = res["hits"]["hits"]
        if hits:
            raw = hits[0]["_source"].get("content", "")
            return jsonify({"content": clean_content_full(raw)})
        return jsonify({"content": "Article not found."})
    except Exception as e:
        return jsonify({"content": f"Error: {e}"})

def clean_content_full(text):
    """Format full wiki articles into clean paragraph-based structured HTML."""
    if not text: 
        return "<p>No content available.</p>"
    clean = clean_wiki(text, max_len=None)
    lines = clean.split(' ')
    html_parts = []
    current_para = []
    
    for word in lines:
        current_para.append(word)
        joined = ' '.join(current_para)
        if len(joined) > 400:
            html_parts.append(f'<p>{joined}</p>')
            current_para = []
            
    if current_para:
        html_parts.append(f'<p>{" ".join(current_para)}</p>')
        
    final = ''
    for part in html_parts:
        content = re.sub(r'^<p>(.*)</p>$', r'\1', part)
        if content.endswith(':') and len(content) < 80:
            heading = content.rstrip(':').strip().title()
            final += f'<h6 class="modal-section-heading">{heading}</h6>'
        else:
            final += part
    return final or "<p>No readable content found.</p>"

if __name__ == "__main__":
    app.run(debug=True, port=5000)
