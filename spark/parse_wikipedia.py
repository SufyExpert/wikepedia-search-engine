import bz2
import xml.etree.ElementTree as ET
import os
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR    = os.path.join(os.path.dirname(__file__), "..", "data")
CHUNKS_DIR  = os.path.join(DATA_DIR, "enwiki_chunks")
TEMP_JSONL   = os.path.join(DATA_DIR, "temp_raw.jsonl")
OUTPUT_JSONL = os.path.join(DATA_DIR, "articles.jsonl")
MAX_ARTICLES = None

def get_chunk_files():
    """Retrieve compressed bz2 wikipedia chunk filepaths."""
    if not os.path.exists(CHUNKS_DIR): return []
    return sorted([os.path.join(CHUNKS_DIR, f) for f in os.listdir(CHUNKS_DIR) if f.endswith(".bz2")])

def parse_wikipedia_xml_to_file(filepath, writer, max_articles=None, already_collected=0):
    """Memory-safe extraction of raw articles from compressed Wikipedia XML chunks."""
    count = 0
    NAMESPACE = "{http://www.mediawiki.org/xml/export-0.11/}"
    remaining = (max_articles - already_collected) if max_articles else None

    with bz2.open(filepath, "rb") as f:
        context = ET.iterparse(f, events=("end",))
        for event, elem in context:
            if elem.tag == NAMESPACE + "page":
                ns = elem.find(NAMESPACE + "ns")
                if ns is None or ns.text != "0":
                    elem.clear(); continue

                title = elem.find(NAMESPACE + "title").text or ""
                text_elem = elem.find(f".//{NAMESPACE}text")
                text = text_elem.text if text_elem is not None else ""

                if not text or text.startswith("#REDIRECT"):
                    elem.clear(); continue

                article = {
                    "id": str(already_collected + count + 1),
                    "title": title.strip(),
                    "content": text
                }
                writer.write(json.dumps(article, ensure_ascii=False) + "\n")
                count += 1
                if count % 2000 == 0:
                    print(f"  Parsed {count} from chunk (Total: {already_collected + count})...")

                if remaining and count >= remaining:
                    elem.clear(); break
                elem.clear()
    return count

def process_with_spark(input_jsonl):
    """Execute parallel PySpark transformations for data cleansing and metrics generation."""
    print("\n" + "="*60)
    print("  Processing with Apache Spark (PySpark)")
    print("="*60)

    spark = SparkSession.builder \
        .appName("WikipediaSearchEngine") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.executor.memory", "3g") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    print(f"  Spark is reading from temporary file...")
    df = spark.read.json(input_jsonl)
    print(f"  Total raw articles loaded: {df.count()}")
    
    print("  Performing parallel cleaning transformations...")
    
    # regex matches running natively in parallel across JVM nodes
    df_clean = df.withColumn("content", F.regexp_replace(F.col("content"), r"(?s)<ref[^>]*>.*?</ref>", ""))
    df_clean = df_clean.withColumn("content", F.regexp_replace(F.col("content"), r"\{\{[^{}]*\}\}", ""))
    df_clean = df_clean.withColumn("content", F.regexp_replace(F.col("content"), r"\[\[(?:[^|\]]*\|)?([^\]]+)\]\]", r"$1"))
    df_clean = df_clean.withColumn("content", F.regexp_replace(F.col("content"), r"\s+", " "))
    df_clean = df_clean.withColumn("content", F.trim(F.col("content")))
    df_clean = df_clean.withColumn("content", F.substring(F.col("content"), 1, 5000))
    df_clean = df_clean.filter(F.length(F.col("content")) > 150)

    df_with_wc = df_clean.withColumn("word_count", F.size(F.split(F.col("content"), " ")))
    
    print("  Sample Articles:")
    df_with_wc.select("id", "title", "word_count").show(10, truncate=40)

    print("  Collecting results...")
    result = df_with_wc.select("id", "title", "content").collect()
    spark.stop()
    return result

if __name__ == "__main__":
    print("\n  Wikipedia PDC Project - Memory Safe Parser")
    
    chunk_files = get_chunk_files()
    if not chunk_files:
        print("  Error: No chunks found. Run download_dataset.py first."); sys.exit(1)

    print(f"  Phase 1: Extracting to {os.path.basename(TEMP_JSONL)}...")
    total_parsed = 0
    with open(TEMP_JSONL, "w", encoding="utf-8") as writer:
        for chunk in chunk_files:
            count = parse_wikipedia_xml_to_file(chunk, writer, MAX_ARTICLES, total_parsed)
            total_parsed += count
            if MAX_ARTICLES and total_parsed >= MAX_ARTICLES: break

    processed_data = process_with_spark(TEMP_JSONL)

    print(f"\n  Phase 3: Saving final {os.path.basename(OUTPUT_JSONL)}...")
    with open(OUTPUT_JSONL, "w", encoding="utf-8") as f:
        for row in processed_data:
            d = {"id": str(row["id"]), "title": row["title"], "content": row["content"]}
            f.write(json.dumps(d, ensure_ascii=False) + "\n")

    if os.path.exists(TEMP_JSONL): 
        os.remove(TEMP_JSONL)
    
    print("\n" + "="*60)
    print(f"  SUCCESS! Processed {len(processed_data):,} articles.")
    print("="*60)
