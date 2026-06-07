import subprocess
import sys
import os

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def run_script(script_path, description):
    """Execute python script as a subprocess with live output streaming."""
    print()
    print("=" * 60)
    print(f"  RUNNING: {description}")
    print(f"  Script:  {script_path}")
    print("=" * 60)

    result = subprocess.run(
        [sys.executable, script_path],
        cwd=ROOT,
        check=False
    )

    if result.returncode == 0:
        print(f"\n  ✓ {description} - COMPLETED SUCCESSFULLY")
    else:
        print(f"\n  ✗ {description} - FAILED (exit code {result.returncode})")
        sys.exit(1)

    return result.returncode

if __name__ == "__main__":
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   Wikipedia Full-Text Search Engine                      ║")
    print("║   PDC Final Exam Project - Complete Pipeline             ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()
    
    input("  Press ENTER to start the pipeline...")

    # Phase 1: Spark XML Parser
    p1 = os.path.join(ROOT, "data", "articles.jsonl")
    p2 = os.path.join(ROOT, "data", "articles.jsonll")
    fname = os.path.basename(p1) if os.path.exists(p1) else (os.path.basename(p2) if os.path.exists(p2) else None)
    
    if fname:
        print(f"\n  [INFO] data/{fname} already exists. Skipping parallel parsing stage.")
    else:
        run_script(
            os.path.join(ROOT, "spark", "parse_wikipedia.py"),
            "Step 1: Parse Wikipedia XML Chunks with PySpark"
        )

    # Phase 2: Elasticsearch Indexing
    run_script(
        os.path.join(ROOT, "elasticsearch", "index_elasticsearch.py"),
        "Step 2: Index parsed documents into Elasticsearch"
    )

    # Phase 3: Apache Solr Indexing
    run_script(
        os.path.join(ROOT, "solr", "index_solr.py"),
        "Step 3: Index parsed documents into Apache Solr"
    )

    # Done!
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║   ALL PIPELINE PIPES COMPLETED SUCCESSFULLY!             ║")
    print("║                                                          ║")
    print("║   Search Engine Active Endpoints:                        ║")
    print("║   - Flask Web App:   http://localhost:5000               ║")
    print("║   - Elasticsearch:   http://localhost:9200               ║")
    print("║   - Kibana Interface: http://localhost:5601               ║")
    print("║   - Solr Admin UI:   http://localhost:8983/solr/         ║")
    print("╚══════════════════════════════════════════════════════════╝")
