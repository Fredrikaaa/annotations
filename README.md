# Multithreaded annotator

Annotate documents with CoreNLP



## Prerequisites

*   Java
*   Maven
*   An SQLite database file (`.db`) containing a table named `documents`.

## Database Schema

The tool expects an SQLite database with a `documents` table structured as follows:

*   `document_id` (INTEGER, PRIMARY KEY): Unique identifier for the document.
*   `text` (TEXT): The raw text content of the document.
*   `timestamp` (TEXT, optional): Timestamp of the document (e.g., "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD"). This is used by CoreNLP's SUTime to resolve relative dates if present.

## Building the Tool

1.  Clone this repository 
2.  Navigate to the root directory of the project 
3.  Run the following Maven command to build the executable JAR:

    ```bash
    mvn package
    ```
    This will create a JAR file (e.g., `annotation-tool-1.0-SNAPSHOT.jar`) in the `target/` directory.

## Running the Tool

Execute the JAR file from your terminal, providing the path to your SQLite database.

**Basic Usage:**

```bash
java -jar target/annotation-tool-1.0-SNAPSHOT.jar --database-path /path/to/your/database.db
```

**Command-Line Options:**

*   `-db, --database-path <path>`: (Required) Path to the SQLite database file.
*   `-t, --threads <N>`: Number of parallel threads for CoreNLP processing. (Default: number of available processors).
    *   Example: `--threads 4`
*   `-b, --batch-size <N>`: Number of documents to commit per transaction during annotation. (Default: 1000).
    *   Example: `--batch-size 500`
*   `-l, --limit <N>`: Maximum number of new documents to process in this run (optional).
    *   Example: `--limit 10000`
*   `--force`: Force re-annotation from the beginning (document ID 1), overwriting existing annotations for processed documents.

**Example with more options:**

```bash
java -jar target/annotation-tool-1.0-SNAPSHOT.jar \
     --database-path /data/mydocs.db \
     --threads 8 \
     --batch-size 2000 \
     --limit 5000
```

## Output

The tool will create (if they don't exist) and populate two tables in your SQLite database:

*   `annotations`: Stores token-level annotations (lemma, POS, NER, etc.).
*   `dependencies`: Stores grammatical dependency relations between tokens.

## Logging

The tool uses SLF4J with a Logback backend for logging. Progress and errors will be printed to the console.
