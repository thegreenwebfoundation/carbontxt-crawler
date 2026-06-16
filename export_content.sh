#!/bin/bash

# Exit immediately if a command exits with a non-zero status,
# if an undefined variable is referenced, or if a piped command fails.
set -euo pipefail

# Configuration with defaults
OS_URL="${1:-http://localhost:9200/content/}"
OUTPUT_FILE="${2:-carbontxt.ndjson}"
SCROLL_TIMEOUT="1m"
BATCH_SIZE=1000

# Ensure jq is installed
if ! command -v jq &> /dev/null; then
    echo "Error: jq is required but not installed." >&2
    exit 1
fi

# Ensure curl is installed
if ! command -v curl &> /dev/null; then
    echo "Error: curl is required but not installed." >&2
    exit 1
fi

# Clean up trailing slash and parse URL parts
OS_URL="${OS_URL%/}"
BASE_URL="${OS_URL%/*}"
INDEX_NAME="${OS_URL##*/}"

echo "Connecting to OpenSearch / Elasticsearch at $BASE_URL..."
echo "Exporting index '$INDEX_NAME' to '$OUTPUT_FILE'..."

# Truncate or create the output file as empty
> "$OUTPUT_FILE"

# 1. Initialize the scroll query
INITIAL_RESPONSE=$(curl -s -X POST "$BASE_URL/$INDEX_NAME/_search?scroll=$SCROLL_TIMEOUT" \
    -H 'Content-Type: application/json' \
    -d "{
      \"size\": $BATCH_SIZE,
      \"query\": {
        \"match_all\": {}
      }
    }")

# Check if response contains an error
if echo "$INITIAL_RESPONSE" | jq -e '.error' &> /dev/null; then
    ERROR_MSG=$(echo "$INITIAL_RESPONSE" | jq -r '.error.reason // "Unknown error"')
    echo "Error from server: $ERROR_MSG" >&2
    exit 1
fi

# Extract Scroll ID
SCROLL_ID=$(echo "$INITIAL_RESPONSE" | jq -r '._scroll_id // empty')

if [ -z "$SCROLL_ID" ]; then
    echo "Error: Failed to retrieve scroll_id. Is the index empty or missing?" >&2
    exit 1
fi

# Extract and write the first batch of documents
echo "$INITIAL_RESPONSE" | jq -c '.hits.hits[]._source' >> "$OUTPUT_FILE"
COUNT=$(echo "$INITIAL_RESPONSE" | jq '.hits.hits | length')
TOTAL_COUNT=$COUNT

echo "Retrieved $COUNT documents..."

# 2. Scroll loop to get all subsequent documents
while true; do
    SCROLL_RESPONSE=$(curl -s -X POST "$BASE_URL/_search/scroll" \
        -H 'Content-Type: application/json' \
        -d "{
          \"scroll\": \"$SCROLL_TIMEOUT\",
          \"scroll_id\": \"$SCROLL_ID\"
        }")

    # Check for empty hits
    COUNT=$(echo "$SCROLL_RESPONSE" | jq '.hits.hits | length')
    if [ "$COUNT" -eq 0 ]; then
        break
    fi

    # Append to output file
    echo "$SCROLL_RESPONSE" | jq -c '.hits.hits[]._source' >> "$OUTPUT_FILE"
    TOTAL_COUNT=$((TOTAL_COUNT + COUNT))
    echo "Retrieved $TOTAL_COUNT documents in total..."

    # Extract new Scroll ID (scroll_id can occasionally change)
    NEW_SCROLL_ID=$(echo "$SCROLL_RESPONSE" | jq -r '._scroll_id // empty')
    if [ ! -z "$NEW_SCROLL_ID" ]; then
        SCROLL_ID="$NEW_SCROLL_ID"
    fi
done

# 3. Clear the scroll context from the server
if [ ! -z "$SCROLL_ID" ]; then
    curl -s -X DELETE "$BASE_URL/_search/scroll" \
        -H 'Content-Type: application/json' \
        -d "{
          \"scroll_id\": [\"$SCROLL_ID\"]
        }" > /dev/null || true
fi

echo "Success! Exported $TOTAL_COUNT documents to $OUTPUT_FILE"
