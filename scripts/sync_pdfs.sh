#!/usr/bin/env bash
# Sync PDFs from Azure (DEFAULT profile) to AWS (aws-field-eng profile)
#
# Flow:
#   1. Download PDFs from Azure: /Volumes/main/dbdemos_rag_chatbot/volume_databricks_documentation/databricks-pdf/
#   2. Upload to AWS: /Volumes/nfleming/raw_sources_nate_fleming/incoming/databricks-pdf/
#
# Usage:
#   ./scripts/sync_pdfs.sh            # sync from databricks-pdf/ (default)
#   ./scripts/sync_pdfs.sh landing    # sync from landing/ instead

set -euo pipefail

SOURCE_SUBDIR="${1:-databricks-pdf}"

AZURE_PROFILE="DEFAULT"
AZURE_PATH="dbfs:/Volumes/main/dbdemos_rag_chatbot/volume_databricks_documentation/${SOURCE_SUBDIR}"

AWS_PROFILE="aws-field-eng"
AWS_PATH="dbfs:/Volumes/nfleming/raw_sources_nate_fleming/incoming/databricks-pdf"

TMPDIR=$(mktemp -d -t llm-wiki-pdfs.XXXXXX)
trap 'rm -rf "$TMPDIR"' EXIT

echo "Azure source: $AZURE_PATH"
echo "AWS target:   $AWS_PATH"
echo "Staging dir:  $TMPDIR"
echo ""

echo "[1/2] Downloading PDFs from Azure..."
databricks fs cp "$AZURE_PATH/" "$TMPDIR/" --recursive --overwrite --profile "$AZURE_PROFILE"

# Filter out README markers - don't copy them to the ingestion target
find "$TMPDIR" -name "README.md" -delete

FILE_COUNT=$(find "$TMPDIR" -type f | wc -l | tr -d ' ')
echo "Downloaded $FILE_COUNT files to $TMPDIR"

if [ "$FILE_COUNT" -eq 0 ]; then
    echo "No files to sync. Exiting."
    exit 0
fi

echo ""
echo "[2/2] Uploading to AWS..."
databricks fs cp "$TMPDIR/" "$AWS_PATH/" --recursive --overwrite --profile "$AWS_PROFILE"

echo ""
echo "Sync complete. $FILE_COUNT files are now at:"
echo "  $AWS_PATH"
echo ""
echo "Next steps:"
echo "  1. Run the pipeline:         databricks bundle run llm_wiki_etl -t dev"
echo "  2. Seed the compile queue:   see scripts/seed_queue.sh"
echo "  3. Run the compile job:      databricks bundle run llm_wiki_compile -t dev"
