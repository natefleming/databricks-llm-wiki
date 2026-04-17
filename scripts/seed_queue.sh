#!/usr/bin/env bash
# Seed the compilation queue with any silver_content entries that don't
# yet have a corresponding compiled page.
#
# Run this after the SDP pipeline completes and before running the compile job.

set -euo pipefail

PROFILE="${PROFILE:-aws-field-eng}"
WAREHOUSE_ID="${WAREHOUSE_ID:-4b9b953939869799}"
CATALOG="${CATALOG:-nfleming}"
WIKI_SCHEMA="${WIKI_SCHEMA:-wiki_nate_fleming}"
RAW_SCHEMA="${RAW_SCHEMA:-raw_sources_nate_fleming}"

read -r -d '' SQL <<EOF || true
INSERT INTO ${CATALOG}.${WIKI_SCHEMA}.compilation_queue
SELECT uuid() as queue_id, slug as page_id, 'new_source' as trigger_type,
       array(source_id) as trigger_source_ids, 10 as priority, 'pending' as status,
       current_timestamp() as created_at, null as completed_at, '' as error_message
FROM ${CATALOG}.${RAW_SCHEMA}.silver_content sc
WHERE NOT EXISTS (
    SELECT 1 FROM ${CATALOG}.${WIKI_SCHEMA}.pages p WHERE p.page_id = sc.slug
)
  AND NOT EXISTS (
    SELECT 1 FROM ${CATALOG}.${WIKI_SCHEMA}.compilation_queue cq
    WHERE cq.page_id = sc.slug AND cq.status IN ('pending', 'in_progress')
)
EOF

PAYLOAD=$(python3 -c "import json,sys; print(json.dumps({'warehouse_id': '${WAREHOUSE_ID}', 'statement': sys.stdin.read(), 'wait_timeout': '30s'}))" <<< "$SQL")

databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json "$PAYLOAD" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print('Queue seed:', d.get('status',{}).get('state','?'))"

# Show current pending count
STATUS_SQL="SELECT status, count(*) as n FROM ${CATALOG}.${WIKI_SCHEMA}.compilation_queue GROUP BY status ORDER BY status"
STATUS_PAYLOAD=$(python3 -c "import json; print(json.dumps({'warehouse_id': '${WAREHOUSE_ID}', 'statement': '''${STATUS_SQL}''', 'wait_timeout': '30s'}))")

echo ""
echo "Current queue status:"
databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json "$STATUS_PAYLOAD" | \
    python3 -c "
import sys, json
rows = json.load(sys.stdin).get('result',{}).get('data_array',[])
for row in rows:
    print(f'  {row[0]:15s} {row[1]}')
"
