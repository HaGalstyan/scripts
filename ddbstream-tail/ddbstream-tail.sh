#!/usr/bin/env bash
set -euo pipefail

# dynastream-tail.sh — tail a DynamoDB Stream from the shell (Bash 3+ & zsh-friendly)
#
# Prereqs: aws CLI configured; jq installed
#
# Usage:
#   ./dynastream-tail.sh -t MyTable \
#     [--iterator LATEST|TRIM_HORIZON|AT_SEQUENCE_NUMBER|AFTER_SEQUENCE_NUMBER] \
#     [--sequence-number <seq>] \
#     [--region us-east-1] [--profile myprof] \
#     [--limit 1000] [--interval 1.0] \
#     [--out stream.log] \
#     [--range-start YYYY/MM/DD/hh:mm] [--range-end YYYY/MM/DD/hh:mm]
#
# Behavior:
# - If BOTH --range-start and --range-end are omitted, shard date filtering is skipped entirely.
# - If either bound is provided, we probe shards to determine overlap (efficient early-stopping).
# - Logs: total shard count, and per-shard logs prefixed with [i/N][shardId-...].

TABLE_NAME=""
STREAM_ARN=""
ITERATOR_TYPE="LATEST"
START_SEQUENCE_NUMBER=""
REGION=""
PROFILE=""
LIMIT="1000"
INTERVAL="1.0" # seconds when idle
DATE_FMT="+%Y-%m-%dT%H:%M:%S%z"

# --- Output file (optional) ---
OUTFILE=""         # set via --out
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]:-${0}}")" && pwd)"
LOCKDIR=""

# Range filtering (optional)
RANGE_START=""
RANGE_END=""
RANGE_START_EPOCH=""
RANGE_END_EPOCH=""

# String filtering (optional)
CONTAINS_FILTER=""

log() { echo "[$(date "$DATE_FMT")] $*" >&2; }

usage() {
  cat <<'USAGE'
dynastream-tail.sh - Tail a DynamoDB Stream from the shell

Required:
  -t, --table NAME            Table name (unless --stream-arn is provided)

Optional:
      --stream-arn ARN        Stream ARN (skip table lookup)
      --iterator TYPE         LATEST | TRIM_HORIZON | AT_SEQUENCE_NUMBER | AFTER_SEQUENCE_NUMBER (default LATEST)
      --sequence-number SEQ   Required for AT_/AFTER_SEQUENCE_NUMBER
      --region REGION
      --profile PROFILE
      --limit N               get-records limit (1..1000, default 1000)
      --interval SECONDS      sleep when idle (default 1.0)
      --out FILE              Append records to FILE (relative path is created beside this script)
      --range-start YYYY/MM/DD/hh:mm  Only read shards that might overlap this time window
      --range-end   YYYY/MM/DD/hh:mm
      --contains STRING        Only emit records that contain the specified string

Examples:
  ./dynastream-tail.sh -t MyTable --iterator LATEST
  ./dynastream-tail.sh -t MyTable --iterator TRIM_HORIZON
  ./dynastream-tail.sh --stream-arn arn:aws:dynamodb:... --iterator AFTER_SEQUENCE_NUMBER --sequence-number 111...
  ./dynastream-tail.sh -t MyTable --iterator TRIM_HORIZON --range-start 2025/10/09/10:00 --range-end 2025/10/09/17:30
  ./dynastream-tail.sh -t MyTable --iterator LATEST --contains "specific-string"
USAGE
  exit 1
}

# --- Arg parsing ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    -t|--table) TABLE_NAME="$2"; shift 2;;
    --stream-arn) STREAM_ARN="$2"; shift 2;;
    --iterator) ITERATOR_TYPE="$2"; shift 2;;
    --sequence-number) START_SEQUENCE_NUMBER="$2"; shift 2;;
    --region) REGION="$2"; shift 2;;
    --profile) PROFILE="$2"; shift 2;;
    --limit) LIMIT="$2"; shift 2;;
    --interval) INTERVAL="$2"; shift 2;;
    --out) OUTFILE="$2"; shift 2;;
    --range-start) RANGE_START="$2"; shift 2;;
    --range-end) RANGE_END="$2"; shift 2;;
    --contains) CONTAINS_FILTER="$2"; shift 2;;
    -h|--help) usage;;
    *) log "Unknown arg: $1"; usage;;
  esac
done

# Validate LIMIT
if ! [[ "$LIMIT" =~ ^[0-9]+$ ]] || (( LIMIT < 1 || LIMIT > 1000 )); then
  log "--limit must be an integer between 1 and 1000 (got '$LIMIT')"
  exit 2
fi

aws_args=()
[[ -n "$REGION"  ]] && aws_args+=(--region "$REGION")
[[ -n "$PROFILE" ]] && aws_args+=(--profile "$PROFILE")

# Resolve output file (append mode; create if missing)
emit() {
  echo "$*"
  if [[ -n "${OUTFILE:-}" ]]; then
    while ! mkdir "$LOCKDIR" 2>/dev/null; do sleep 0.01; done
    printf "%s\n" "$*" >> "$OUTFILE"
    rmdir "$LOCKDIR"
  fi
}
emit_raw() {
  printf "%s" "$*"
  if [[ -n "${OUTFILE:-}" ]]; then
    while ! mkdir "$LOCKDIR" 2>/dev/null; do sleep 0.01; done
    printf "%s" "$*" >> "$OUTFILE"
    rmdir "$LOCKDIR"
  fi
}

if [[ -n "$OUTFILE" ]]; then
  if [[ "${OUTFILE:0:1}" != "/" ]]; then
    OUTFILE="$SCRIPT_DIR/$OUTFILE"
  fi
  mkdir -p "$(dirname "$OUTFILE")"
  touch "$OUTFILE"
  LOCKDIR="${OUTFILE}.lock"
  log "Appending records to: $OUTFILE"
fi

# Get Stream ARN if not provided
if [[ -z "$STREAM_ARN" ]]; then
  [[ -z "$TABLE_NAME" ]] && { log "Provide --table or --stream-arn"; usage; }
  log "Fetching Stream ARN for table: $TABLE_NAME"
  STREAM_ARN=$(aws dynamodb describe-table \
    --table-name "$TABLE_NAME" "${aws_args[@]}" \
    --query "Table.LatestStreamArn" --output text 2>/dev/null || true)

  if [[ -z "$STREAM_ARN" || "$STREAM_ARN" == "None" ]]; then
    log "Streams not enabled on table '$TABLE_NAME' or no latest stream ARN."
    exit 2
  fi
fi

log "Using Stream ARN: $STREAM_ARN"
log "Iterator type: $ITERATOR_TYPE${START_SEQUENCE_NUMBER:+ (seq=$START_SEQUENCE_NUMBER)}"
log "Poll limit: $LIMIT; idle interval: ${INTERVAL}s"
[[ -n "$CONTAINS_FILTER" ]] && log "String filter: --contains '$CONTAINS_FILTER'"

# --- Date helpers ---
# Parse YYYY/MM/DD/hh:mm -> epoch seconds. Works on GNU date, gdate, or BSD date (macOS).
to_epoch() {
  local s="$1"
  if [[ ! "$s" =~ ^([0-9]{4})/([0-9]{2})/([0-9]{2})/([0-9]{2}):([0-9]{2})$ ]]; then
    return 2
  fi
  local Y=${BASH_REMATCH[1]} M=${BASH_REMATCH[2]} D=${BASH_REMATCH[3]}
  local h=${BASH_REMATCH[4]} m=${BASH_REMATCH[5]}
  local iso="${Y}-${M}-${D} ${h}:${m}"

  if command -v gdate >/dev/null 2>&1; then
    gdate -d "$iso" +%s 2>/dev/null || return 1
  elif date -d "1970-01-01" +%s >/dev/null 2>&1; then
    date -d "$iso" +%s 2>/dev/null || return 1
  else
    date -j -f "%Y-%m-%d %H:%M" "$iso" "+%s" 2>/dev/null || return 1
  fi
}

epoch_to_str() {
  local e="$1"
  if command -v gdate >/dev/null 2>&1; then
    gdate -d "@$e" "+%F %T" 2>/dev/null || echo "$e"
  elif date -d "@0" +%s >/dev/null 2>&1; then
    date -d "@$e" "+%F %T" 2>/dev/null || echo "$e"
  else
    date -r "$e" "+%F %T" 2>/dev/null || echo "$e"
  fi
}

# --- Validate and convert date filters (YYYY/MM/DD/hh:mm) ---
if [[ -n "$RANGE_START" ]]; then
  if ! RANGE_START_EPOCH=$(to_epoch "$RANGE_START"); then
    log "Invalid --range-start format. Use YYYY/MM/DD/hh:mm (e.g. 2025/09/30/13:45)"
    exit 2
  fi
fi

if [[ -n "$RANGE_END" ]]; then
  if ! RANGE_END_EPOCH=$(to_epoch "$RANGE_END"); then
    log "Invalid --range-end format. Use YYYY/MM/DD/hh:mm (e.g. 2025/09/30/14:00)"
    exit 2
  fi
fi

# If both provided, ensure start <= end (auto-swap if reversed)
if [[ -n "$RANGE_START_EPOCH" && -n "$RANGE_END_EPOCH" ]]; then
  if (( RANGE_END_EPOCH < RANGE_START_EPOCH )); then
    log "Warning: --range-end is before --range-start; swapping the two."
    tmp="$RANGE_START_EPOCH"; RANGE_START_EPOCH="$RANGE_END_EPOCH"; RANGE_END_EPOCH="$tmp"
    tmp="$RANGE_START"; RANGE_START="$RANGE_END"; RANGE_END="$tmp"
  fi
fi

# --- Get a shard iterator (trim CR/LF) ---
get_iterator() {
  local shard_id="$1"
  local iterator_type="$2"
  local seq_num="${3:-}"
  local it=""

  if [[ "$iterator_type" == "AT_SEQUENCE_NUMBER" || "$iterator_type" == "AFTER_SEQUENCE_NUMBER" ]]; then
    if [[ -z "$seq_num" ]]; then
      log "[$shard_id] Iterator $iterator_type requires a sequence number"
      return 1
    fi
    it=$(aws dynamodbstreams get-shard-iterator \
          --stream-arn "$STREAM_ARN" \
          --shard-id "$shard_id" \
          --shard-iterator-type "$iterator_type" \
          --sequence-number "$seq_num" \
          "${aws_args[@]}" \
          --query "ShardIterator" --output text 2>/dev/null || true)
  else
    it=$(aws dynamodbstreams get-shard-iterator \
          --stream-arn "$STREAM_ARN" \
          --shard-id "$shard_id" \
          --shard-iterator-type "$iterator_type" \
          "${aws_args[@]}" \
          --query "ShardIterator" --output text 2>/dev/null || true)
  fi

  # Trim CR/LF
  it="${it//$'\r'/}"
  it="${it//$'\n'/}"

  printf '%s' "$it"
}

# --- Pretty-print one record ---
print_record() {
  local shard_id="$1"
  local rec="$2"
  
  # If --contains filter is provided, check if record contains the string
  if [[ -n "$CONTAINS_FILTER" ]]; then
    if echo "$rec" | grep -q "$CONTAINS_FILTER"; then
      emit "$rec"
    fi
  else
    # If no --contains filter, emit every record
    emit "$rec"
  fi
}

# --- Reader loop for a single shard (hardened) ---
read_shard() {
  local shard_id="$1"
  local iterator_type="$2"
  local last_seq="${3:-}"
  local shard_index="${4:-?}"   # for logging [i/N]
  local total_shards="${5:-?}"  # for logging [i/N]
  local prefix="[$shard_index/$total_shards][$shard_id]"

  local iter=""
  local next_iter=""
  local out=""
  local records=""
  local count=0
  local first_ts="" last_ts=""
  local backoff="${INTERVAL}"

  log "$prefix starting reader…"

  get_iter() {
    if [[ -n "$last_seq" ]]; then
      get_iterator "$shard_id" "AFTER_SEQUENCE_NUMBER" "$last_seq"
    else
      get_iterator "$shard_id" "$iterator_type"
    fi
  }

  iter="$(get_iter || echo "")"
  if [[ -z "$iter" || "$iter" == "None" ]]; then
    log "$prefix no valid iterator at start; skipping shard."
    return 0
  fi
  [[ -n "$iter" && "$iter" != "None" ]] && log "$prefix iterator_acquired prefix=$(printf '%s' "$iter" | cut -c1-12)... len=${#iter}"

  while :; do
    # Guard: never call get-records with empty iterator
    if [[ -z "$iter" || "$iter" == "None" ]]; then
      log "$prefix iterator missing before get-records; refreshing…"
      iter="$(get_iter || echo "")"
      if [[ -z "$iter" || "$iter" == "None" ]]; then
        log "$prefix cannot refresh iterator; exiting shard."
        break
      fi
    fi

    if ! out=$(aws dynamodbstreams get-records \
        --shard-iterator "$iter" \
        --limit "$LIMIT" \
        "${aws_args[@]}" 2>&1); then

      if echo "$out" | grep -qi 'Parameter validation failed'; then
        log "$prefix get-records parameter error (iter_len=${#iter}). Refreshing iterator…"
        iter="$(get_iter || echo "")"
        if [[ -z "$iter" || "$iter" == "None" ]]; then
          log "$prefix still no iterator after refresh; exiting shard."
          break
        fi
        continue
      fi

      if echo "$out" | grep -qiE 'ExpiredIteratorException|TrimmedDataAccessException|InvalidArgument'; then
        log "$prefix iterator expired/invalid; refreshing…"
        iter="$(get_iter || echo "")"
        [[ -z "$iter" || "$iter" == "None" ]] && { log "$prefix refresh failed; exiting."; break; }
        continue
      fi

      log "$prefix get-records error: $out"
      sleep "$backoff"
      continue
    fi

    records=$(echo "$out" | jq '.Records')
    next_iter=$(echo "$out" | jq -r '.NextShardIterator // empty')
    count=$(echo "$records" | jq 'length')

    if [[ "$count" -gt 0 ]]; then
      backoff="${INTERVAL}" # reset backoff when we see data
      echo "$records" | jq -c '.[]' | while read -r rec; do
        print_record "$shard_id" "$rec"
        last_seq=$(echo "$rec" | jq -r '.dynamodb.SequenceNumber')
        ts=$(echo "$rec" | jq -r '.dynamodb.ApproximateCreationDateTime // empty')
        [[ -z "$first_ts" && -n "$ts" ]] && first_ts="$ts"
        [[ -n "$ts" ]] && last_ts="$ts"
      done
    else
      # be polite; light backoff up to 5x interval
      sleep "$backoff"
      backoff=$(awk -v b="$backoff" -v i="$INTERVAL" 'BEGIN{b+=i; if(b>5*i)b=5*i; print b}')
    fi

    if [[ -z "$next_iter" || "$next_iter" == "null" ]]; then
      if [[ "$count" -eq 0 ]]; then
        log "$prefix No NextShardIterator (likely closed and fully read). Exiting."
      fi
      break
    fi

    iter="$next_iter"
  done

  log "$prefix range: first_ts=${first_ts:-unknown} last_ts=${last_ts:-unknown}"
  log "$prefix reader finished."
}

# --- Shard listing & optional range filtering via probes (zsh-friendly) ---
log "Listing shards…"

# Collect all shard objects, handling pagination
ALL_SHARDS=()
START_SHARD_ID=""

while :; do
  if [[ -n "$START_SHARD_ID" ]]; then
    PAGE=$(aws dynamodbstreams describe-stream \
      --stream-arn "$STREAM_ARN" \
      --exclusive-start-shard-id "$START_SHARD_ID" \
      "${aws_args[@]}" --output json)
  else
    PAGE=$(aws dynamodbstreams describe-stream \
      --stream-arn "$STREAM_ARN" \
      "${aws_args[@]}" --output json)
  fi

  # Append each shard JSON object into ALL_SHARDS[]
  while IFS= read -r shard_json; do
    [[ -n "$shard_json" ]] && ALL_SHARDS+=("$shard_json")
  done < <(echo "$PAGE" | jq -c '.StreamDescription.Shards[]?')

  # Pagination
  next=$(echo "$PAGE" | jq -r '.StreamDescription.LastEvaluatedShardId // empty')
  if [[ -z "$next" ]]; then
    break
  else
    START_SHARD_ID="$next"
  fi
done

TOTAL_SHARDS=${#ALL_SHARDS[@]}
log "Detected $TOTAL_SHARDS shard(s) from describe-stream."

# Helpers to probe timestamps from real records
probe_first_ts_epoch() {
  local shard_id="$1"
  local it out ts
  it=$(aws dynamodbstreams get-shard-iterator \
        --stream-arn "$STREAM_ARN" \
        --shard-id "$shard_id" \
        --shard-iterator-type TRIM_HORIZON \
        "${aws_args[@]}" \
        --query ShardIterator --output text 2>/dev/null || true)
  if [[ -z "$it" || "$it" == "None" ]]; then
    echo ""; return 0
  fi
  out=$(aws dynamodbstreams get-records --shard-iterator "$it" --limit 1 "${aws_args[@]}" 2>/dev/null || true)
  ts=$(echo "$out" | jq -r '.Records[0].dynamodb.ApproximateCreationDateTime // empty')
  [[ -z "$ts" || "$ts" == "null" ]] && { echo ""; return 0; }

  if command -v gdate >/dev/null 2>&1; then gdate -d "$ts" +%s 2>/dev/null || echo ""
  elif date -d "1970-01-01" +%s >/dev/null 2>&1; then date -d "$ts" +%s 2>/dev/null || echo ""
  else date -j -f "%Y-%m-%dT%H:%M:%S%z" "$ts" "+%s" 2>/dev/null || echo ""; fi
}

# For closed shards; page forward with early stop once we cross RANGE_START
probe_last_ts_epoch_fast() {
  local shard_id="$1" max_pages="${2:-200}"
  local it out ts last_ts="" pages=0

  it=$(aws dynamodbstreams get-shard-iterator \
        --stream-arn "$STREAM_ARN" \
        --shard-id "$shard_id" \
        --shard-iterator-type TRIM_HORIZON \
        "${aws_args[@]}" \
        --query ShardIterator --output text 2>/dev/null || true)
  if [[ -z "$it" || "$it" == "None" ]]; then
    echo ""; return 0
  fi

  while [[ -n "$it" && "$it" != "null" && $pages -lt $max_pages ]]; do
    out=$(aws dynamodbstreams get-records --shard-iterator "$it" --limit 1000 "${aws_args[@]}" 2>/dev/null || true)
    it=$(echo "$out" | jq -r '.NextShardIterator // empty')

    ts=$(echo "$out" | jq -r '.Records[-1].dynamodb.ApproximateCreationDateTime // empty')
    if [[ -n "$ts" && "$ts" != "null" ]]; then
      if command -v gdate >/dev/null 2>&1; then last_ts=$(gdate -d "$ts" +%s 2>/dev/null || echo "")
      elif date -d "1970-01-01" +%s >/dev/null 2>&1; then last_ts=$(date -d "$ts" +%s 2>/dev/null || echo "")
      else last_ts=$(date -j -f "%Y-%m-%dT%H:%M:%S%z" "$ts" "+%s" 2>/dev/null || echo ""); fi
    fi

    # Early stop once we've reached/passed RANGE_START
    if [[ -n "${RANGE_START_EPOCH:-}" && -n "$last_ts" && $last_ts -ge $RANGE_START_EPOCH ]]; then
      break
    fi

    ((pages++))
    [[ "$(echo "$out" | jq '(.Records|length)')" -eq 0 ]] && sleep "${INTERVAL}"
  done

  echo "${last_ts:-}"
}

# --- If both range bounds are missing, skip filtering entirely ---
if [[ -z "${RANGE_START_EPOCH:-}" && -z "${RANGE_END_EPOCH:-}" ]]; then
  log "No --range-start/--range-end provided; skipping date-range checks and including all shards."
  SHARDS=()
  INDEX=0
  for shard_json in "${ALL_SHARDS[@]}"; do
    ((INDEX++))
    shard_id=$(echo "$shard_json" | jq -r '.ShardId')
    log "[$INDEX/$TOTAL_SHARDS][$shard_id] included (no range filtering)."
    SHARDS+=("$shard_id")
  done
else
  # --- Range filtering path (one-sided checks supported) ---
  FILTERED_SHARDS=()
  INDEX=0
  for shard_json in "${ALL_SHARDS[@]}"; do
    ((INDEX++))
    shard_id=$(echo "$shard_json" | jq -r '.ShardId')
    prefix="[$INDEX/$TOTAL_SHARDS][$shard_id]"

    end_seq=$(echo "$shard_json" | jq -r '.SequenceNumberRange.EndingSequenceNumber // empty')
    is_closed=false; [[ -n "$end_seq" ]] && is_closed=true

    first_epoch=$(probe_first_ts_epoch "$shard_id")
    if [[ -z "$first_epoch" ]]; then
      log "$prefix no readable records at TRIM_HORIZON; skipping."
      continue
    fi

    # If only RANGE_END_EPOCH is set, this check still works
    if [[ -n "${RANGE_END_EPOCH:-}" && $first_epoch -gt $RANGE_END_EPOCH ]]; then
      log "$prefix starts at $(epoch_to_str "$first_epoch") > range_end; skipping."
      continue
    fi

    if [[ "$is_closed" == true ]]; then
      last_epoch=$(probe_last_ts_epoch_fast "$shard_id" 200)
      if [[ -z "$last_epoch" ]]; then
        # Conservative include when uncertain
        log "$prefix closed; last timestamp unknown -> INCLUDING conservatively."
        FILTERED_SHARDS+=("$shard_id")
        continue
      fi

      # Overlap if: (no start bound OR last >= start) AND (no end bound OR first <= end)
      start_ok=true
      end_ok=true
      [[ -n "${RANGE_START_EPOCH:-}" && $last_epoch  -lt $RANGE_START_EPOCH ]] && start_ok=false
      [[ -n "${RANGE_END_EPOCH:-}"   && $first_epoch -gt $RANGE_END_EPOCH   ]] && end_ok=false

      if [[ "$start_ok" == true && "$end_ok" == true ]]; then
        FILTERED_SHARDS+=("$shard_id")
        log "$prefix overlaps; first=$(epoch_to_str "$first_epoch") last=$(epoch_to_str "$last_epoch")"
      else
        log "$prefix no overlap; first=$(epoch_to_str "$first_epoch") last=$(epoch_to_str "$last_epoch")"
      fi
    else
      # Open shard: if it started before range_end, it *might* overlap now → include
      FILTERED_SHARDS+=("$shard_id")
      log "$prefix open shard; first=$(epoch_to_str "$first_epoch") → INCLUDING."
    fi
  done

  if [[ ${#FILTERED_SHARDS[@]} -eq 0 ]]; then
    log "No shards overlap with requested range."
    exit 0
  fi

  log "Found ${#FILTERED_SHARDS[@]} shard(s) in requested range:"
  for s in "${FILTERED_SHARDS[@]}"; do
    echo " - $s"
  done

  SHARDS=("${FILTERED_SHARDS[@]}")
fi

# --- Trap to clean up children on Ctrl+C ---
pids=()
cleanup() {
  log "Stopping shard readers…"
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait || true
  log "Done."
}
trap cleanup INT TERM

# --- Start a reader per shard (in parallel) ---
INDEX=0
TOTAL_TO_READ=${#SHARDS[@]}
for shard in "${SHARDS[@]}"; do
  ((INDEX++))
  read_shard "$shard" "$ITERATOR_TYPE" "$START_SEQUENCE_NUMBER" "$INDEX" "$TOTAL_TO_READ" &
  pids+=("$!")
done

wait
