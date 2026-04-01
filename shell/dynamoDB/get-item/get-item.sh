#!/usr/bin/env bash

# get-item.sh — query a DynamoDB table by partition key and unmarshal the result
#
# Prereqs: aws CLI configured; jq installed
#
# Provides two shell functions:
#   ddb_unmarshal   Pipe DynamoDB JSON (with type descriptors) through this to get plain JSON.
#   ddbq            Query a table by partition key; output is plain JSON via ddb_unmarshal.
#
# Usage:
#   source get-item.sh
#   ddbq --table <table> --pk <pk-key> --pk-value <value> \
#     [--profile <profile>] [--region <region>]
#
# Examples:
#   ddbq --table blocks --pk uuid --pk-value 123
#   ddbq --table blocks --pk uuid --pk-value 123 --profile prod --region eu-west-1
#
# Behavior:
# - Runs `aws dynamodb query` with the given partition key and pipes the result
#   through ddb_unmarshal to strip DynamoDB type wrappers (S, N, BOOL, M, L, etc.).
# - Defaults: profile=default, region=us-east-1.

# Ensure required commands exist
_require_cmd() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Missing required command: $1" >&2
    return 127
  }
}

# jq function for unmarshalling DynamoDB JSON to plain JSON
ddb_unmarshal() {
  _require_cmd jq || return $?

  local jq_core='
    def du:
      if type=="object" then
        if      has("S")    then .S
        elif    has("N")    then (.N|tonumber)
        elif    has("BOOL") then .BOOL
        elif    has("NULL") then null
        elif    has("M")    then (.M | with_entries(.value |= du))
        elif    has("L")    then (.L | map(du))
        elif    has("SS")   then .SS
        elif    has("NS")   then (.NS | map(tonumber))
        elif    has("BS")   then .BS
        else with_entries(.value |= du)
        end
      elif type=="array" then map(du)
      else . end;
    .Items | map(du)
  '

  jq "$jq_core"
}

_usage() {
  cat >&2 <<'USAGE'
ddbq - Query a DynamoDB table by partition key and return plain JSON

Required:
  --table NAME          DynamoDB table name
  --pk KEY              Partition key attribute name
  --pk-value VALUE      Partition key value (string)

Optional:
  --profile PROFILE     AWS profile (default: default)
  --region REGION       AWS region (default: us-east-1)
  -h, --help            Show this help

Examples:
  ddbq --table blocks --pk uuid --pk-value 123
  ddbq --table blocks --pk uuid --pk-value 123 --profile prod
  ddbq --table blocks --pk uuid --pk-value 123 --region eu-west-1
USAGE
}

# DynamoDB query wrapper
ddbq() {
  _require_cmd aws || return $?

  local table=""
  local pk_key=""
  local pk_value=""
  local profile="default"
  local region="us-east-1"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --table)
        table="$2"
        shift 2
        ;;
      --pk)
        pk_key="$2"
        shift 2
        ;;
      --pk-value)
        pk_value="$2"
        shift 2
        ;;
      --profile)
        profile="$2"
        shift 2
        ;;
      --region)
        region="$2"
        shift 2
        ;;
      -h|--help)
        _usage
        return 0
        ;;
      *)
        echo "Unknown argument: $1" >&2
        _usage
        return 1
        ;;
    esac
  done

  if [[ -z "$table" || -z "$pk_key" || -z "$pk_value" ]]; then
    echo "Error: --table, --pk, and --pk-value are required." >&2
    _usage
    return 1
  fi

  aws dynamodb query \
    --table-name "$table" \
    --key-condition-expression "$pk_key = :pk" \
    --expression-attribute-values "{\":pk\":{\"S\":\"$pk_value\"}}" \
    --output json \
    --profile "$profile" \
    --region "$region" \
    | ddb_unmarshal
}