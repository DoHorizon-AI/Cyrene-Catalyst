#!/usr/bin/env bash
# Catalyst 数据整理 MVP · 完整操作演示
# 用法：bash examples/demo.sh
# 前置：uv 已安装；脚本会启动本地 ASGI 服务（127.0.0.1:8014）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

export CATALYST_HOME="${CATALYST_HOME:-$ROOT/.catalyst-demo}"
PORT="${CATALYST_PORT:-8014}"
BASE="http://127.0.0.1:${PORT}"

echo "» 启动 Catalyst 服务…"
uv run python -m cyrene_catalyst >/tmp/catalyst-demo.log 2>&1 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true' EXIT

for _ in $(seq 1 40); do
  if curl -sf "$BASE/" >/dev/null 2>&1; then break; fi
  sleep 0.5
done

echo "» 创建数据集"
DATASET_ID=$(curl -s -X POST "$BASE/api/v1/datasets" -H 'Content-Type: application/json' \
  -d '{"name":"demo-dataset","description":"脱敏样本演示"}' \
  | python -c 'import json,sys;print(json.load(sys.stdin)["id"])')
echo "  datasetId = $DATASET_ID"

echo "» 导入 JSONL 脱敏样本"
PREP_ID=$(curl -s -X POST "$BASE/api/v1/datasets/$DATASET_ID/preparations?name=demo-prep&filename=sample_instruction.jsonl" \
  --data-binary @"$ROOT/examples/sample_instruction.jsonl" \
  -H 'Content-Type: application/octet-stream' \
  | python -c 'import json,sys;print(json.load(sys.stdin)["id"])')
curl -s "$BASE/api/v1/preparations/$PREP_ID" | python -c 'import json,sys;d=json.load(sys.stdin);print("  format=" + d["format"] + " rows=" + str(d["rowCount"]) + " fields=" + str(d["detectedFields"]))'

echo "» 应用字段映射（speaker→instruction, text→output, groupBy scene）"
curl -s -X PATCH "$BASE/api/v1/preparations/$PREP_ID/mapping" -H 'Content-Type: application/json' \
  -d '{"mapping":{"mode":"instruction","instruction":{"field":"speaker"},"output":{"field":"text"},"groupBy":"scene"},"normalization":{"trimWhitespace":true,"collapseWhitespace":true,"unicodeNfc":true}}' \
  | python -c 'import json,sys;d=json.load(sys.stdin);r=d["report"];print("  valid=" + str(r["validSamples"]) + " unique=" + str(r["uniqueSamples"]) + " errors=" + str(r["errorSamples"]) + " dups=" + str(r["duplicateSamples"]) + " groups=" + str(r["groupCount"]))'

echo "» 应用划分（trainRatio=0.6）"
curl -s -X PATCH "$BASE/api/v1/preparations/$PREP_ID/split" -H 'Content-Type: application/json' \
  -d '{"split":{"trainRatio":0.6}}' \
  | python -c 'import json,sys;d=json.load(sys.stdin);s=d["splitStats"];print("  train=" + str(s["trainSamples"]) + "/" + str(s["trainGroups"]) + "组 val=" + str(s["valSamples"]) + "/" + str(s["valGroups"]) + "组")'

echo "» 人工确认"
curl -s -X POST "$BASE/api/v1/preparations/$PREP_ID/confirm" \
  | python -c 'import json,sys;print("  state =", json.load(sys.stdin)["state"])'

echo "» 发布 DatasetVersion"
curl -s -X POST "$BASE/api/v1/preparations/$PREP_ID/publish" -H 'Idempotency-Key: demo-publish' \
  | python -c 'import json,sys;d=json.load(sys.stdin);v=d["datasetVersion"];print("  versionId=" + str(v["id"]) + " v" + str(v["version"]) + " rows=" + str(v["rowCount"]) + " schema=" + str(v["schemaFields"]))'

echo "» 导出文件清单"
curl -s "$BASE/api/v1/preparations/$PREP_ID/exports" | python -c 'import json,sys
for e in json.load(sys.stdin): print("  " + e["name"] + " " + str(e["rowCount"]) + " rows")'

echo "» 下载 train.jsonl 并用 DuckDB 真实读取校验"
curl -s "$BASE/api/v1/preparations/$PREP_ID/exports/train.jsonl" -o /tmp/catalyst-train.jsonl
uv run python -c 'import duckdb;con=duckdb.connect();rel=con.read_json("/tmp/catalyst-train.jsonl",format="newline_delimited");rows=rel.fetchall();print("  DuckDB columns:", rel.columns, "rows:", len(rows));print("  示例行:", rows[0])'

echo "» 发送到 Yield（应明确显示 NOT_CONNECTED）"
curl -s -X POST "$BASE/api/v1/preparations/$PREP_ID/yield-draft" \
  | python -c 'import json,sys;d=json.load(sys.stdin);print("  status=" + d["status"]);print("  detail=" + d["detail"])'

echo "» 完成。UI 可在浏览器访问 $BASE/"
