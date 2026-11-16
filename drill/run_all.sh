#!/usr/bin/env bash
# 一键演练：启动本地 PostgreSQL（若未运行）→ 主演练 → SIGKILL 恢复演练
set -euo pipefail

ROOT=/workspace
PGBIN=$ROOT/pg/usr/lib/postgresql/15/bin
PGDATA=$ROOT/pgdata
export MIG_PYLIBS=$ROOT/pylibs/lib/python3.11/site-packages
export MIGRATE_DSN="host=127.0.0.1 port=55432 user=node dbname=postgres"
export LD_LIBRARY_PATH=$ROOT/pg/usr/lib/aarch64-linux-gnu

if ! "$PGBIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1; then
  "$PGBIN/pg_ctl" -D "$PGDATA" -l "$PGDATA/server.log" -w start
fi

echo "########## 主演练（plan/start/copy/status/cutover/abort + 交错 + 锁超时） ##########"
python3 -I "$ROOT/drill/drill.py"

echo "########## SIGKILL 强退恢复演练（1.8 万并发写、3.8 万捕获事件） ##########"
python3 -I "$ROOT/drill/kill_recovery.py"

echo "全部演练通过。证据：$ROOT/drill/logs/drill_evidence.txt"
