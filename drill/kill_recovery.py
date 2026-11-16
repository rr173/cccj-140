#!/usr/bin/env python3
"""SIGKILL 强退恢复演练：复制进行中杀掉 -9 进程，重开后必须一致收敛。

不修改产品代码；通过在另一连接制造持续写入压力 + 小批次 + SIGKILL 实现。
"""
import os
import signal
import subprocess
import sys
import time
from decimal import Decimal

sys.path.insert(0, "/workspace/pylibs/lib/python3.11/site-packages")
import psycopg

DSN = "host=127.0.0.1 port=55432 user=node dbname=drillkill"
TOOL = "/workspace/tool/migrate.py"
CFG = "/workspace/tool/config.json"
ENV = {**os.environ,
       "MIGRATE_DSN": DSN,
       "MIG_PYLIBS": "/workspace/pylibs/lib/python3.11/site-packages",
       "MIG_MAX_BATCHES": "100000"}  # 让它一直跑，由我们杀


def main():
    admin = psycopg.connect(
        "host=127.0.0.1 port=55432 user=node dbname=postgres", autocommit=True)
    admin.execute("DROP DATABASE IF EXISTS drillkill")
    admin.execute("CREATE DATABASE drillkill")
    admin.close()
    c = psycopg.connect(DSN, autocommit=True)
    c.execute("CREATE TABLE public.fact (row_id bigint PRIMARY KEY, "
              "amount_text text, location_text text, note text)")
    c.execute("CREATE VIEW public.v_fact_ro AS SELECT row_id, amount_text, "
              "location_text, note FROM public.fact")
    with c.transaction():
        c.cursor().executemany(
            "INSERT INTO fact VALUES (%s,%s,%s,%s)",
            [(i, f"{i}.00", f"r{i % 5}/s{i}", f"n{i}") for i in range(1, 201)])

    def tool(cmd):
        return subprocess.run([sys.executable, "-I", TOOL, cmd,
                               "--config", CFG], env=ENV,
                              capture_output=True, text=True)

    tool("start")
    # 后台启动 copy（小批次，跑久一点），并发写入线程制造交错
    logf = open("/tmp/kill_copy.log", "w")
    proc = subprocess.Popen([sys.executable, "-I", TOOL, "copy",
                             "--config", CFG], env=ENV,
                            stdout=logf, stderr=subprocess.STDOUT,
                            text=True)
    writer = psycopg.connect(DSN, autocommit=True)
    # 写入压力：持续 insert/update/delete，直到 copy 进程被杀
    nxt = 1000
    deadline = time.monotonic() + 6
    ops = 0
    while time.monotonic() < deadline and proc.poll() is None:
        # 每行唯一 (region,site)，不制造人为唯一冲突；压力来自纯交错
        writer.execute("INSERT INTO fact VALUES (%s,%s,%s,%s) ON CONFLICT "
                       "(row_id) DO UPDATE SET amount_text=EXCLUDED.amount_text",
                       (nxt, f"{nxt}.55", f"w{nxt}/z{nxt}", "pressure"))
        nxt += 1
        writer.execute("UPDATE fact SET note=%s WHERE row_id=%s",
                       (f"bump-{ops}", 1 + (ops % 150)))
        if ops % 37 == 0:
            writer.execute("DELETE FROM fact WHERE row_id=%s", (500 + ops,))
        ops += 1
    writer.close()
    # 强杀 -9（模拟崩溃，无 Python 清理、无正常解锁）
    if proc.poll() is None:
        proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=30)
    logf.close()
    print(f"[kill] copy 进程被 SIGKILL，rc={proc.returncode}，期间业务写操作 {ops} 次")
    print("[kill] 被杀进程最后日志: " + open("/tmp/kill_copy.log").read().strip()
          .replace("\n", " | "))
    assert proc.returncode and proc.returncode < 0

    # 重开新进程继续；advisory lock 由 PostgreSQL 在后端退出时自动释放
    r = tool("copy")
    print(r.stdout.strip())
    assert r.returncode == 0, r.stderr
    st = c.execute("SELECT state FROM shadow_mig.registry").fetchone()[0]
    assert st == "READY", st
    r = tool("status")
    print(r.stdout.strip())
    assert "行级错误=0" in r.stdout
    # 崩溃前后数据一致性：源与影子逐行全量比对（含并发写入结果）
    src = dict((r[0], r[1:]) for r in c.execute(
        "SELECT row_id, amount_text, location_text, note FROM fact ORDER BY 1"))
    ms = c.execute("SELECT mig_schema FROM shadow_mig.registry").fetchone()[0]
    sh = dict((r[0], r[1:]) for r in c.execute(
        f"SELECT row_id, amount, region, site, note FROM {ms}.shadow ORDER BY 1"))
    assert set(src) == set(sh), (len(src), len(sh))
    for rid, (amt, loc, note) in src.items():
        expect_amt = Decimal(amt).quantize(Decimal("0.01")) if amt else None
        region, site = (loc.split("/", 1) if loc else (None, None))
        got_amt, got_r, got_s, got_note = sh[rid]
        assert (got_amt, got_r, got_s, got_note) == \
               (expect_amt, region, site, note), rid
    print(f"[kill] 恢复后源/影子 {len(src)} 行逐行一致，"
          f"水位与流水位点：{c.execute(f'SELECT k,v FROM {ms}.meta ORDER BY 1').fetchall()}")

    # 继续完成切换并验证
    r = tool("cutover")
    print(r.stdout.strip())
    assert r.returncode == 0, r.stderr
    n = c.execute("SELECT count(*) FROM fact").fetchone()[0]
    assert n == len(src)
    assert c.execute("SELECT state FROM shadow_mig.registry").fetchone()[0] == "DONE"
    print("[kill] SIGKILL 恢复演练全部通过。")


if __name__ == "__main__":
    main()
