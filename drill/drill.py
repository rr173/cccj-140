#!/usr/bin/env python3
"""端到端确定性演练：多连接、屏障、崩溃恢复、锁超时、冲突阻塞与修复。

所有屏障均由脚本显式控制（提交点/轮询位点），不使用时间随机因素。
"""
import json
import os
import subprocess
import sys
_li = os.environ.get("MIG_PYLIBS", "/workspace/pylibs/lib/python3.11/site-packages")
sys.path.insert(0, _li)
import time
from decimal import Decimal

import psycopg

DSN = "host=127.0.0.1 port=55432 user=node dbname=drilldb"
TOOL = "/workspace/tool/migrate.py"
CFG = "/workspace/tool/config.json"
CHECK = "/workspace/drill/independent_check.py"
LOGDIR = "/workspace/drill/logs"
os.makedirs(LOGDIR, exist_ok=True)
PASS, FAIL = "PASS", "FAIL"
results = []
evidence = []


def log(msg):
    line = f"{msg}"
    print(line, flush=True)
    evidence.append(line)


def check(name, cond, detail=""):
    tag = PASS if cond else FAIL
    results.append((name, tag, detail))
    log(f"[{tag}] {name} {detail}")
    if not cond:
        raise AssertionError(name + " " + detail)


def run_tool(cmd, extra_env=None, expect_rc=0):
    env = dict(os.environ)
    env["MIGRATE_DSN"] = DSN
    env["MIG_PYLIBS"] = "/workspace/pylibs/lib/python3.11/site-packages"
    if extra_env:
        env.update(extra_env)
    argv = [sys.executable, "-I", TOOL, cmd, "--config", CFG]
    proc = subprocess.run(argv, env=env, capture_output=True, text=True)
    log(f"$ MIGRATE_DSN=... {'MIG_MAX_BATCHES='+extra_env.get('MIG_MAX_BATCHES','') if extra_env else ''}"
        f" python -I migrate.py {cmd} --config config.json  (rc={proc.returncode})")
    for line in proc.stdout.splitlines():
        log("    " + line)
    for line in proc.stderr.splitlines():
        log("    E " + line)
    if expect_rc is not None:
        check(f"命令 rc={expect_rc}: {cmd}", proc.returncode == expect_rc)
    return proc


def q(conn, sql, args=None):
    return conn.execute(sql, args)


def reset_db():
    admin = psycopg.connect("host=127.0.0.1 port=55432 user=node dbname=postgres",
                            autocommit=True)
    admin.execute("DROP DATABASE IF EXISTS drilldb")
    admin.execute("CREATE DATABASE drilldb")
    admin.close()
    c = psycopg.connect(DSN, autocommit=True)
    c.execute("""
        CREATE TABLE public.fact (
            row_id bigint PRIMARY KEY,
            amount_text text, location_text text, note text
        )""")
    c.execute("""
        CREATE VIEW public.v_fact_ro AS
          SELECT row_id, amount_text, location_text, note FROM public.fact
    """)
    return c


def seed_initial(conn):
    rows = [
        (1, "100.00", "east/site-a", "note-1"),
        (2, "1,234.50", "east/site-b", "note-2"),
        (3, "0.01", "west/site-c", "note-3"),
        (4, "12.345", "west/site-d", "half-even→12.34"),
        (5, "12.355", "north/site-e", "half-even→12.36"),
        (6, None, None, "null-row"),
        (7, "7.00", "east\\/escape/site-f", "escaped separator"),
        (8, "8.00", "east/site-g", "will-conflict-1"),
        (9, "9.00", "east/site-g", "will-conflict-2"),
        (10, "bad-amount", "south/site-h", "bad amount then fixed"),
        (11, "11.00", "bad-loc-onlyone", "bad location then fixed"),
        (12, "12.00", "south/site-i", "deleted before copied"),
        (13, "13.00", "south/site-j", "delete then reinsert"),
        (14, "14.00", "south/site-k", "rolled-back update target"),
        (15, "15.00", "south/site-l", "normal"),
    ]
    with conn.transaction():
        conn.cursor().executemany(
            "INSERT INTO fact(row_id,amount_text,location_text,note) "
            "VALUES (%s,%s,%s,%s)", rows)


def table_columns(conn, fq):
    return [r[0] for r in conn.execute("""
        SELECT a.attname FROM pg_attribute a
        JOIN pg_class c ON c.oid=a.attrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=split_part(%s,'.',1) AND c.relname=split_part(%s,'.',2)
          AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum
    """, (fq, fq)).fetchall()]


def view_relation(conn, viewname):
    return conn.execute("""
        SELECT c.relname, pg_get_viewdef(v.oid, true)
        FROM pg_class v JOIN pg_namespace n ON n.oid=v.relnamespace
        JOIN pg_rewrite r ON r.ev_class=v.oid
        JOIN pg_depend d ON d.objid=r.oid
        JOIN pg_class c ON c.oid=d.refobjid
        WHERE n.nspname='public' AND v.relname=%s AND c.relkind='r'
    """, (viewname,)).fetchone()


# ======================================================================
def main():
    log("=" * 78)
    log("演练 A：门禁、复制中崩溃恢复、错误清除、唯一冲突阻塞 cutover、修复后切换")
    log("=" * 78)
    conn = reset_db()
    seed_initial(conn)

    # ---- A0 依赖门禁：制造一个不支持依赖（触发器），plan 必须拒绝 ----
    conn.execute("CREATE FUNCTION public.trg_fn() RETURNS trigger "
                 "LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$")
    conn.execute("CREATE TRIGGER bad_trg BEFORE UPDATE ON fact "
                 "FOR EACH ROW EXECUTE FUNCTION public.trg_fn()")
    p = run_tool("plan", expect_rc=2)
    check("plan 拒绝触发器依赖", "触发器" in p.stdout or "触发器" in p.stderr)
    conn.execute("DROP TRIGGER bad_trg ON fact")
    conn.execute("DROP FUNCTION public.trg_fn")
    run_tool("plan", expect_rc=0)

    # ---- A1 start 与重复 start 幂等 ----
    run_tool("start", expect_rc=0)
    run_tool("start", expect_rc=0)  # 重复 start 返回同一身份
    st = conn.execute("SELECT migration_id, state, mig_schema FROM "
                      "shadow_mig.registry ORDER BY migration_id").fetchall()
    check("只有一条迁移登记", len(st) == 1, str(st))
    mid = st[0][0]
    ms = st[0][2]
    trg = conn.execute("SELECT tgname FROM pg_trigger WHERE tgrelid='public.fact'::regclass "
                       "AND NOT tgisinternal").fetchall()
    check("源表已装捕获触发器", trg == [("trg_shadow_capture",)], str(trg))

    # ---- A2 复制中强退（只跑 2 批，每批 3 行），再重开继续 ----
    run_tool("copy", extra_env={"MIG_MAX_BATCHES": "2"}, expect_rc=0)
    wm1 = conn.execute(f"SELECT v FROM {ms}.meta WHERE k='copy_watermark'").fetchone()[0]
    check("崩溃点水位已持久化(row_id=6)", int(wm1) == 6, wm1)
    sh = conn.execute(f"SELECT count(*) FROM {ms}.shadow").fetchone()[0]
    check("已提交批次行已落影子", sh == 6, str(sh))
    # 重开继续（不带批次限制），跑到 READY
    run_tool("copy", expect_rc=0)
    state = conn.execute("SELECT state FROM shadow_mig.registry "
                         "WHERE migration_id=%s", (mid,)).fetchone()[0]
    check("重开后到达 READY", state == "READY", state)

    # ---- A3 status 显示源行/原因/源状态；修复后错误消失 ----
    p = run_tool("status", expect_rc=0)
    check("status 列出坏行 row_id=10", "row_id=10" in p.stdout)
    check("status 给出原因", "金额文本 'bad-amount'" in p.stdout)
    check("status 给出源状态 JSON", '"note": "bad amount then fixed"' in p.stdout)
    check("status 列出坏行 row_id=11", "row_id=11" in p.stdout)
    check("status 报告唯一冲突", "目标唯一键" in p.stdout)
    # 修复两个映射坏行
    conn.execute("UPDATE fact SET amount_text='10.00' WHERE row_id=10")
    conn.execute("UPDATE fact SET location_text='north/rep-11' WHERE row_id=11")
    # 删除一个冲突行
    conn.execute("DELETE FROM fact WHERE row_id=9")
    run_tool("status", expect_rc=0)
    nerr = conn.execute(f"SELECT count(*) FROM {ms}.row_errors").fetchone()[0]
    check("修复/删除后过期错误清零", nerr == 0, f"remaining={nerr}")

    # ---- A4 制造冲突：cutover 必须失败，旧表照常可写 ----
    conn.execute("UPDATE fact SET location_text='east/site-g' WHERE row_id=8")
    conn.execute("INSERT INTO fact VALUES (20,'20.00','east/site-g','new conflict')")
    run_tool("status", expect_rc=0)
    p = run_tool("cutover", expect_rc=2)
    check("cutover 因冲突被拒绝", "拒绝切换" in (p.stdout + p.stderr))
    # 旧表写入正常（结构未变、可增删改）
    conn.execute("INSERT INTO fact VALUES (21,'21.00','east/z21','write during blocked')")
    conn.execute("UPDATE fact SET note='still writable' WHERE row_id=1")
    conn.execute("DELETE FROM fact WHERE row_id=21")
    cols = table_columns(conn, "public.fact")
    check("旧表仍为旧结构且可写", cols == ["row_id", "amount_text",
                                           "location_text", "note"], str(cols))
    # 修复冲突（不需要重新全表复制）
    conn.execute("UPDATE fact SET location_text='east/fixed-8' WHERE row_id=8")
    conn.execute("UPDATE fact SET location_text='east/fixed-20' WHERE row_id=20")
    run_tool("status", expect_rc=0)
    nerr = conn.execute(f"SELECT count(*) FROM {ms}.row_errors").fetchone()[0]
    check("冲突修复后错误清零", nerr == 0)

    # ---- A5 切换等锁超时演练 ----
    holder = psycopg.connect(DSN, autocommit=False)
    holder.execute("BEGIN")
    holder.execute("UPDATE fact SET note='lock holder' WHERE row_id=1")
    try:
        p = run_tool("cutover", expect_rc=2)
        check("等锁超时时回滚且迁移仍 READY",
              "等锁超时" in (p.stdout + p.stderr))
        st2 = conn.execute("SELECT state FROM shadow_mig.registry "
                           "WHERE migration_id=%s", (mid,)).fetchone()[0]
        check("超时后状态仍 READY", st2 == "READY", st2)
        check("超时后旧表名仍是旧表",
              table_columns(conn, "public.fact") ==
              ["row_id", "amount_text", "location_text", "note"])
    finally:
        holder.execute("ROLLBACK")
        holder.close()

    # ---- A6 正式 cutover；两侧断开重连核对 ----
    conn.close()
    p = run_tool("cutover", expect_rc=0)
    c1 = psycopg.connect(DSN, autocommit=True)
    c2 = psycopg.connect(DSN, autocommit=True)
    for tag, cc in (("连接1", c1), ("连接2", c2)):
        check(f"{tag} 看到新列结构",
              table_columns(cc, "public.fact") ==
              ["row_id", "amount", "region", "site", "note", "last_src_seq"])
    state = c1.execute("SELECT state FROM shadow_mig.registry "
                       "WHERE migration_id=%s", (mid,)).fetchone()[0]
    check("登记为 DONE", state == "DONE", state)
    trg = c1.execute("SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal "
                     "AND tgrelid='public.fact'::regclass").fetchone()[0]
    check("新表上没有捕获器", trg == 0)
    old = c1.execute("SELECT old_physical_name FROM shadow_mig.registry "
                     "WHERE migration_id=%s", (mid,)).fetchone()[0]
    check("旧物理表保留", old == f"public.fact__old_{mid}", old)
    v = view_relation(c1, "v_fact_ro")
    check("视图绑定新物理表", v is not None and v[0] == "fact", str(v))
    check("视图定义为新列",
          all(tok in v[1] for tok in ("amount", "region", "site"))
          and "amount_text" not in v[1], v[1])
    # 新结构写入
    c1.execute("INSERT INTO fact(row_id,amount,region,site,note) "
               "VALUES (99, 42.50, 'newr','news','post-cutover')")
    got = c1.execute("SELECT amount,region,site FROM fact WHERE row_id=99").fetchone()
    check("切换后新写入走新列", got == (Decimal("42.50"), "newr", "news"), str(got))
    c1.execute("DELETE FROM fact WHERE row_id=99")

    # ---- A7 独立核对（静态快照逐行重算，不调用产品函数）----
    checker = subprocess.run([sys.executable, "-I", CHECK, "--dsn", DSN,
                              "--source", f"public.fact__old_{mid}"],
                             capture_output=True, text=True)
    log("$ python -I independent_check.py --source " + f"public.fact__old_{mid}")
    for line in checker.stdout.splitlines():
        log("    " + line)
    check("独立逐行核对通过", checker.returncode == 0, checker.stderr)
    # 手写预期值
    exp = {
        1: (Decimal("100.00"), "east", "site-a"),
        2: (Decimal("1234.50"), "east", "site-b"),
        4: (Decimal("12.34"), "west", "site-d"),   # half-even
        5: (Decimal("12.36"), "north", "site-e"),  # half-even
        6: (None, None, None),
        7: (Decimal("7.00"), "east/escape", "site-f"),
    }
    for rid, (a, r, s) in exp.items():
        g = c2.execute("SELECT amount,region,site FROM fact WHERE row_id=%s",
                       (rid,)).fetchone()
        check(f"手写预期 row_id={rid}", g == (a, r, s), str(g))
    vq = c2.execute("SELECT amount, region, site FROM v_fact_ro "
                    "WHERE row_id=4").fetchone()
    check("视图查询数值为手写预期", vq == (Decimal("12.34"), "west", "site-d"),
          str(vq))
    c1.close(); c2.close()

    # ==================================================================
    log("=" * 78)
    log("演练 B：交错裁定（删前插、旧值后更新、复制前删除、回滚、负序/小 row_id）")
    log("=" * 78)
    conn = reset_db()
    seed_initial(conn)
    # 去掉会制造错误的行，保证本轮可切换
    conn.execute("DELETE FROM fact WHERE row_id IN (10,11)")
    conn.execute("UPDATE fact SET location_text='east/conf8' WHERE row_id=8")
    conn.execute("UPDATE fact SET location_text='east/conf9' WHERE row_id=9")
    run_tool("start", expect_rc=0)
    mid2 = conn.execute("SELECT migration_id FROM shadow_mig.registry "
                        "ORDER BY migration_id DESC LIMIT 1").fetchone()[0]
    ms2 = conn.execute("SELECT mig_schema FROM shadow_mig.registry "
                       "WHERE migration_id=%s", (mid2,)).fetchone()[0]

    # 多连接写入器
    w1 = psycopg.connect(DSN, autocommit=True)
    w2 = psycopg.connect(DSN, autocommit=True)
    w3 = psycopg.connect(DSN, autocommit=False)  # 回滚连接

    # 屏障：先让复制跑 1 批（row_id 1..3 落影子，含 row_id=1 旧值）
    run_tool("copy", extra_env={"MIG_MAX_BATCHES": "1"}, expect_rc=0)
    old_a = conn.execute(f"SELECT amount FROM {ms2}.shadow WHERE row_id=1").fetchone()[0]
    check("复制先读到旧 A(100.00)", old_a == Decimal("100.00"), str(old_a))

    # 交错 1：复制拿到旧 A 后业务把 A 改新
    w1.execute("UPDATE fact SET amount_text='111.11' WHERE row_id=1")
    # 交错 2：未复制 B(=12) 在复制前删除
    w1.execute("DELETE FROM fact WHERE row_id=12")
    # 交错 3：同 row_id 先删后插（13）
    w1.execute("DELETE FROM fact WHERE row_id=13")
    w1.execute("INSERT INTO fact VALUES (13,'13.50','south/re-j','reinserted')")
    # 交错 4：回滚更新（14 保持 14.00）
    w3.execute("BEGIN")
    w3.execute("UPDATE fact SET amount_text='999.99' WHERE row_id=14")
    w3.execute("ROLLBACK")
    # 交错 5：迁移中新增负序/更小 row_id
    w2.execute("INSERT INTO fact VALUES (-5,'-5.00','neg/r1','negative id')")
    w2.execute("INSERT INTO fact VALUES (0,'0.00','zero/r0','zero id')")
    # 交错 6：复制后再更新一个已复制行（3）
    w2.execute("UPDATE fact SET amount_text='3.33', location_text='west/new-c' "
               "WHERE row_id=3")
    w1.close(); w2.close(); w3.close()

    run_tool("copy", expect_rc=0)
    st = conn.execute("SELECT state FROM shadow_mig.registry "
                      "WHERE migration_id=%s", (mid2,)).fetchone()[0]
    check("B 轮到达 READY", st == "READY", st)
    cases = [
        (1, Decimal("111.11"), "east", "site-a"),   # 后更新覆盖
        (3, Decimal("3.33"), "west", "new-c"),      # 复制后更新
        (12, None),                                  # 复制前删除：影子无
        (13, Decimal("13.50"), "south", "re-j"),    # 删后重插
        (14, Decimal("14.00"), "south", "site-k"),  # 回滚不留痕
        (-5, Decimal("-5.00"), "neg", "r1"),        # 负序补写
        (0, Decimal("0.00"), "zero", "r0"),         # 小 row_id 补写
    ]
    for rid, *rest in cases:
        if rest == [None]:
            g = conn.execute(f"SELECT count(*) FROM {ms2}.shadow WHERE row_id=%s",
                             (rid,)).fetchone()[0]
            check(f"row_id={rid} 已删除不入影子", g == 0)
            g2 = conn.execute("SELECT count(*) FROM fact WHERE row_id=%s",
                              (rid,)).fetchone()[0]
            check(f"源表 row_id={rid} 确实不存在", g2 == 0)
        else:
            g = conn.execute(
                f"SELECT amount,region,site FROM {ms2}.shadow WHERE row_id=%s",
                (rid,)).fetchone()
            check(f"交错裁定 row_id={rid}", tuple(g) == tuple(rest), str(g))
    ghost = conn.execute(
        f"SELECT count(*) FROM {ms2}.change_log WHERE row_id=14 AND "
        "amount_text='999.99'").fetchone()[0]
    check("回滚更新无幽灵流水", ghost == 0)
    # 双向完整性
    src_n = conn.execute("SELECT count(*) FROM fact").fetchone()[0]
    sh_n = conn.execute(f"SELECT count(*) FROM {ms2}.shadow").fetchone()[0]
    check("源/影子行数一致（无错误时）", src_n == sh_n, f"{src_n} vs {sh_n}")

    # B 轮也走完整 cutover + 独立核对
    run_tool("cutover", expect_rc=0)
    old2 = conn.execute("SELECT old_physical_name FROM shadow_mig.registry "
                        "WHERE migration_id=%s", (mid2,)).fetchone()[0]
    checker = subprocess.run([sys.executable, "-I", CHECK, "--dsn", DSN,
                              "--source", old2],
                             capture_output=True, text=True,
                             env={**os.environ,
                                  "MIG_PYLIBS":
                                  "/workspace/pylibs/lib/python3.11/site-packages"})
    log(f"$ python -I independent_check.py --source {old2}")
    for line in checker.stdout.splitlines():
        log("    " + line)
    check("B 轮独立逐行核对通过", checker.returncode == 0, checker.stderr)
    check("B 轮视图仍绑定新表",
          view_relation(conn, "v_fact_ro")[0] == "fact")
    conn.close()

    # ==================================================================
    log("=" * 78)
    log("演练 C：abort 只清理迁移对象，业务表不受影响")
    log("=" * 78)
    conn = reset_db()
    seed_initial(conn)
    run_tool("start", expect_rc=0)
    run_tool("copy", extra_env={"MIG_MAX_BATCHES": "1"}, expect_rc=0)
    run_tool("abort", expect_rc=0)
    check("迁移 schema 已删除",
          conn.execute("SELECT count(*) FROM pg_namespace WHERE nspname LIKE 'shmig_%'")
          .fetchone()[0] == 0)
    check("源表触发器已摘除",
          conn.execute("SELECT count(*) FROM pg_trigger WHERE NOT tgisinternal "
                       "AND tgrelid='public.fact'::regclass").fetchone()[0] == 0)
    n = conn.execute("SELECT count(*) FROM fact").fetchone()[0]
    check("源表数据完好(15行)", n == 15, str(n))
    conn.execute("INSERT INTO fact VALUES (50,'50.00','a/b','post abort write')")
    check("abort 后旧表可正常写入",
          conn.execute("SELECT note FROM fact WHERE row_id=50").fetchone()[0]
          == "post abort write")
    # abort 后可以重新 start
    run_tool("start", expect_rc=0)
    run_tool("abort", expect_rc=0)
    conn.close()

    # ---- 汇总 ----
    log("=" * 78)
    npass = sum(1 for _, t, _ in results if t == PASS)
    nfail = sum(1 for _, t, _ in results if t == FAIL)
    log(f"演练汇总：{npass} PASS / {nfail} FAIL，共 {len(results)} 项断言")
    with open(os.path.join(LOGDIR, "drill_evidence.txt"), "w",
              encoding="utf-8") as fh:
        fh.write("\n".join(evidence) + "\n")
    return 1 if nfail else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as exc:
        log(f"演练中止: {exc}")
        with open(os.path.join(LOGDIR, "drill_evidence.txt"), "w",
                  encoding="utf-8") as fh:
            fh.write("\n".join(evidence) + "\n")
        sys.exit(1)
