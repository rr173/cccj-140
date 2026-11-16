#!/usr/bin/env python3
"""在线影子重编码迁移工具（受管事实表子集，非通用迁移平台）。

接口：plan / start / copy / status / cutover / abort
变更捕获完全在本 PostgreSQL 实例内用行级触发器实现，不使用外部复制服务。
转换为源行的确定性纯函数：无当前时间、无随机数、无外部查询、无浮点金额。
"""
from __future__ import annotations

import os
import sys
# 兼容 python -I（忽略 PYTHONPATH）：由 MIG_PYLIBS 显式声明依赖目录
_li = os.environ.get("MIG_PYLIBS")
if _li:
    sys.path.insert(0, _li)

import argparse
import json
import re
import sys
import zlib
from decimal import Decimal, InvalidOperation, ROUND_HALF_EVEN, ROUND_HALF_UP, \
    ROUND_HALF_DOWN, ROUND_DOWN, localcontext

import psycopg
from psycopg import sql
from psycopg.errors import UniqueViolation, QueryCanceled, LockNotAvailable, UndefinedTable

CONTROL_SCHEMA_DEFAULT = "shadow_mig"
INT64_MIN = -(2 ** 63)
ROUNDING = {
    "half_even": ROUND_HALF_EVEN,
    "half_up": ROUND_HALF_UP,
    "half_down": ROUND_HALF_DOWN,
    "truncate": ROUND_DOWN,
}
ACTIVE_STATES = ("INIT", "COPYING", "CATCHUP", "READY", "CUTTING_OVER")
TERMINAL_STATES = ("DONE", "ABORTED")


class MigrationError(Exception):
    pass


# --------------------------------------------------------------------------
# 配置
# --------------------------------------------------------------------------
def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    src = cfg.get("source", {})
    if not src.get("schema") or not src.get("table"):
        raise MigrationError("config.source.schema/table 必填")
    cfg.setdefault("control_schema", CONTROL_SCHEMA_DEFAULT)
    amt = cfg.setdefault("amount", {})
    amt.setdefault("decimal_sep", ".")
    amt.setdefault("thousands_sep", ",")
    amt.setdefault("scale", 2)
    amt.setdefault("rounding", "half_even")
    amt.setdefault("pattern", r"^-?[0-9]+(\\.[0-9]+)?$")
    amt.setdefault("nullable", True)
    amt.setdefault("min", None)
    amt.setdefault("max", None)
    if amt["rounding"] not in ROUNDING:
        raise MigrationError(f"不支持的舍入模式: {amt['rounding']}")
    loc = cfg.setdefault("location", {})
    loc.setdefault("separator", "/")
    loc.setdefault("nullable", True)
    tgt = cfg.setdefault("target", {})
    tgt.setdefault("amount_type", f"numeric(14,{amt['scale']})")
    tgt.setdefault("region_not_null", not loc["nullable"])
    tgt.setdefault("site_not_null", not loc["nullable"])
    tgt.setdefault("amount_not_null", not amt["nullable"])
    tgt.setdefault("unique_keys", [["region", "site"]])
    cfg.setdefault("managed_views", [])
    cfg.setdefault("cutover", {"lock_timeout_ms": 3000})
    cfg.setdefault("copy", {"batch_size": 200})
    return cfg


def config_hash(cfg: dict) -> str:
    import hashlib
    blob = json.dumps(cfg, sort_keys=True, ensure_ascii=False).encode()
    return hashlib.sha256(blob).hexdigest()[:24]


def qident(*parts):
    return sql.Identifier(*parts)


def advisory_key(cfg: dict) -> int:
    h1 = zlib.crc32(cfg["source"]["schema"].encode())
    h2 = zlib.crc32(cfg["source"]["table"].encode())
    v = (h1 << 32) | h2
    if v >= 2 ** 63:
        v -= 2 ** 64
    return v


# --------------------------------------------------------------------------
# 确定性行级转换（纯函数）
# --------------------------------------------------------------------------
class TransformError(Exception):
    pass


def parse_amount(text, acfg):
    if text is None:
        if acfg["nullable"]:
            return None
        raise TransformError("amount_text 为 NULL，但目标金额声明 NOT NULL")
    if not isinstance(text, str):
        raise TransformError(f"amount_text 不是文本: {text!r}")
    raw = text
    t = raw
    if acfg["thousands_sep"]:
        t = t.replace(acfg["thousands_sep"], "")
    if acfg["decimal_sep"] != ".":
        t = t.replace(acfg["decimal_sep"], ".", 1)
    if not re.match(acfg["pattern"], t):
        raise TransformError(f"金额文本 {raw!r} 不符合声明格式 {acfg['pattern']}")
    try:
        with localcontext() as ctx:
            ctx.prec = 50
            d = Decimal(t)
            q = d.quantize(Decimal(1).scaleb(-acfg["scale"]),
                           rounding=ROUNDING[acfg["rounding"]])
    except (InvalidOperation, ValueError) as exc:
        raise TransformError(f"金额 {raw!r} 定点量化失败: {exc}")
    if acfg["min"] is not None and q < Decimal(str(acfg["min"])):
        raise TransformError(f"金额 {q} 低于声明下限 {acfg['min']}")
    if acfg["max"] is not None and q > Decimal(str(acfg["max"])):
        raise TransformError(f"金额 {q} 高于声明上限 {acfg['max']}")
    return q


def split_location(text, lcfg):
    if text is None:
        if lcfg["nullable"]:
            return None, None
        raise TransformError("location_text 为 NULL，但目标 region/site 声明 NOT NULL")
    if not isinstance(text, str):
        raise TransformError(f"location_text 不是文本: {text!r}")
    sep = lcfg["separator"]
    parts, cur, esc = [], [], False
    for ch in text:
        if esc:
            if ch == sep:
                cur.append(sep)
            elif ch == "\\":
                cur.append("\\")
            else:
                raise TransformError(
                    f"location_text {text!r} 含非法转义序列 \\{ch}")
            esc = False
        elif ch == "\\":
            esc = True
        elif ch == sep:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if esc:
        raise TransformError(f"location_text {text!r} 以悬空反斜杠结尾")
    parts.append("".join(cur))
    if len(parts) != 2:
        raise TransformError(
            f"location_text {text!r} 按分隔符 {sep!r} 拆分得到 {len(parts)} 段，"
            "必须恰好为 region/site 两段（需字面分隔符请写 \\| 形式）")
    region, site = parts
    if not lcfg["nullable"] and (region == "" or site == ""):
        raise TransformError(f"location_text {text!r} 拆出空 region 或空 site")
    return region, site


def transform_row(payload, cfg):
    amount = parse_amount(payload["amount_text"], cfg["amount"])
    region, site = split_location(payload["location_text"], cfg["location"])
    return {
        "row_id": payload["row_id"],
        "amount": amount,
        "region": region,
        "site": site,
        "note": payload["note"],
    }


# --------------------------------------------------------------------------
# 控制库与迁移对象 DDL
# --------------------------------------------------------------------------
def ensure_control_schema(conn, cfg):
    cs = cfg["control_schema"]
    conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(qident(cs)))
    conn.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {cs}.registry (
            migration_id      bigserial PRIMARY KEY,
            source_schema     text NOT NULL,
            source_table      text NOT NULL,
            mig_schema        text NOT NULL,
            state             text NOT NULL,
            config_hash       text NOT NULL,
            managed_views     jsonb NOT NULL DEFAULT '[]'::jsonb,
            old_physical_name text
        )""").format(cs=qident(cs)))
    conn.execute(sql.SQL("""
        CREATE UNIQUE INDEX IF NOT EXISTS registry_one_active ON {cs}.registry
        (source_schema, source_table)
        WHERE state IN ('INIT','COPYING','CATCHUP','READY','CUTTING_OVER')
    """).format(cs=qident(cs)))


def get_active(conn, cfg):
    return conn.execute(sql.SQL(
        "SELECT migration_id, mig_schema, state, config_hash, managed_views, "
        "old_physical_name FROM {cs}.registry "
        "WHERE source_schema=%s AND source_table=%s "
        "AND state IN ('INIT','COPYING','CATCHUP','READY','CUTTING_OVER')"
    ).format(cs=qident(cfg["control_schema"])),
        (cfg["source"]["schema"], cfg["source"]["table"])).fetchone()


def get_any_latest(conn, cfg):
    return conn.execute(sql.SQL(
        "SELECT migration_id, mig_schema, state FROM {cs}.registry "
        "WHERE source_schema=%s AND source_table=%s "
        "ORDER BY migration_id DESC LIMIT 1"
    ).format(cs=qident(cfg["control_schema"])),
        (cfg["source"]["schema"], cfg["source"]["table"])).fetchone()


def shadow_ddl(schema, cfg):
    t = cfg["target"]
    cols = [
        sql.SQL("row_id bigint PRIMARY KEY"),
        sql.SQL("amount {}").format(sql.SQL(t["amount_type"])),
        sql.SQL("region text"), sql.SQL("site text"),
        sql.SQL("note text"), sql.SQL("last_src_seq bigint"),
    ]
    if t["amount_not_null"]:
        cols.append(sql.SQL("amount NOT NULL"))
    if t["region_not_null"]:
        cols.append(sql.SQL("region NOT NULL"))
    if t["site_not_null"]:
        cols.append(sql.SQL("site NOT NULL"))
    a = cfg["amount"]
    if a["min"] is not None:
        cols.append(sql.SQL("CHECK (amount IS NULL OR amount >= {})").format(
            sql.Literal(Decimal(str(a["min"])))))
    if a["max"] is not None:
        cols.append(sql.SQL("CHECK (amount IS NULL OR amount <= {})").format(
            sql.Literal(Decimal(str(a["max"])))))
    stmts = [sql.SQL("CREATE TABLE {s}.shadow ({cols})").format(
        s=qident(schema), cols=sql.SQL(", ").join(cols))]
    for i, keycols in enumerate(t["unique_keys"]):
        stmts.append(sql.SQL(
            "CREATE UNIQUE INDEX shadow_uq_{i} ON {s}.shadow ({kc})").format(
            i=sql.Literal(i), s=qident(schema),
            kc=sql.SQL(", ").join(qident(c) for c in keycols)))
    return stmts


def create_migration_objects(conn, cfg, mig_id):
    s = f"shmig_{mig_id}"
    conn.execute(sql.SQL("CREATE SCHEMA {}").format(qident(s)))
    for stmt in shadow_ddl(s, cfg):
        conn.execute(stmt)
    conn.execute(sql.SQL("""
        CREATE TABLE {s}.change_log (
            seq bigserial PRIMARY KEY,
            op char(1) NOT NULL CHECK (op IN ('I','U','D')),
            row_id bigint NOT NULL,
            amount_text text, location_text text, note text
        )""").format(s=qident(s)))
    conn.execute(sql.SQL("""
        CREATE TABLE {s}.row_errors (
            row_id bigint PRIMARY KEY,
            reason text NOT NULL,
            source_state jsonb NOT NULL,
            at_seq bigint
        )""").format(s=qident(s)))
    conn.execute(sql.SQL("""
        CREATE TABLE {s}.meta (k text PRIMARY KEY, v text NOT NULL)
    """).format(s=qident(s)))
    conn.execute(sql.SQL("""
        INSERT INTO {s}.meta(k,v) VALUES
        ('state','COPYING'), ('copy_watermark',%s), ('applied_seq','0')
    """).format(s=qident(s)), (str(INT64_MIN),))
    # 每迁移独立的捕获函数：abort DROP SCHEMA 即可彻底清除
    conn.execute(sql.SQL("""
        CREATE FUNCTION {s}.capture_fn() RETURNS trigger
        LANGUAGE plpgsql AS $fn$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                INSERT INTO {s}.change_log(op,row_id,amount_text,location_text,note)
                VALUES ('D', OLD.row_id, OLD.amount_text, OLD.location_text, OLD.note);
                RETURN OLD;
            END IF;
            INSERT INTO {s}.change_log(op,row_id,amount_text,location_text,note)
            VALUES (left(TG_OP,1), NEW.row_id, NEW.amount_text,
                    NEW.location_text, NEW.note);
            RETURN NEW;
        END;
        $fn$""").format(s=qident(s)))
    conn.execute(sql.SQL("""
        CREATE TRIGGER trg_shadow_capture
        AFTER INSERT OR UPDATE OR DELETE ON {src}
        FOR EACH ROW EXECUTE FUNCTION {s}.capture_fn()
    """).format(src=qident(cfg["source"]["schema"], cfg["source"]["table"]),
                s=qident(s)))
    return s


# --------------------------------------------------------------------------
# 依赖门禁
# --------------------------------------------------------------------------
def check_supported(conn, cfg):
    sc, tb = cfg["source"]["schema"], cfg["source"]["table"]
    row = conn.execute(
        "SELECT c.oid, c.relkind, c.relrowsecurity, c.relispartition, c.relpartbound "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname=%s AND c.relname=%s", (sc, tb)).fetchone()
    if row is None:
        raise MigrationError(f"源表 {sc}.{tb} 不存在")
    oid, relkind, rls, ispart, partbound = row
    if relkind != "r":
        raise MigrationError(f"仅支持普通表(relkind='r')，当前 relkind={relkind}")
    if ispart or partbound is not None:
        raise MigrationError("不支持分区表")
    if rls:
        raise MigrationError("不支持启用行级安全的表")
    problems, deps = [], []

    inh = conn.execute(
        "SELECT 1 FROM pg_inherits WHERE inhparent=%s OR inhrelid=%s",
        (oid, oid)).fetchall()
    if inh:
        problems.append("表存在继承关系")

    cols = conn.execute(
        "SELECT a.attname, format_type(a.atttypid,a.atttypmod), a.attnotnull "
        "FROM pg_attribute a WHERE a.attrelid=%s AND a.attnum>0 AND NOT a.attisdropped "
        "ORDER BY a.attnum", (oid,)).fetchall()
    expect = {"row_id": ("bigint", True), "amount_text": ("text", None),
              "location_text": ("text", None), "note": ("text", None)}
    got = {r[0]: (r[1], r[2]) for r in cols}
    if set(got) != set(expect):
        problems.append(f"列集合不匹配: 实际 {sorted(got)}，期望 {sorted(expect)}")
    for name, (typ, notnull) in expect.items():
        if name in got and got[name][0] != typ:
            problems.append(f"列 {name} 类型为 {got[name][0]}，期望 {typ}")
    if got.get("row_id", (None, False))[1] is not True:
        problems.append("row_id 必须是非空列")

    pk = conn.execute(
        "SELECT con.contype, pg_get_constraintdef(con.oid) "
        "FROM pg_constraint con WHERE con.conrelid=%s ORDER BY 1", (oid,)).fetchall()
    for contype, defn in pk:
        if contype == "p":
            if "ROW_ID" not in defn.upper() or "row_id" not in defn:
                problems.append(f"主键必须是 row_id: {defn}")
        elif contype == "f":
            problems.append(f"不支持外键: {defn}")
        elif contype == "u":
            problems.append(f"不支持源表额外唯一约束: {defn}")
        else:
            problems.append(f"不支持的约束({contype}): {defn}")

    trg = conn.execute(
        "SELECT tgname FROM pg_trigger WHERE tgrelid=%s AND NOT tgisinternal",
        (oid,)).fetchall()
    if trg:
        problems.append("表上存在触发器: " + ", ".join(r[0] for r in trg))

    # 依赖该表的对象（排除 TOAST 等内部关系）
    dependents = conn.execute("""
        SELECT DISTINCT c.relkind, n.nspname, c.relname
        FROM pg_depend d
        JOIN pg_rewrite rw ON rw.oid=d.objid
        JOIN pg_class c ON c.oid=rw.ev_class
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE d.refobjid=%s AND d.classid='pg_rewrite'::regclass
        UNION
        SELECT DISTINCT c.relkind, n.nspname, c.relname
        FROM pg_depend d
        JOIN pg_class c ON c.oid=d.objid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE d.refobjid=%s AND d.classid='pg_class'::regclass
              AND d.objid <> %s
              AND c.relkind NOT IN ('t','i')
              AND n.nspname NOT LIKE %s
    """, (oid, oid, oid, "pg_toast%")).fetchall()
    managed = {(v["schema"], v["name"]) for v in cfg["managed_views"]}
    view_oid = dict()
    for kind, ns, name in dependents:
        if kind == "v" and (ns, name) in managed:
            deps.append(f"受管只读视图 {ns}.{name}")
            void = conn.execute(
                "SELECT oid FROM pg_class WHERE relname=%s AND relnamespace="
                "(SELECT oid FROM pg_namespace WHERE nspname=%s)",
                (name, ns)).fetchone()[0]
            view_oid[(ns, name)] = void
            extra = conn.execute(
                "SELECT 1 FROM pg_rewrite r WHERE r.ev_class=%s "
                "AND r.ev_type IS DISTINCT FROM '1'", (void,)).fetchall()
            if extra:
                problems.append(f"视图 {ns}.{name} 含 SELECT 以外的规则，非只读视图")
            other_tabs = conn.execute("""
                SELECT DISTINCT n2.nspname, c2.relname
                FROM pg_depend d2
                JOIN pg_class c2 ON c2.oid=d2.refobjid
                JOIN pg_namespace n2 ON n2.oid=c2.relnamespace
                WHERE d2.classid='pg_rewrite'::regclass
                  AND d2.objid IN (SELECT oid FROM pg_rewrite WHERE ev_class=%s)
                  AND d2.refclassid='pg_class'::regclass
                  AND c2.oid NOT IN (%s, %s)
            """, (void, oid, void)).fetchall()
            if other_tabs:
                problems.append(
                    f"视图 {ns}.{name} 依赖了源表以外的对象: {other_tabs}")
        elif kind == "m":
            problems.append(f"不支持物化视图: {ns}.{name}")
        elif kind == "v":
            problems.append(f"未在 managed_views 声明的视图: {ns}.{name}")
        else:
            problems.append(f"不支持的依赖对象(relkind={kind}): {ns}.{name}")

    pub = conn.execute(
        "SELECT p.pubname FROM pg_publication_rel pr JOIN pg_publication p "
        "ON p.oid=pr.prpubid WHERE pr.prrelid=%s", (oid,)).fetchall()
    if pub:
        problems.append("表属于逻辑复制发布: " + ", ".join(r[0] for r in pub))
    seq = conn.execute(
        "SELECT sequence_schema, sequence_name FROM information_schema.sequences "
        "WHERE sequence_schema=%s", (sc,)).fetchall()
    # 仅拒绝 owned 序列
    owned = conn.execute(
        "SELECT n.nspname, s.relname FROM pg_depend d JOIN pg_class s ON s.oid=d.objid "
        "JOIN pg_namespace n ON n.oid=s.relnamespace "
        "WHERE d.refobjid=%s AND s.relkind='S' AND d.deptype='a'", (oid,)).fetchall()
    if owned:
        problems.append("不支持属主序列: " + ", ".join(f"{a}.{b}" for a, b in owned))
    deps.append(f"列: {[c[0] for c in cols]}")
    return {"oid": oid, "deps": deps, "problems": problems}


# --------------------------------------------------------------------------
# 影子应用（幂等）
# --------------------------------------------------------------------------
UPSERT_SQL = None  # 动态构建


def apply_payload(conn, schema, cfg, op, payload, seq):
    """op: I/U/D。payload 为源行当前/删除前四元组。返回 'ok'/'error'。"""
    rid = payload["row_id"]
    if op == "D":
        conn.execute(sql.SQL("DELETE FROM {s}.shadow WHERE row_id=%s").format(
            s=qident(schema)), (rid,))
        conn.execute(sql.SQL("DELETE FROM {s}.row_errors WHERE row_id=%s").format(
            s=qident(schema)), (rid,))
        return "ok"
    try:
        row = transform_row(payload, cfg)
    except TransformError as exc:
        conn.execute(sql.SQL("DELETE FROM {s}.shadow WHERE row_id=%s").format(
            s=qident(schema)), (rid,))
        conn.execute(sql.SQL("""
            INSERT INTO {s}.row_errors(row_id, reason, source_state, at_seq)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (row_id) DO UPDATE SET
              reason=EXCLUDED.reason,
              source_state=EXCLUDED.source_state,
              at_seq=EXCLUDED.at_seq
        """).format(s=qident(schema)),
            (rid, str(exc), json.dumps(payload, ensure_ascii=False, default=str), seq))
        return "error"
    try:
        with conn.transaction():  # 保存点：唯一冲突只回滚本行
            conn.execute(sql.SQL("""
                INSERT INTO {s}.shadow
                  (row_id, amount, region, site, note, last_src_seq)
                VALUES (%(row_id)s,%(amount)s,%(region)s,%(site)s,%(note)s,%(seq)s)
                ON CONFLICT (row_id) DO UPDATE SET
                  amount=EXCLUDED.amount, region=EXCLUDED.region,
                  site=EXCLUDED.site, note=EXCLUDED.note,
                  last_src_seq=EXCLUDED.last_src_seq
            """).format(s=qident(schema)),
                         {**row, "seq": seq})
    except UniqueViolation:
        # 找到每个声明唯一键上的冲突对方，全部登记错误（旧表写入不受影响）
        offenders = {rid}
        reasons = []
        for uq in cfg["target"]["unique_keys"]:
            conds = sql.SQL(" AND ").join(
                sql.SQL("{c} IS NOT DISTINCT FROM %s").format(c=qident(c))
                for c in uq)
            params = [row[c] for c in uq] + [rid]
            other = conn.execute(sql.SQL("""
                SELECT row_id FROM {s}.shadow WHERE {conds} AND row_id <> %s
            """).format(s=qident(schema), conds=conds), params).fetchall()
            if other:
                ids = [r[0] for r in other]
                offenders.update(ids)
                reasons.append(f"目标唯一键 {uq} 冲突: row_id={rid} 与 {ids}")
        reason = "; ".join(reasons) or f"目标唯一约束冲突: row_id={rid}"
        for bad_rid in sorted(offenders):
            src = fetch_source(conn, cfg, bad_rid)
            if src is None:
                # 冲突对方已从源表删除：清掉其过期登记，等待 reconcile 清影子
                conn.execute(sql.SQL(
                    "DELETE FROM {s}.row_errors WHERE row_id=%s").format(
                    s=qident(schema)), (bad_rid,))
                continue
            conn.execute(sql.SQL("""
                INSERT INTO {s}.row_errors(row_id, reason, source_state, at_seq)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (row_id) DO UPDATE SET
                  reason=EXCLUDED.reason,
                  source_state=EXCLUDED.source_state,
                  at_seq=EXCLUDED.at_seq
            """).format(s=qident(schema)),
                         (bad_rid, reason,
                          json.dumps(src, ensure_ascii=False, default=str), seq))
        return "error"
    conn.execute(sql.SQL("DELETE FROM {s}.row_errors WHERE row_id=%s").format(
        s=qident(schema)), (rid,))
    return "ok"


def fetch_source(conn, cfg, rid):
    r = conn.execute(sql.SQL(
        "SELECT row_id, amount_text, location_text, note FROM {t} WHERE row_id=%s"
    ).format(t=qident(cfg["source"]["schema"], cfg["source"]["table"])),
        (rid,)).fetchone()
    if r is None:
        return None
    return {"row_id": r[0], "amount_text": r[1],
            "location_text": r[2], "note": r[3]}


def get_meta(conn, schema):
    rows = conn.execute(sql.SQL("SELECT k,v FROM {s}.meta").format(
        s=qident(schema))).fetchall()
    return {k: v for k, v in rows}


def set_meta(conn, schema, kv):
    for k, v in kv.items():
        conn.execute(sql.SQL("""
            INSERT INTO {s}.meta(k,v) VALUES (%s,%s)
            ON CONFLICT (k) DO UPDATE SET v=EXCLUDED.v
        """).format(s=qident(schema)), (k, str(v)))


def drain_change_log(conn, schema, cfg):
    """应用所有未处理流水，返回处理条数。"""
    meta = get_meta(conn, schema)
    applied = int(meta["applied_seq"])
    n = 0
    while True:
        rows = conn.execute(sql.SQL("""
            SELECT seq, op, row_id, amount_text, location_text, note
            FROM {s}.change_log WHERE seq > %s ORDER BY seq LIMIT 500
        """).format(s=qident(schema)), (applied,)).fetchall()
        if not rows:
            break
        for seq, op, rid, amt, loc, note in rows:
            payload = {"row_id": rid, "amount_text": amt,
                       "location_text": loc, "note": note}
            apply_payload(conn, schema, cfg, op, payload, seq)
            applied = seq
            n += 1
        set_meta(conn, schema, {"applied_seq": applied})
    return n


def reconcile(conn, schema, cfg):
    """双向反连接：缺行补写、多行删除。返回 (补缺, 清多)。"""
    src_t = qident(cfg["source"]["schema"], cfg["source"]["table"])
    missing = conn.execute(sql.SQL("""
        SELECT t.row_id, t.amount_text, t.location_text, t.note
        FROM {t} t
        LEFT JOIN {s}.shadow h ON h.row_id=t.row_id
        LEFT JOIN {s}.row_errors e ON e.row_id=t.row_id
        WHERE h.row_id IS NULL AND e.row_id IS NULL
    """).format(t=src_t, s=qident(schema))).fetchall()
    for rid, amt, loc, note in missing:
        apply_payload(conn, schema, cfg, "I",
                      {"row_id": rid, "amount_text": amt,
                       "location_text": loc, "note": note}, None)
    extra = conn.execute(sql.SQL("""
        SELECT h.row_id FROM {s}.shadow h
        LEFT JOIN {t} t ON t.row_id=h.row_id WHERE t.row_id IS NULL
    """).format(s=qident(schema), t=src_t)).fetchall()
    for (rid,) in extra:
        conn.execute(sql.SQL("DELETE FROM {s}.shadow WHERE row_id=%s").format(
            s=qident(schema)), (rid,))
        conn.execute(sql.SQL("DELETE FROM {s}.row_errors WHERE row_id=%s").format(
            s=qident(schema)), (rid,))
    return len(missing), len(extra)


def recheck_errors(conn, schema, cfg):
    """错误复检：删除的源行清错；仍存在的按当前源状态重算。返回剩余错误数。"""
    rids = [r[0] for r in conn.execute(sql.SQL(
        "SELECT row_id FROM {s}.row_errors ORDER BY row_id").format(
        s=qident(schema))).fetchall()]
    for rid in rids:
        src = fetch_source(conn, cfg, rid)
        if src is None:
            conn.execute(sql.SQL(
                "DELETE FROM {s}.row_errors WHERE row_id=%s").format(
                s=qident(schema)), (rid,))
        else:
            apply_payload(conn, schema, cfg, "I", src, None)
    return conn.execute(sql.SQL(
        "SELECT count(*) FROM {s}.row_errors").format(
        s=qident(schema))).fetchone()[0]


def conflict_scan(conn, schema, cfg):
    out = []
    for keycols in cfg["target"]["unique_keys"]:
        kc = sql.SQL(", ").join(qident(c) for c in keycols)
        rows = conn.execute(sql.SQL("""
            SELECT {kc}, count(*) FROM {s}.shadow
            GROUP BY {kc} HAVING count(*) > 1
        """).format(kc=kc, s=qident(schema))).fetchall()
        for r in rows:
            out.append((keycols, r[:-1], r[-1]))
    return out


# --------------------------------------------------------------------------
# 命令实现
# --------------------------------------------------------------------------
def connect(dsn):
    conn = psycopg.connect(dsn, autocommit=True)
    return conn


def cmd_plan(conn, cfg):
    ensure_control_schema(conn, cfg)
    info = check_supported(conn, cfg)
    print(f"[plan] 源表 {cfg['source']['schema']}.{cfg['source']['table']} 依赖盘点:")
    for d in info["deps"]:
        print(f"  - {d}")
    for v in cfg["managed_views"]:
        print(f"  - 受管视图切换后重建: {v['schema']}.{v['name']}")
    if info["problems"]:
        print("[plan] 拒绝：存在不支持的依赖/结构：")
        for p in info["problems"]:
            print(f"  ! {p}")
        raise MigrationError("plan 未通过，start 前必须先处理上述问题")
    print("[plan] 通过：该表属于受管普通表子集，可执行 start。")
    return info


def cmd_start(conn, cfg):
    ensure_control_schema(conn, cfg)
    active = get_active(conn, cfg)
    ch = config_hash(cfg)
    if active:
        mid, mschema, state, old_hash, views, old_name = active
        if old_hash != ch:
            raise MigrationError(
                f"冲突：该源表已有活跃迁移 id={mid} state={state}，"
                "且配置哈希与本次请求不一致；禁止安装第二套捕获器。"
                "请 abort 后再 start，或使用原配置。")
        print(f"[start] 已有迁移身份 id={mid} schema={mschema} state={state}，"
              "直接返回，不重复创建捕获器。")
        return mid
    latest = get_any_latest(conn, cfg)
    if latest and latest[2] not in TERMINAL_STATES:
        raise MigrationError(f"迁移状态异常: {latest}")
    # 门禁在创建任何对象之前
    cmd_plan(conn, cfg)
    with conn.transaction():
        reg = conn.execute(sql.SQL("""
            INSERT INTO {cs}.registry
              (source_schema, source_table, mig_schema, state, config_hash,
               managed_views)
            VALUES (%s,%s,'', 'INIT', %s, %s) RETURNING migration_id
        """).format(cs=qident(cfg["control_schema"])),
            (cfg["source"]["schema"], cfg["source"]["table"], ch,
             json.dumps(cfg["managed_views"], ensure_ascii=False))).fetchone()
        mid = reg[0]
        mschema = create_migration_objects(conn, cfg, mid)
        conn.execute(sql.SQL("""
            UPDATE {cs}.registry SET mig_schema=%s, state='COPYING'
            WHERE migration_id=%s
        """).format(cs=qident(cfg["control_schema"])), (mschema, mid))
    print(f"[start] 已建立迁移身份 id={mid} schema={mschema}；"
          "触发器已安装（本数据库内捕获），存量扫描待 copy。")
    return mid


def require_active(conn, cfg):
    active = get_active(conn, cfg)
    if not active:
        raise MigrationError("该源表没有活跃迁移，请先 start")
    return active


def cmd_copy(conn, cfg):
    mid, mschema, state, *_ = require_active(conn, cfg)
    if state == "READY":
        print(f"[copy] 迁移 {mid} 已 READY，复制与追赶此前已完成。")
        return
    key = advisory_key(cfg)
    conn.execute("SELECT pg_advisory_lock(%s)", (key,))
    try:
        batch = int(cfg["copy"]["batch_size"])
        # 演练用开关：复制指定批数后退出，模拟复制中途崩溃（正常使用不设置）
        import os
        max_batches = int(os.environ.get("MIG_MAX_BATCHES", "0"))
        batches = 0
        src_t = qident(cfg["source"]["schema"], cfg["source"]["table"])
        # 阶段 1：keyset 存量复制（每批一个事务，崩溃只丢当前批）
        while True:
            with conn.transaction():
                meta = get_meta(conn, mschema)
                wm = int(meta["copy_watermark"])
                rows = conn.execute(sql.SQL("""
                    SELECT row_id, amount_text, location_text, note
                    FROM {t} WHERE row_id > %s
                    ORDER BY row_id ASC LIMIT %s
                """).format(t=src_t), (wm, batch)).fetchall()
                if not rows:
                    break
                for rid, amt, loc, note in rows:
                    apply_payload(conn, mschema, cfg, "I",
                                  {"row_id": rid, "amount_text": amt,
                                   "location_text": loc, "note": note}, None)
                set_meta(conn, mschema, {"copy_watermark": rows[-1][0]})
                batches += 1
                if max_batches and batches >= max_batches:
                    print(f"[copy] MIG_MAX_BATCHES={max_batches} 到达，"
                          "演练用提前退出（模拟崩溃点）。")
                    return
                if len(rows) < batch:
                    break
        # 阶段 2：流水追赶 + 双向对账，直到一轮内无新事件且差集为空
        rounds = 0
        while True:
            rounds += 1
            with conn.transaction():
                set_meta(conn, mschema, {"state": "CATCHUP"})
                conn.execute(sql.SQL(
                    "UPDATE {cs}.registry SET state='CATCHUP' WHERE migration_id=%s"
                ).format(cs=qident(cfg["control_schema"])), (mid,))
                applied = drain_change_log(conn, mschema, cfg)
                miss, extra = reconcile(conn, mschema, cfg)
                errs = recheck_errors(conn, mschema, cfg)
                maxseq = conn.execute(sql.SQL(
                    "SELECT coalesce(max(seq),0) FROM {s}.change_log").format(
                    s=qident(mschema))).fetchone()[0]
                cur_applied = int(get_meta(conn, mschema)["applied_seq"])
                if applied == 0 and miss == 0 and extra == 0 \
                        and cur_applied >= maxseq:
                    set_meta(conn, mschema, {"state": "READY"})
                    conn.execute(sql.SQL(
                        "UPDATE {cs}.registry SET state='READY' WHERE migration_id=%s"
                    ).format(cs=qident(cfg["control_schema"])), (mid,))
                    print(f"[copy] 迁移 {mid} 到达 READY：追赶轮次={rounds}，"
                          f"当前行级错误={errs}")
                    break
                print(f"[copy] 追赶第 {rounds} 轮：事件 {applied} 条，"
                      f"补缺 {miss} 行，清多 {extra} 行，错误 {errs} 行")
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s)", (key,))


def cmd_status(conn, cfg):
    active = get_active(conn, cfg)
    if not active:
        latest = get_any_latest(conn, cfg)
        if latest:
            print(f"[status] 最近迁移 id={latest[0]} 终态={latest[2]}，无活跃迁移。")
        else:
            print("[status] 该源表没有任何迁移登记。")
        return
    mid, mschema, state, _, views, old_name = active
    key = advisory_key(cfg)
    conn.execute("SELECT pg_advisory_lock(%s)", (key,))
    try:
        with conn.transaction():
            # 状态查询即复检：修好/删除的源行立刻清除过期错误
            if state in ("CATCHUP", "READY", "COPYING"):
                drain_change_log(conn, mschema, cfg)
                reconcile(conn, mschema, cfg)
                recheck_errors(conn, mschema, cfg)
            meta = get_meta(conn, mschema)
            nsrc = conn.execute(sql.SQL("SELECT count(*) FROM {t}").format(
                t=qident(cfg["source"]["schema"],
                         cfg["source"]["table"]))).fetchone()[0]
            nsh = conn.execute(sql.SQL(
                "SELECT count(*) FROM {s}.shadow").format(
                s=qident(mschema))).fetchone()[0]
            nerr = conn.execute(sql.SQL(
                "SELECT count(*) FROM {s}.row_errors").format(
                s=qident(mschema))).fetchone()[0]
            nlog = conn.execute(sql.SQL(
                "SELECT coalesce(max(seq),0) FROM {s}.change_log").format(
                s=qident(mschema))).fetchone()[0]
            conflicts = conflict_scan(conn, mschema, cfg)
        print(f"[status] migration_id={mid} state={state} schema={mschema}")
        print(f"  源行数={nsrc} 影子行数={nsh} 行级错误={nerr} "
              f"流水最大seq={nlog} 已应用seq={meta.get('applied_seq')} "
              f"复制水位row_id={meta.get('copy_watermark')}")
        if old_name:
            print(f"  旧物理表保留名: {old_name}")
        if conflicts:
            print("  目标唯一键冲突:")
            for kc, vals, cnt in conflicts:
                print(f"    键 {kc} = {vals} 重复 {cnt} 行")
        rows = conn.execute(sql.SQL("""
            SELECT row_id, reason, source_state, at_seq
            FROM {s}.row_errors ORDER BY row_id LIMIT 20
        """).format(s=qident(mschema))).fetchall()
        for rid, reason, src, atseq in rows:
            print(f"  错误 row_id={rid} at_seq={atseq}: {reason}")
            print(f"    源状态={json.dumps(src, ensure_ascii=False)}")
        print("  结论: " + ("可 cutover" if state == "READY" and nerr == 0
                            and not conflicts else "暂不可切换（见上）"))
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s)", (key,))


def cmd_cutover(conn, cfg):
    mid, mschema, state, *_ = require_active(conn, cfg)
    if state not in ("READY",):
        # 允许 copy 未跑完时先自动跑到 READY
        if state in ("COPYING", "CATCHUP", "INIT"):
            print("[cutover] 尚未 READY，先执行 copy/catchup …")
            cmd_copy(conn, cfg)
        else:
            raise MigrationError(f"状态 {state} 不允许 cutover")
    key = advisory_key(cfg)
    conn.execute("SELECT pg_advisory_lock(%s)", (key,))
    try:
        timeout_ms = int(cfg["cutover"]["lock_timeout_ms"])
        sc, tb = cfg["source"]["schema"], cfg["source"]["table"]
        old_name = f"{tb}__old_{mid}"
        try:
            with conn.transaction():
                conn.execute(sql.SQL("SET LOCAL lock_timeout = {}").format(
                    sql.Literal(int(timeout_ms))))
                conn.execute(sql.SQL(
                    "UPDATE {cs}.registry SET state='CUTTING_OVER' "
                    "WHERE migration_id=%s").format(
                        cs=qident(cfg["control_schema"])), (mid,))
                # 短暂源写锁；长复制阶段从不持有它
                conn.execute(sql.SQL(
                    "LOCK TABLE {t} IN SHARE ROW EXCLUSIVE MODE").format(
                    t=qident(sc, tb)))
                # 锁内最后排空与全量核验
                drain_change_log(conn, mschema, cfg)
                reconcile(conn, mschema, cfg)
                nerr = recheck_errors(conn, mschema, cfg)
                conflicts = conflict_scan(conn, mschema, cfg)
                if nerr != 0 or conflicts:
                    raise MigrationError(
                        f"拒绝切换：行级错误 {nerr} 行，唯一冲突 {len(conflicts)} 组。"
                        "修正源数据后可重试 cutover，无需重做全表复制。")
                # 受管视图：先 DROP，表名切换后按新结构重建，避免静默指向旧物理表
                for v in cfg["managed_views"]:
                    conn.execute(sql.SQL("DROP VIEW IF EXISTS {v}").format(
                        v=qident(v["schema"], v["name"])))
                conn.execute(sql.SQL("ALTER TABLE {t} RENAME TO {old}").format(
                    t=qident(sc, tb), old=qident(old_name)))
                conn.execute(sql.SQL("ALTER TABLE {s}.shadow SET SCHEMA {sc}")
                             .format(s=qident(mschema), sc=qident(sc)))
                conn.execute(sql.SQL("ALTER TABLE {sc}.shadow RENAME TO {tb}")
                             .format(sc=qident(sc), tb=qident(tb)))
                for v in cfg["managed_views"]:
                    conn.execute(sql.SQL(v["definition_sql"]))
                # 旧捕获器摘除：旧物理表上不再有任何触发器拦截
                conn.execute(sql.SQL("DROP TRIGGER IF EXISTS trg_shadow_capture "
                                     "ON {old}").format(old=qident(sc, old_name)))
                conn.execute(sql.SQL("DROP FUNCTION IF EXISTS {s}.capture_fn()")
                             .format(s=qident(mschema)))
                conn.execute(sql.SQL("""
                    UPDATE {cs}.registry
                    SET state='DONE', old_physical_name=%s
                    WHERE migration_id=%s
                """).format(cs=qident(cfg["control_schema"])),
                             (f"{sc}.{old_name}", mid))
            print(f"[cutover] 迁移 {mid} 已提交：{sc}.{tb} 现在是新结构，"
                  f"旧物理表保留为 {sc}.{old_name}，视图已绑定新表。")
        except (QueryCanceled, LockNotAvailable) as exc:
            print(f"[cutover] 等锁超时（上限 {timeout_ms}ms），已回滚："
                  "旧表保持可写，迁移保持 READY，可稍后重试。")
            raise MigrationError("lock_timeout") from exc
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s)", (key,))


def cmd_abort(conn, cfg):
    active = get_active(conn, cfg)
    if not active:
        print("[abort] 没有活跃迁移，无需清理。")
        return
    mid, mschema, state, *_ = active
    if state == "DONE":
        raise MigrationError("迁移已 DONE，不允许 abort（新表已是正式表）。")
    key = advisory_key(cfg)
    conn.execute("SELECT pg_advisory_lock(%s)", (key,))
    try:
        with conn.transaction():
            conn.execute(sql.SQL(
                "DROP TRIGGER IF EXISTS trg_shadow_capture ON {t}").format(
                t=qident(cfg["source"]["schema"], cfg["source"]["table"])))
            conn.execute(sql.SQL("DROP SCHEMA {s} CASCADE").format(
                s=qident(mschema)))
            conn.execute(sql.SQL(
                "UPDATE {cs}.registry SET state='ABORTED' WHERE migration_id=%s"
            ).format(cs=qident(cfg["control_schema"])), (mid,))
        print(f"[abort] 迁移 {mid} 的影子表/流水/捕获器已清除，"
              "源业务表与数据原样保留，登记置 ABORTED。")
    finally:
        conn.execute("SELECT pg_advisory_unlock(%s)", (key,))


# --------------------------------------------------------------------------
def main(argv=None):
    ap = argparse.ArgumentParser(description="受管事实表在线影子重编码迁移工具")
    ap.add_argument("command",
                    choices=["plan", "start", "copy", "status",
                             "cutover", "abort"])
    ap.add_argument("--config", required=True)
    ap.add_argument("--dsn", default=None)
    args = ap.parse_args(argv)
    cfg = load_config(args.config)
    dsn = args.dsn or __import__("os").environ.get("MIGRATE_DSN")
    if not dsn:
        raise MigrationError("需要 --dsn 或环境变量 MIGRATE_DSN")
    conn = connect(dsn)
    handlers = {"plan": cmd_plan, "start": cmd_start, "copy": cmd_copy,
                "status": cmd_status, "cutover": cmd_cutover,
                "abort": cmd_abort}
    try:
        handlers[args.command](conn, cfg)
    except MigrationError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"ERROR: 未预期异常 {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
