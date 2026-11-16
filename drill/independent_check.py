#!/usr/bin/env python3
"""独立核对脚本（不 import 产品回填代码）。

在写入静止后：
  1. 对源表做事务快照，逐行独立实现金额定点化与地点转义拆分；
  2. 与目标表按 row_id 全量比对（amount 用 Decimal 字符串精确比对，无浮点）；
  3. 输出不通过行并以退出码表达结果。
"""
import argparse
import json
import os
import sys
_li = os.environ.get("MIG_PYLIBS")
if _li:
    sys.path.insert(0, _li)
from decimal import Decimal, ROUND_HALF_EVEN, localcontext

import psycopg


def parse_amount(raw):
    if raw is None:
        return None
    t = raw.replace(",", "")
    assert re_match(t), f"格式不符: {raw!r}"
    with localcontext() as ctx:
        ctx.prec = 50
        return Decimal(t).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)


def re_match(t):
    import re
    return re.match(r"^-?[0-9]+(\.[0-9]+)?$", t) is not None


def split_location(raw):
    if raw is None:
        return None, None
    out, cur, esc = [], [], False
    for ch in raw:
        if esc:
            assert ch in ("/", "\\"), f"非法转义: {raw!r}"
            cur.append(ch)
            esc = False
        elif ch == "\\":
            esc = True
        elif ch == "/":
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    assert not esc, f"悬空反斜杠: {raw!r}"
    out.append("".join(cur))
    assert len(out) == 2, f"拆分段数={len(out)}: {raw!r}"
    return out[0], out[1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--source", default=None,
                    help="源表（旧结构）；不指定时从 DONE 登记自动取旧物理表")
    ap.add_argument("--expect-fail", action="store_true",
                    help="源中存在注定转换失败的行时，核对它们不在目标即可")
    args = ap.parse_args()
    conn = psycopg.connect(args.dsn, autocommit=True)
    source = args.source
    if not source:
        row = conn.execute(
            "SELECT old_physical_name FROM shadow_mig.registry "
            "WHERE state='DONE' ORDER BY migration_id DESC LIMIT 1").fetchone()
        if row and row[0]:
            source = row[0]
            print(f"[check] 检测到已切换，源快照取自旧物理表 {source}")
    if not source:
        raise SystemExit("[check] 需要 --source 或存在 DONE 登记")
    with conn.transaction():
        src = conn.execute(
            f"SELECT row_id, amount_text, location_text, note FROM {source} "
            "ORDER BY row_id").fetchall()
        tgt = conn.execute(
            "SELECT row_id, amount, region, site, note FROM public.fact "
            "ORDER BY row_id").fetchall()
    expected, bad = {}, []
    for rid, amt, loc, note in src:
        try:
            a = parse_amount(amt)
            r, s = split_location(loc)
        except AssertionError as exc:
            bad.append((rid, str(exc)))
            continue
        expected[rid] = (a, r, s, note)
    got = {r[0]: (r[1], r[2], r[3], r[4]) for r in tgt}
    errors = []
    for rid, exp in expected.items():
        if rid not in got:
            errors.append(f"目标缺少 row_id={rid}")
            continue
        g = got[rid]
        if exp[0] is not None and Decimal(str(g[0])) != exp[0]:
            errors.append(f"row_id={rid} amount 期望 {exp[0]} 实际 {g[0]}")
        if (g[1], g[2], g[3]) != (exp[1], exp[2], exp[3]):
            errors.append(f"row_id={rid} region/site/note 期望 "
                          f"{exp[1:]} 实际 {g[1:]}")
    extra = sorted(set(got) - set(expected))
    if extra:
        errors.append(f"目标多出 row_id={extra}")
    print(f"[check] 源快照 {len(src)} 行，独立转换成功 {len(expected)} 行，"
          f"源内坏行 {len(bad)} 行，目标 {len(got)} 行")
    for rid, why in bad:
        print(f"[check] 源坏行（独立实现同样拒绝）row_id={rid}: {why}")
    if args.expect_fail:
        present_bad = [rid for rid, _ in bad if rid in got]
        if present_bad:
            errors.append(f"坏行不应进入目标: {present_bad}")
    elif bad:
        errors.append(f"未声明 expect-fail 但源存在坏行: {[b[0] for b in bad]}")
    if errors:
        for e in errors:
            print("[check] MISMATCH:", e)
        print("[check] 结论: 不通过")
        return 1
    print("[check] 结论: 通过，目标与独立重算的源快照逐行一致。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
