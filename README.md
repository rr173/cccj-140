# 在线影子重编码迁移工具（受管事实表子集）

## 目录

```
tool/
  migrate.py        产品代码：plan/start/copy/status/cutover/abort 六个接口
  config.json       映射与目标声明（金额格式/舍入/范围、地点分隔符、唯一键、受管视图、锁超时）
  DESIGN.md         schema/锁序/捕获/回填协调的完整设计与交错裁定论证
drill/
  drill.py                  端到端确定性演练（多连接、屏障、崩溃、锁超时、冲突、独立核对）
  kill_recovery.py          复制中 SIGKILL 强退 → 重开继续演练
  independent_check.py      独立核对：静止后逐行重算源快照并与目标全量比对（不 import 产品代码）
  run_all.sh                一键运行全部演练
  logs/drill_evidence.txt   最近一次主演练的完整命令与输出证据
```

## 环境（本仓库内的用户态实例，无需 root）

- PostgreSQL 15.20 解包在 `/workspace/pg`，数据目录 `/workspace/pgdata`，监听 `127.0.0.1:55432`
- Python 3.11 + psycopg 3.3.9 装在 `/workspace/pylibs`
- 所有 Python 用 `python3 -I` 运行，依赖目录由环境变量 `MIG_PYLIBS` 显式声明

## 六个接口

```bash
export MIGRATE_DSN="host=127.0.0.1 port=55432 user=node dbname=<db>"
export MIG_PYLIBS=/workspace/pylibs/lib/python3.11/site-packages
python3 -I tool/migrate.py plan     --config tool/config.json   # 依赖门禁与盘点，不改任何对象
python3 -I tool/migrate.py start    --config tool/config.json   # 建迁移身份+触发器捕获，再可 copy
python3 -I tool/migrate.py copy     --config tool/config.json   # keyset 存量复制+流水追赶+对账，可反复/重开
python3 -I tool/migrate.py status   --config tool/config.json   # 水位/错误(含源行与源状态)/冲突/结论，顺带复检
python3 -I tool/migrate.py cutover  --config tool/config.json   # 事务内锁等待上限内切换名称/视图/登记
python3 -I tool/migrate.py abort    --config tool/config.json   # 只清本迁移对象，业务表原样可用
```

进程在任意阶段被 kill -9 后，直接重新执行同一命令即可继续：水位、捕获位点、
影子状态都在同一数据库事务内推进，advisory lock 由 PostgreSQL 在后端退出时自动释放。

## 关键设计要点（详见 DESIGN.md）

- 捕获仅用本库内的 `AFTER ROW` 触发器 + `change_log(bigserial)` 全序流水；
  回滚事务的触发器写入随事务回滚，无幽灵行；不使用任何外部复制服务。
- 触发器 DDL 的表锁语义 + 晚于触发器提交的快照事务，保证捕获覆盖无缺口；
  不把 `max(row_id)` 当完整性证明，收尾用双向反连接对账。
- 转换是源行纯函数：`Decimal` 定点量化（half_even/half_up/half_down/truncate）、
  反斜杠转义状态机拆分地点，禁止时间/随机/外部查询/浮点。
- 行级错误与唯一冲突只登记在迁移 schema，不阻塞旧表合法写入；源行修复或删除后，
  下一次事件/status/cutover 复检即清除过期错误。
- 同一源表仅允许一个活跃迁移：registry 部分唯一索引 + advisory lock；
  重复 start 返回同一身份，配置不一致明确报冲突。
- cutover 单事务、固定锁序、`lock_timeout` 可配；超时回滚后旧表保持可写、迁移保持 READY。
- 受管只读视图切换时 DROP 后按新结构重建，按名绑定新表 OID；外键/物化视图/触发器/
  规则/分区/RLS/发布等未支持依赖在 plan/start 建任何对象之前拒绝。

## 一键演练

```bash
bash drill/run_all.sh
```

演练覆盖（断言全部 PASS 才算通过）：门禁拒绝、重复 start 幂等与配置冲突、
复制读到旧 A 后更新、未复制 B 先删、同 row_id 删后重插、回滚更新无幽灵、
迁移中负序/小 row_id 补写、坏行错误登记与修复清除、两个源行映射同一目标键时
旧表照常写但 cutover 被拒、修复后免重做复制直接切换、锁超时回滚、
SIGKILL 强退重开收敛、切换后两连接核对名称/登记/捕获器/视图绑定、
独立脚本逐行重算核对与手写预期值。
