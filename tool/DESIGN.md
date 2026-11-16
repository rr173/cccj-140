# 受管事实表在线影子重编码迁移工具 — 设计说明

适用范围：项目声明的**普通表子集**（单表、固定 4 列、`row_id bigint` 不可变非空主键），
不追求通用数据库迁移平台。所有变更捕获均在本 PostgreSQL 实例内实现，不依赖逻辑复制槽、
外部复制服务或触发器以外的中间件。

## 1. 总体结构

```
源表 public.fact（旧结构，业务全程可写）
   │  AFTER ROW 触发器 trg_shadow_capture（本工具安装，可随 abort 摘除）
   ▼
shadow_mig.registry                 迁移身份登记（每源仅一条活跃记录）
shmig_<id>.change_log               提交后的行级变更流水（bigserial 全序）
shmig_<id>.shadow                   影子表：新结构 + 目标约束（定点 numeric、region/site、唯一键）
shmig_<id>.row_errors               当前仍失败的行级错误（含原因与源状态快照）
shmig_<id>.meta                     复制水位、change_log 应用位点、阶段状态
```

迁移身份：`registry` 上有部分唯一索引
`UNIQUE(source_schema, source_table) WHERE state NOT IN ('DONE','ABORTED')`，
数据库层面保证同一源表不可能存在两套活跃捕获器。重复 `start` 只返回已有身份与状态，
配置不一致时明确报冲突，不会再装第二个触发器。运行期另用
`pg_advisory_lock(crc32(schema), crc32(table))` 保证只有一个回填/追赶进程，
进程崩溃后会话锁由 PostgreSQL 自动释放。

状态机：`INIT → COPYING → CATCHUP → READY → CUTTING_OVER → DONE`，
异常路径 `→ ABORTED`。每次阶段推进与水位推进都在事务内提交，崩溃后按登记状态与水位继续。

## 2. 行级转换的确定性

转换是源行四元组的纯函数，禁止 `now()`/随机数/外部查询/浮点：

- 金额：严格正则校验文本格式（千分位/小数点由配置声明），用 Python `decimal.Decimal`
  做定点解析，按配置的 scale 与舍入模式（half_even/half_up/half_down/truncate）量化，
  再做声明的范围检查。全程无 float。
- 地点：按配置分隔符做**反斜杠转义状态机**拆分（`\|` 为字面分隔符、`\\` 为字面反斜杠、
  悬空反斜杠报错），固定得到 region/site 两个字段；NULL 输入产生两个 NULL，
  目标列声明 NOT NULL 时记为行错误。
- note 原样透传，row_id 原样保留。

任一规则违反 → 不写影子行，改写 `row_errors(row_id, reason, source_state jsonb, phase)`。
源行被修复后，下一次事件重算或 `status`/cutover 的全量复检会删除该错误；
源行被删除则错误与影子行一起删除。错误不会永久阻塞，也不会阻塞旧表业务写入
（错误只登记在迁移自己的 schema 里，源表上没有任何新增约束）。

## 3. 捕获覆盖区间：为什么没有缺口

1. `CREATE TRIGGER` 是 DDL，需要源表 **AccessExclusiveLock**，与任何 DML 锁冲突。
   触发器创建事务提交的那一刻，所有此前正在访问该表的事务都已结束；
   此后开始（或此前被 DDL 锁阻塞、之后才继续）的每条 INSERT/UPDATE/DELETE 都必然触发触发器。
2. 触发器在**事务提交后**才对其他会话可见：回滚事务连带其 `change_log` 写入一起回滚，
   不可能留下幽灵行；追赶会话 READ COMMITTED 也读不到未提交的流水。
3. 存量快照事务在触发器提交之后才打开。于是任意一行的最终来源只有两类：
   快照读到的版本，或 `change_log` 里的提交事件；两者并集覆盖全过程，且
   `change_log.seq`（bigserial）给出提交事件的全局应用顺序。

不使用 `max(row_id)` 作为完整性证据。收尾一致性用双向反连接裁决：
源表每行必须在影子表（合法）或 `row_errors`（当前非法）中存在；影子表每行必须仍存在于源表。

## 4. 回填与捕获的协调及交错裁定

回填（keyset 批量，`row_id > 水位 ORDER BY row_id LIMIT N`，每批一个事务并提交水位）
与流水追赶不会并发：同一 advisory lock 下先跑完存量复制，再排空 `change_log`，
然后做双向反连接对账；若对账期间又有新 seq，则继续排空再对账，直到一轮内
“无新 seq 且双向差集皆空”才置 `READY`。

| 交错场景 | 裁定 |
|---|---|
| 复制读到旧 A，之后业务把 A 改新 | UPDATE 事件在 catchup 按 seq 覆盖，最终为新值；若复制晚于更新则复制即新值，事件幂等重放同一值 |
| 未复制的 B 在复制前被删 | 快照不可见则复制跳过；DELETE 事件在 catchup 幂等删除 |
| 同 row_id 先删后插 | 流水顺序为 D 后 I，按序应用最终保留重插行 |
| 更新事务回滚 | 触发器写入随事务回滚，无任何痕迹 |
| 迁移中插入负序/更小 row_id | 触发器照常捕获；keyset 漏扫由双向反连接兜底补写 |
| 复制到一半进程崩溃 | 已提交批次的水位/影子/流水位点已落盘；重开后从水位与 seq 位点继续，三者同库事务对齐，不会失配 |

影子写入全部幂等：`INSERT ... ON CONFLICT (row_id) DO UPDATE`，错误按 row_id upsert，
重复应用任意 seq 都收敛到同一状态。

## 5. 唯一键冲突的处理

影子表按配置创建真实的 UNIQUE 索引与 CHECK/NOT NULL 约束。应用行时若撞唯一键，
保存点回滚该行，把**冲突双方**的 row_id、原因和各自源状态登记进 `row_errors`，
旧表业务写入不受影响。源数据修复后（事件或全量复检）冲突消失即自动清除错误并补写影子。
cutover 时再执行一次全表 `GROUP BY 目标键 HAVING count(*)>1` 防御性检查，
错误未清零或冲突仍存在则拒绝切换，已完成的全表复制不重做。

## 6. 依赖门禁（plan/start，动手前拒绝）

通过 `pg_depend`/`pg_trigger`/`pg_constraint`/`pg_class` 枚举：外键（双向）、
触发器、物化视图、非受管视图、规则、分区/继承、行级安全策略、属主序列、
发布/订阅、额外列或非声明类型，一律在创建任何迁移对象之前拒绝。
仅允许：表本身 + 配置显式列出的普通只读视图（relkind='v'、无触发器、
除视图自身 `_RETURN` 规则外无其他规则、表级依赖只指向该源表）。
视图的新结构定义 SQL 在配置中声明，plan 时登记并随身份保存。

## 7. cutover 事务、锁序与超时

全部切换动作在一个事务内，锁序固定为：

1. `SET LOCAL lock_timeout = 配置上限`；
2. `LOCK TABLE 源表 IN SHARE ROW EXCLUSIVE MODE`（阻塞写、允许读）。
   超时 → 回滚，旧表保持可写、状态保持 READY，可稍后重试，长复制阶段从不持此锁；
3. 锁内排空最后一段 `change_log`，做错误全量复检、双向反连接、全表唯一冲突检查；
4. DROP 受管视图 → 源表改名 `<表>__old_<id>` → 影子表 `SET SCHEMA` 后改回原表名 →
   按配置重建视图（按名重建即绑定新表 OID，不会因改名静默指向旧物理表）；
5. 删除旧物理表上的捕获触发器（旧捕获器不再拦截任何写入），登记置 DONE；
6. 提交。提交后原表名即新结构，旧物理表保留供人工检查，不自动删除。

业务写入路径的加锁顺序是“源表行锁 → change_log 插入”，工具路径是
“advisory lock → 源表表锁 → 影子对象”，不存在反向获取，无死锁环。

“用一个事务包起来”本身并不能证明回填期间的交错正确：第 3–5 节的论证依赖的是
触发器的锁语义覆盖、提交可见性、seq 全序、幂等应用与双向对账，事务只负责
切换瞬间的原子性。

## 8. abort

仅允许 DONE 之前。先 DROP 源表触发器，再 DROP 迁移 schema（影子表、流水、错误、函数），
登记置 ABORTED。源表及其数据、业务写入完全不受影响。
