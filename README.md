# 排水管网增量同步与上游追溯

值班员接到积水报告后，用本服务回答：**哪些接点、哪些管段会把水引到事发接点，依据的是哪些工程改接批次。**

实现语言 Python 3.11，存储 SQLite（**接收库 / 发布库两个独立文件**），查询接口为本地 HTTP。无第三方依赖。

```
app/
  jsonstream.py   大日志片段的分块 JSON 读取（单条占位内存，不整体载入）
  ids.py          接点/管段编号语法（旧号 J-000123、片区号 Q4:00123）
  wire.py         日志片段格式校验（整段校验通过才可能被确认）
  db.py           两套 SQLite 结构与连接（WAL；读连接 mode=ro）
  archive.py      接收：校验 → 落盘（内容寻址）→ 写入接收库暂存表，与"确认"同一次提交
  snapshot.py     按批次顺序把完整批次从接收库应用到发布库（单事务、可回滚、崩溃后对账）
  migrate.py      旧单库 → 接收库+发布库 的迁移与逐域校验（快照/编号映射/拓扑/批次依据）
  tracing.py      沿水流方向做上游可达性追溯（只读发布库）
  server.py       本地 HTTP 查询（只依赖发布库；接收库停了查询照常）
  service.py      进程内编排（接收 → 发布 → 状态）
  replay.py       离线回放源 + 示例数据生成
  __main__.py     命令行：sync / serve / status / trace / migrate
tests/            54 个 unittest 用例（标准库，无需安装）
examples/         demo.py、splitcheck.py（离线验收）、sample_batches/（示例日志）
```

## 1. 两个独立的数据库

* **接收库 inbox**（改接日志接收）：片段落盘记录、暂存表、批次接收进度。
  确认 = 落盘文件 + 暂存行 + 片段记录在接收库的同一次事务里提交。
* **发布库 publish**（管网发布）：已发布管网（接点、管段、编号映射）、批次效果、
  发布台账 `published_batches`。每个批次在发布库的一个事务里生效。

由此得到三件事：**接收库一停，HTTP 查接点编号、上游、依据批次照常**（它们只读发布库）；
**发布大批次期间，后续片段照常拿到确认**（接收不写发布库，不被长事务堵住）；
**小批次的写入量只随改动规模增长**（发布库按批次增量改写，不整网重写）。

## 2. 启动

```bash
# 生成示例日志（已随仓库提供在 examples/sample_batches/，可重新生成）
python3 -m app.replay --out examples/sample_batches

# 接收并应用：把目录里的 *.json 片段逐个投递（可反复运行，重复投递无副作用）
python3 -m app sync  --inbox-db /tmp/drain-inbox.db --publish-db /tmp/drain-publish.db \
                     --data /tmp/drain-data --from-source examples/sample_batches

# 起本地查询服务（--inbox-db 可选：只影响 /v1/batches 的接收进度展示）
python3 -m app serve --publish-db /tmp/drain-publish.db \
                     --inbox-db /tmp/drain-inbox.db --port 8787

# 命令行直接查询（不必起 HTTP 服务）
python3 -m app trace --publish-db /tmp/drain-publish.db --ref Q4:01012 --format text
python3 -m app status --publish-db /tmp/drain-publish.db --inbox-db /tmp/drain-inbox.db
```

### 从旧的单库迁移

```bash
python3 -m app migrate --db /tmp/drain.db \
       --inbox-db /tmp/drain-inbox.db --publish-db /tmp/drain-publish.db
```

迁移只读旧库、写入两个新库，并逐项校验：**快照、编号映射、拓扑、批次依据**必须与旧库
一致；**已收齐未发布**的批次从暂存续跑发布，**已确认但仍缺片**的批次保留已收分片、
只等缺片到齐。目标文件已存在时拒绝覆盖（`--force` 强制）。

## 3. 一条测试命令（80 秒内输出可读结果）

```bash
timeout 80 python3 examples/splitcheck.py
```

本地夹具、全程离线，覆盖：**旧库迁移**（四处一致 + 断点续跑）、**发布期间确认片段**
（大批次发布 1.2s 期间新片段毫秒级拿到可恢复确认；改一处属性的小批写入几十 KiB，
不随全网体量放大）、**强停恢复**（`kill -9` 后确认不丢、状态与可查询版本一致、
效果只生效一次）、**接收库停止时 HTTP 查询不断线**。全程约 12 秒。

另有一条端到端演示：`timeout 80 python3 examples/demo.py`（约 3 秒）。
完整测试套件：`python3 tests/run_all.py`（约 25 秒）。

## 4. 日志片段格式

一个片段是一个 JSON 对象；其数组字段可达千万级，服务按元素流式处理，
**单批日志可以远大于同步进程可用内存**。

```json
{
  "segment": "B-0002-p0",   // 片段 id，源端唯一
  "batch":   "B-0002",      // 所属工程改接批次
  "seq":     2,             // 批次在源端的顺序（1 起）
  "total":   6,             // 源端一共有多少批次——据此知道是否齐全
  "part":    0,             // 本片段是该批次的第几片
  "parts":   1,             // 该批次一共几片
  "upgrade": false,         // 该批次是否引入片区编号（全市唯一 → 片区内唯一）
  "producer": "offline-replay",
  "sent_at":  "2026-11-19T00:00:00Z",

  "ids":       [{"old": "J-004521", "new": "C4:04521"}],
  "taps":      [{"ref": "J-001030", "zone": "C4", "local": "01030",
                 "attrs": {"kind": "污水"}}],
  "lays":      [{"code": "P-090100", "from": "J-001030", "to": "J-001010",
                 "attrs": {"dn": 400}}],
  "redirects": [{"from": "J-001099", "to": "J-001012"},
                {"code": "P-090002", "to": {"from": "J-001020", "to": "J-001011"}}],
  "retires":   [{"kind": "joint", "ref": "J-001099"},
                {"kind": "pipe", "code": "P-090003"}]
}
```

| 字段 | 含义 |
|---|---|
| `ids` | 新旧编号对照，**从该批次起**两种写法都指同一接点 |
| `taps` | 新增或更新接点；`ref` 可用任一写法，`zone`/`local` 显式给出升级后身份；`attrs` 合并 |
| `lays` | 敷设管段（新增）；同一 `code` 再次出现即**改向**（端点被替换） |
| `redirects` | `from` 为接点：该接点出发的**所有**在用管段改从 `to` 出发；`from` 为管段号：该管段端点整体改为 `{"from","to"}` |
| `retires` | 停用接点（连带停用它两端的在用管段）或停用管段 |

规则：**任意元素校验失败 → 整个片段不确认**，暂存表无残留。未知字段同样拒绝
（避免"改了但没生效"的静默错误）。管段号以 `P-` 开头，接点号以 `J-`/片区号带冒号，
两者不会混淆。

## 5. 接收、确认与崩溃恢复

* 每个片段独立确认。确认 = 该片段的**落盘文件 + 暂存行 + 片段记录**在**接收库**同一次
  事务里提交；提交之后源端即可清理副本。确认不依赖发布库：发布暂停、崩溃或正在
  应用大批次，确认都照常成立（即可恢复的确认）。
* 进程在提交后立刻被 `kill -9`：重启后该片段仍是"已确认"，不会要求重传
  （`tests/test_receive.py::test_ack_survives_hard_kill_and_restart`）。
* 进程在提交前被杀死：事务回滚，暂存表和片段记录都不存在，源端重传即可
  （`test_crash_during_staging_leaves_no_partial_state`）。
* 重复投递：按**内容摘要**去重——字节相同、或字节不同但内容等价（如缩进不同）都
  回答 `duplicate` 且 `acknowledged=false`，不重复写入。
* 片段可乱序到达；同一批次可以等多片到齐才发布。并发投递安全
  （`test_concurrency.py`）。

## 6. 按批次生效（两库一个提交点）

* 只有"下一顺位"的批次完整（`parts` 全部到齐，`total` 给出源端批次数）才会被应用。
* 应用 = 从接收库读暂存行，在**发布库的一个写事务**里改网；该事务同时写入
  `published_batches` 台账与 `applied_seq`，所以**台账永远和可查询的管网一致**，
  读连接永远看不到半个批次。随后才用第二个小事务把接收库的批次行标记为
  `applied`。
* 在两个事务之间被 `kill -9`：发布库已提交、接收库还停在 `staging`。重启时
  `reconcile` 以发布库为准对齐接收库；由于应用以 `seq == applied_seq + 1` 和台账的
  `UNIQUE(seq)` 为门槛，**已提交的改接效果只生效一次**
  （`tests/test_split.py::ReconcileTest`、`HardKillRecoveryTest`）。
* 应用失败（数据与已有管网矛盾，例如管段指向不存在的接点）→ 回滚并在两库标记
  `rejected`，保留错误原因；**上一次完整发布的管网继续可查**，后续批次被阻塞直到
  人工处理（`status` 会给出 blocker）。
* 有未完成批次时，查询照常返回最近一次完整发布的结果，响应里标明
  `snapshot.published_batch`（`test_queries_stay_available_while_a_big_batch_is_applied`）。
* 发布是**增量**的：只改一处接点属性的小批，新增发布数据与写入页数随改动规模增长，
  不随全网体量放大（`tests/test_split.py::WriteVolumeTest`）。

## 7. 新老编号并存

* 接点内部用稳定 `uid`；管段只引用 `uid`，所以**既有管段无需改写**，升级后仍按实际
  连接参与追溯。
* `joint_refs` 表把旧号与片区号都挂到同一 `uid`（`ref` 唯一），
  因此 `J-001012` 与 `Q4:01012` 解析到同一接点，查询结果完全相同
  （`test_trace.py::test_legacy_and_zoned_spellings_resolve_to_one_joint`）。
* 升级批次之后仍可用旧号写入新的改接（`test_new_pipes_may_be_written_with_old_ids`）。
* 同一片区号在不同片区是不同接点（`test_same_local_number_in_two_zones_stays_two_joints`）。
接点整体迁到另一片区时 `uid` 与既有管段不变，旧片区号失效、旧号继续可用
（`test_joint_moved_to_another_zone_keeps_its_uid_and_pipes`）。

## 8. 查询接口

```
GET /health
GET /v1/snapshot                      当前已发布快照摘要
GET /v1/batches                       所有批次：seq、状态、已收分片、错误
GET /v1/batches/<batch_id>            单批次：片段清单 + 该批次实际产生的效果计数
GET /v1/joint?ref=J-001012            解析一个编号（返回 uid、旧号、片区号、别名）
GET /v1/trace/upstream?ref=Q4:01012[&format=text][&limit=50000][&include_retired=1]
```

`/v1/snapshot`、`/v1/joint`、`/v1/trace/upstream` 只读**发布库**；`/v1/batches*`
在接收库可用时并入接收进度，接收库停止时降级为发布台账视图（响应里
`inbox: "unavailable"`），不报错。

追溯结果（JSON）：

```jsonc
{
  "query":   {"ref": "Q4:01012", "resolved": {"uid": 12, "legacy_id": "J-001012", ...}},
  "snapshot": {"published_batch": "B-0005", "applied_seq": 5, ...},
  "totals":  {"upstream_joints": 13, "upstream_pipes": 15, "max_depth": 5,
              "retired_pipes_not_followed": 2},
  "joints":  [{"display": "Q4:01012", "legacy_id": "J-001012", "depth": 0,
               "batch": "B-0003", ...}],          // 按由近及远排序
  "pipes":   [{"code": "P-001001", "from": "Q4:00001", "to": "Q4:00002",
               "batch": "B-0001", ...}],          // 水流 from -> to
  "basis":   {"published_batch": "B-0005", "published_seq": 5,
              "referenced_batches": [{"batch": "B-0001", "seq": 1,
                                      "joints": 0, "pipes": 8}, ...]}
}
```

追溯语义：管段 `from -> to` 表示水流方向，因此"能到达 X 的上游"＝所有 `to` 属于
X 或 X 上游的**在用**管段；已停用管段不参与（不计入结果，`include_retired=1` 可查看
数量），改向过的管段只按当前端点参与——即"按实际连接"。
错误码：缺 `ref` → 400；编号无法解析/未知 → 404；结果超过 `limit` → 413。

## 9. 离线回放源

`app/replay.py` 模拟源端日志系统：

* `generate_demo(dir)`：写出示例批次（含 2 片的大批次、升级批次、混合写法批次、
  停用批次、以及源端只交付了一半的最后一批）；
* `deliver(source_dir, sync_service)`：可指定投递顺序、按概率重复投递，模拟乱序/重复；
* `write_streamed_segment(...)`：逐条写超大批次，供容量测试使用；
* 命令行：`python3 -m app.replay --out DIR`（只生成）或
  `--deliver --inbox-db I --publish-db P --data DIR`（生成并投递）。

## 10. 测试

```bash
python3 tests/run_all.py            # 54 个用例，约 25 秒
python3 tests/run_all.py 2>/dev/null | tail -3
```

覆盖面：乱序/重复/冲突片段、非法 JSON 与非法字段、`kill -9` 后恢复、发布中途失败回滚、
超大批次（4 万接点+4 万管段、约 7 MB）在 96 MiB 数据段上限下接收并应用、应用期间并发查询、
新旧编号等价、改向/停用的追溯语义、HTTP 状态码、离线回放与命令行全流程，
以及拆分专项：旧库迁移四处一致与断点续跑、发布期间确认片段、写入量随改动规模、
接收库停止时查询不断线、发布中 `kill -9` 后效果只生效一次。
