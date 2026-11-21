# 排水管网增量同步与上游追溯

值班员接到积水报告后，用本服务回答：**哪些接点、哪些管段会把水引到事发接点，依据的是哪些工程改接批次。**

实现语言 Python 3.11，存储 SQLite，查询接口为本地 HTTP。无第三方依赖。

```
app/
  jsonstream.py   大日志片段的分块 JSON 读取（单条占位内存，不整体载入）
  ids.py          接点/管段编号语法（旧号 J-000123、片区号 Q4:00123）
  wire.py         日志片段格式校验（整段校验通过才可能被确认）
  db.py           SQLite 结构与连接（WAL；读连接 mode=ro；含历史版本表与触发器）
  archive.py      接收：校验 → 落盘（内容寻址）→ 写入暂存表，与"确认"同一次提交
  snapshot.py     按批次顺序把完整批次应用到已发布管网（单事务、可回滚）
  history.py      逐批次历史：发布时记录版本；现有库用已落地数据在线补建
  tracing.py      沿水流方向做上游可达性追溯（最新快照或指定批次）
  server.py       本地 HTTP 查询
  service.py      进程内编排（接收 → 发布 → 状态）
  replay.py       离线回放源 + 示例数据生成
  __main__.py     命令行：sync / serve / status / trace / backfill
tests/            64 个 unittest 用例（标准库，无需安装）
examples/         demo.py、sample_batches/（示例日志）
```

## 1. 启动

```bash
# 生成示例日志（已随仓库提供在 examples/sample_batches/，可重新生成）
python3 -m app.replay --out examples/sample_batches

# 接收并应用：把目录里的 *.json 片段逐个投递（可反复运行，重复投递无副作用）
python3 -m app sync  --db /tmp/drain.db --data /tmp/drain-data \
                     --from-source examples/sample_batches

# 起本地查询服务
python3 -m app serve --db /tmp/drain.db --data /tmp/drain-data --port 8787

# 命令行直接查询（不必起 HTTP 服务）
python3 -m app trace --db /tmp/drain.db --ref Q4:01012 --format text
python3 -m app status --db /tmp/drain.db
```

## 2. 一条测试命令（80 秒内输出可读结果）

```bash
timeout 80 python3 examples/demo.py
```

它会：生成示例源 → 乱序+重复投递 → 打印发布状态 → 起 HTTP 服务 →
用**旧号**和**片区号**各查一次同一接点（结果一致）→ 打印值班员可读的上游追溯清单
（含"依据批次"）→ 按**指定历史批次**各查一次 → 模拟"老库无历史"并**在线补建**
（补建前 409，补建后可查）。全程约 3 秒。完整测试套件：`python3 tests/run_all.py`（约 15 秒）。

## 3. 日志片段格式

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

## 4. 接收、确认与崩溃恢复

* 每个片段独立确认。确认 = 该片段的**落盘文件 + 暂存行 + 片段记录**在同一次 SQLite
  事务里提交；提交之后源端即可清理副本。
* 进程在提交后立刻被 `kill -9`：重启后该片段仍是"已确认"，不会要求重传
  （`tests/test_receive.py::test_ack_survives_hard_kill_and_restart`）。
* 进程在提交前被杀死：事务回滚，暂存表和片段记录都不存在，源端重传即可
  （`test_crash_during_staging_leaves_no_partial_state`）。
* 重复投递：按**内容摘要**去重——字节相同、或字节不同但内容等价（如缩进不同）都
  回答 `duplicate` 且 `acknowledged=false`，不重复写入。
* 片段可乱序到达；同一批次可以等多片到齐才发布。并发投递安全
  （`test_concurrency.py`）。

## 5. 按批次生效

* 只有"下一顺位"的批次完整（`parts` 全部到齐，`total` 给出源端批次数）才会被应用。
* 应用在**一个写事务**内完成；读连接永远看不到半个批次。
* 应用失败（数据与已有管网矛盾，例如管段指向不存在的接点）→ 回滚并标记
  `rejected`，保留错误原因；**上一次完整发布的管网继续可查**，后续批次被阻塞直到
  人工处理（`status` 会给出 blocker）。
* 有未完成批次时，查询照常返回最近一次完整发布的结果，响应里标明
  `snapshot.published_batch`（`test_queries_stay_available_while_a_big_batch_is_applied`）。

## 6. 新老编号并存

* 接点内部用稳定 `uid`；管段只引用 `uid`，所以**既有管段无需改写**，升级后仍按实际
  连接参与追溯。
* `ids` 表把旧号与片区号都挂到同一 `uid`（`joint_refs` 表，`ref` 唯一），
  因此 `J-001012` 与 `Q4:01012` 解析到同一接点，查询结果完全相同
  （`test_trace.py::test_legacy_and_zoned_spellings_resolve_to_one_joint`）。
* 升级批次之后仍可用旧号写入新的改接（`test_new_pipes_may_be_written_with_old_ids`）。
* 同一片区号在不同片区是不同接点（`test_same_local_number_in_two_zones_stays_two_joints`）。
接点整体迁到另一片区时 `uid` 与既有管段不变，旧片区号失效、旧号继续可用
（`test_joint_moved_to_another_zone_keeps_its_uid_and_pipes`）。

## 7. 查询接口

```
GET /health
GET /v1/snapshot                      当前已发布快照摘要
GET /v1/batches                       所有批次：seq、状态、已收分片、错误
GET /v1/batches/<batch_id>            单批次：片段清单 + 该批次实际产生的效果计数
GET /v1/history                       逐批次历史的构建状态（水位线、是否补齐）
GET /v1/joint?ref=J-001012[&batch=B-0003]
                                      解析一个编号（返回 uid、旧号、片区号、别名）；
                                      带 batch 时按该批次的编号解析
GET /v1/trace/upstream?ref=Q4:01012[&batch=B-0003][&format=text][&limit=50000][&include_retired=1]
```

`batch` 参数（可选）：**按指定已发布批次回答**——接点编号、管段流向、批次依据都取
该批次发布完成那一刻的状态（见第 8 节）。未指定时查最近一次完整发布的管网。
错误码：缺 `ref` → 400；编号无法解析/未知 → 404；批次未知 → 404；批次未发布或
其历史尚未补齐 → **409（绝不返回部分结果）**；结果超过 `limit` → 413。

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

## 8. 按批次回溯（历史追溯）

`batch` 参数把同一个追溯问题问向过去：**该批次发布完成那一刻**的管网什么样，
答案就什么样——

* **接点编号**按该批次解析：升级前只有旧号可解析，升级后新旧号并存；迁区后旧片区号
  失效。显示名（`display`）同样是当时的写法。
* **管段流向**按该批次：之后批次的改向、停用不影响当时的状态。
* **批次依据**：`basis.published_batch` 就是所指定的批次，`referenced_batches` 只引用
  不超过它的批次。

实现是三张**时态版本表**（`hist_joints` / `hist_pipes` / `hist_refs`）：每行是一个
接点/管段/编号的一段有效期 `[from_seq, to_seq)`。批次应用时由触发器在同一事务里
记录**实际发生变更**的行（只改 `last_seq` 的触碰不产生版本），因此——

* **存储增量随实际变更增长**：一个只改接一根管段的小批次只增加一两行版本，
  不会按批次复制整张管网；
* **查询不从头回放**：按批次查询是对版本表的索引范围扫描
  （`from_seq <= N AND (to_seq IS NULL OR to_seq > N)`），与批次总数无关；
  即使把已落地的片段文件和暂存表全部删除，历史查询结果也不变
  （`test_queries_do_not_replay_landed_data`）。

**发布时记录**：水位线 `meta.history_upto_seq` 标记"历史完整到哪个 seq"。发布批次
N 时若水位线正好是 N-1，就在同一事务里记录该批次的版本并把水位线推进到 N——
新库从第一个批次起就自带完整历史，补建完成后新发布的批次也立即可回查。

**现有库在线补建**：老库（或补建期间又发布了新批次的库）用**已落地数据**重建历史——
把每个已应用批次的暂存行按 seq 顺序回放到一个临时库（走的是与正式发布完全相同的
`apply_batch` 代码，并用 `batch_effects` 逐批校验回放结果一致），再把版本行**按 seq
逐批**拷回正式库，每批一个小事务：

* 期间现网查询（WAL 读者）和新批次发布照常进行；回放阶段不持有正式库写锁；
* 水位线只在"该 seq 的版本行完整提交"的同一事务里推进，发布与补建谁在
  N-1 → N 上先到谁写，**不会重复也不会各写一半**；
* 历史未补齐的批次查询返回 409，**绝不返回部分结果**；补齐之前的现网查询不受
  影响（`test_backfill_while_publishing_and_querying_continue`）。

```bash
python3 -m app backfill --db /tmp/drain.db --verbose   # 可反复运行，只补缺口
python3 -m app trace --db /tmp/drain.db --ref J-001012 --batch B-0002
```

## 9. 离线回放源

`app/replay.py` 模拟源端日志系统：

* `generate_demo(dir)`：写出示例批次（含 2 片的大批次、升级批次、混合写法批次、
  停用批次、以及源端只交付了一半的最后一批）；
* `deliver(source_dir, sync_service)`：可指定投递顺序、按概率重复投递，模拟乱序/重复；
* `write_streamed_segment(...)`：逐条写超大批次，供容量测试使用；
* 命令行：`python3 -m app.replay --out DIR`（只生成）或 `--deliver-to DB --data DIR`
  （生成并投递）。

## 10. 测试

```bash
python3 tests/run_all.py            # 64 个用例，约 15 秒
python3 tests/run_all.py 2>/dev/null | tail -3
```

覆盖面：乱序/重复/冲突片段、非法 JSON 与非法字段、`kill -9` 后恢复、发布中途失败回滚、
超大批次（4 万接点+4 万管段、约 7 MB）在 96 MiB 数据段上限下接收并应用、应用期间并发查询、
新旧编号等价、改向/停用的追溯语义、HTTP 状态码、离线回放与命令行全流程，
以及逐批次历史：按批次的编号/流向/依据、跨编号升级·迁区·改向·停用的离线批次验证、
补建期间发布与查询并发、未补齐不返回部分结果、补建结果与实时记录逐批一致、
存储增量只随实际变更增长、删除落地数据后历史查询不变。
