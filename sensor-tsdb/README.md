# sensor-tsdb

传感器测试平台：把高频数值压缩成**可随机查询的不可变文件段**。
Rust + Axum 提供 API，SQLite 管理序列目录，本地磁盘保存段文件。无前端。

```
┌────────────┐   写入    ┌──────────────┐  tmp+fsync+rename  ┌─────────────┐
│  Axum API  │ ────────► │ 段编码(内存) │ ─────────────────► │ 不可变段文件 │
│            │           └──────────────┘                    └─────────────┘
│            │ ◄──────── ┌──────────────┐   state=sealed     ┌─────────────┐
└────────────┘  区间查询  │ SQLite 目录  │ ◄───────────────── │  两阶段提交  │
                        └──────────────┘                    └─────────────┘
```

## 构建与运行

```bash
cargo build
cargo test                 # 25 个单元/集成测试
DATA_DIR=./data PORT=3000 cargo run
```

快速体验：

```bash
curl -X POST localhost:3000/api/seed          # 生成样本并逐位核对
curl localhost:3000/api/series                 # 序列列表
curl "localhost:3000/api/series/1/query?from=1700000000500&to=1700000000599"
```

## 数据模型与编码

### 时间戳：显式增量编码（delta-of-delta）

平台统一使用 i64 毫秒时间戳（编码层对单位无感知）。每个数据块：

- 块头原样保存块内第一个时间戳（i64 LE）；
- 第二个点存 `delta = ts[1] - ts[0]`（zigzag varint）；
- 之后每个点存 `dod = delta[i] - delta[i-1]`（zigzag varint）。

等间隔数据每点约 1 字节；间隔抖动只影响 dod 大小，不影响正确性。

### 浮点值：按位保存 / 可验证 XOR，绝不量化

所有值一律按 **IEEE-754 位模式（u64）** 处理，两种编码都逐位无损：

- `raw`：每值 8 字节原样存放；
- `xor`：Gorilla 风格 XOR。首值 64 位原样；后续值与前值异或——
  异或为 0 写 1 bit；否则写控制位 + 前导零(5bit，截断到 31) +
  有效长度(6bit，64 编码为 0) + 有效位，可复用上一窗口。

**NaN、负零、无穷的处理（显式承诺）**：

- 存储层只搬运位模式，**不做任何量化、舍入或正规化**。安静 NaN、
  带负载 NaN（如 `0x7ff8000000000001`）、`-0.0`、`±inf`、次正规数
  全部逐位还原（有测试 `xor_special_values_bit_exact` 锁定）。
- 写入 API 中 value 可以是：JSON 数值、`"NaN"` / `"Infinity"` /
  `"-Infinity"` / `"-0.0"` 字符串、或 `{"bits":"0x..."}` 直接给位模式。
  JSON 数值经 serde_json 按 IEEE 最近舍入解析一次（JSON 本身的语义），
  此后不再有任何精度损失。
- 读取 API 中每个点同时返回 `value`（有限值为数值，非有限值为字符串）
  和 `bits`（16 位十六进制），调用方可逐位核对。

### 段文件格式（小端，详见 `src/segment.rs` 头注释）

```
Header(64B, 带 CRC) │ 数据块×N(每块≤256点, 块级 CRC) │ 块索引 │ Footer(16B, 索引CRC+全文件CRC)
```

索引条目 = `(first_ts, file_offset, block_len, crc)`，按时间有序。

## 区间查询：索引定位，绝不整段解压

1. SQLite 找出与 `[from,to]` 重叠的 `sealed` 段；
2. 每段只读 64B 头 + 16B 尾 + 索引区（全部带 CRC 校验）；
3. 在索引上二分定位重叠块，**只读取并解压这些块**。

响应中的 `blocks_read / blocks_total` 可直接验证：对 1ms 序列（40 块）
查 100 个点只读 2 块。

## 重复时间戳规则（显式）

- **批次内**：相同 ts 保留最后一条（keep-last），响应带 `duplicates_dropped`；
- **跨批次**：批次最小 ts 必须严格大于该序列已封存数据的最大 ts，
  否则整体拒绝（409，错误信息中写明规则）。不存在静默覆盖。

## 校验与损坏定位

`POST /api/segments/:id/verify` 逐块校验 CRC 并完整解码，返回：

- `header_ok / index_ok / footer_ok / file_crc_ok` 分级状态；
- 每块 `ok / error`；
- **`corrupt_ranges`**：损坏块对应的时间范围 `[from_ts, to_ts]`，
  可直接用于定位与重采。

校验失败的段自动转为 `corrupt` 状态，之后的查询不再命中。

## 崩溃一致性：尾部写入中断不会产生可查询的半成品

写入采用两阶段提交：

1. 目录登记 `state=writing`；
2. 段完整编码后写 `tmp/*.tmp`，fsync，原子 rename 到
   `segments/<series>/seg_<id>.seg`，fsync 目录；
3. 目录更新 `state=sealed` —— **只有 sealed 才参与查询**。

启动恢复（`Catalog::open` + `main`）：

- 残留的 `writing` 行 → `aborted`（永不查询）；
- 删除 `tmp/` 残留临时文件；
- 删除未在目录登记的孤儿段文件（rename 后、seal 前崩溃的情形）。

测试 `interrupted_tail_write_is_never_queryable` 与
`writing_segments_are_aborted_on_reopen` 锁定该行为。

## API 一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/series` | 建序列 `{name, codec?}`（默认 xor） |
| GET | `/api/series` | 序列列表 |
| POST | `/api/series/:id/points` | 追加 `{points:[{ts,value}], codec?}`，一次追加 = 一个不可变段 |
| GET | `/api/series/:id/query?from=&to=` | 区间查询（索引定位） |
| GET | `/api/series/:id/segments` | 段列表（含 state） |
| POST | `/api/segments/:id/verify` | 校验，返回可定位损坏范围 |
| POST | `/api/seed` | 生成样本并逐位核对（幂等） |

### 示例

```bash
# 写入含特殊值的批次（注意 ts=1005 重复，keep-last 生效）
curl -X POST localhost:3000/api/series/6/points -H 'content-type: application/json' -d '{
  "points": [
    {"ts":1000,"value":25.5},
    {"ts":1001,"value":"NaN"},
    {"ts":1002,"value":{"bits":"0x8000000000000000"}},
    {"ts":1005,"value":{"bits":"0x7ff8000000000001"}},
    {"ts":1005,"value":99.9}
  ]}'
# -> {"count":4,"duplicates_dropped":1,...}

# 读回逐位核对：NaN -> 0x7ff8000000000000，-0.0 -> 0x8000000000000000
curl "localhost:3000/api/series/6/query?from=1000&to=1010"

# 模拟位翻转后校验：返回 corrupt_ranges 精确到块级时间范围
printf '\xff' | dd of=data/segments/4/seg_4.seg bs=1 seek=2000 count=1 conv=notrunc
curl -X POST localhost:3000/api/segments/4/verify
```

## 内置样本（POST /api/seed）

| 序列 | 间隔 | 编码 | 内容 |
|---|---|---|---|
| `sample-1ms-sine` | 1ms | xor | 正弦波，10k 点 |
| `sample-100ms-const` | 100ms | xor | 常值 25.0，5k 点（约 1 bit/点） |
| `sample-100ms-const-raw` | 100ms | raw | 同上，压缩率对照 |
| `sample-1s-step` | 1s | xor | 两次阶跃突变 + NaN/-0.0/±inf/负载NaN/次正规数 |
| `sample-10ms-random-raw` | 10ms | raw | 确定性伪随机位模式 |

seed 对每个序列执行「写入 → 完整读回 → 逐点比较 (ts, bits)」，
报告 `bit_exact` 与 `mismatches`，即压缩前后逐位核对的证据。

## 已知取舍

- 单写者：追加在目录锁内串行（保证 max-ts 检查无竞争），同步 rusqlite；
- 段在内存中一次构建完成（一次追加 = 一个段），超大批次会占用相应内存；
- 查询按段读文件，无读缓存；校验失败的段需人工重写后才会重新出现。
