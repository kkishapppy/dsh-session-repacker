# dsh-session-repacker

把 DSH 会话日志中成千上万个独立 zstd 帧重打包为官方原生两帧形态，大幅加快旧会话打开速度。

## 2026-10-13 改造与实测（先把这段读完）

这一版在原版基础上修了 4 处「说得对、但实际不生效」的地方，并补了并发写入的回滚护栏：

| 改动 | 为什么 |
|---|---|
| **认两代文件名**：v0 的 `session.jsonl.zstd` + vN 的 `session.v<N>.jsonl.zstd` | 原版只认 v0 名。DSH 0.2.0-rc.2 起日志叫 `session.v3/v4.jsonl.zstd` ⇒ **原版在新版机器上一个文件都匹配不到**（实测 65 个文件命中 0） |
| **新文件继承原权限** | dsh 用 `0600` 写日志，而新建临时文件按 umask 022 会落成 `0644` ⇒ 直接 rename 等于把会话日志变成同机其他用户可读 |
| **换名窗口的硬链接影子护栏**（新增 `changed-during-swap` + 自动回滚） | 原来只在写临时文件**之前**复查 size/mtime；writer 若在这之后、rename 之前追加，旧 inode 被换掉 ⇒ **追加的数据静默丢失**。现在交换后再看旧 inode 长没长，长了就把旧 inode 换回去 |
| **`--backup-dir`**（可选） | 它重写的是会话正本，没有第二份。开了就先把校验过的原始字节落一份备份，路径写进结果 |
| 修 `apply()` 的统计行 | 原来写 `summary.repacked.length`（它是数字）⇒ 恒为 undefined，**真干了活也打印 "nothing to repack"** |

实测（2026-10-13，两台机器，其中老版本那台**只读取样**）：

| 会话 | 重打包前 | 重打包后 |
|---|---|---|
| 一台仍在跑 0.1.2-rc.1 的机器上最大的旧会话 | 42.48MB / **75,393 帧** / 逐帧解压 **5586ms** | 22.70MB / 2 帧 / **258ms** |
| 同一台机器上一个中等大小的会话 | 7.59MB / 7,981 帧 / 1130ms | 4.15MB / 2 帧 / 42ms |
| 另一台跑 0.2.0-rc.2 的机器上最大的 v3 会话 | 11.64MB / 5,426 帧 | 7.74MB / 2 帧 |

约束与现实：

- **它没发到 npm**（`package.json` 还是 `private: true`），所以下面那条 `dsh plugin add dsh-session-repacker` 会在 registry 上 404；要用只能 clone 下来 / `link` 进 profile，或直接跑 `tools/repack.mjs`。
- **运行纪律**：它是原地重写会话正本。dsh 的会话对象在进程里长驻内存，`ctx.sessions.list()` 只覆盖**活跃**会话、空闲但常驻的对象不一定会报 ⇒ 批量跑之前**先停 web，跑完再起**（独立 CLI 那条路完全不检查会话是否正开着）。
- 只读场景（不写盘）用 `--dry-run`；写盘务必带 `--backup-dir`。

## 背景：为什么会话打开慢

DSH 的 JSONL 会话后端（`dsh-session-persistence-jsonl`）**每次写入批处理都单独压缩成一个 zstd 帧**再追加到 `session.jsonl.zstd` 尾部。流式输出期间每 ~200ms 一批，一个长会话动辄上万帧；而读取时必须**逐帧解压**（且后端没有 seek 式后缀读取，无论只要尾部还是全部都要解码整个文件）。

实测（2026-08-14）：本机最大会话 **5.49MB 压缩 = 14,467 帧 / 19,310 事件**，纯解压 ~660ms，真实读取路径约 1.5~3s。

| 会话 | 重打包前 | 重打包后 |
|---|---|---|
| 最大会话 | 14,467 帧 / 5.49MB / ~661ms | **2 帧** / 2.09MB / **~126ms（6 倍）** |
| 全量 24 个旧会话 | 39,911 帧 / 20.08MB | 48 帧 / 10.67MB |

重打包同时把压缩体积减半（单大帧压缩率远好于万个小帧）。

## 原理

把"万帧"日志重打包成官方写入时的原生形态——**头部帧（恰好一行 header）+ 单个事件帧**（其余全部明文拼接后重压一帧）。明文**逐字节不变**，dsh 读取路径完全兼容（`assertZstdHeaderFrame` 只要求第一帧恰为一行 header）。

覆盖一切从磁盘读旧会话的路径：GUI 打开、新对话接续/分支（fork 前需全量解码父会话）、子会话继承。

## 安装

```bash
# 作为 DSH 插件（推荐）：
dsh plugin --profile web add dsh-session-repacker   # ⚠ 未发布到 npm，见上面「改造与实测」
# 或克隆到 plugins/ 目录 + link 进 profile（见 DSH 插件开发手册）
```

配置（`profiles/<profile>/cordis.patch.yml`）：

```yaml
- id: session-repacker
  config:
    root: 'E:\DeepSeekHarness\sessions'   # 会话根目录（必填）
    minFrames: 200        # 帧数低于此值不处理（默认 200）
    minAgeMs: 600000      # mtime 距今不足此值不处理，避开活跃写入（默认 10 分钟）
    intervalMs: 3600000   # 周期维护间隔毫秒（默认 1 小时）
    backupDir: ''         # 可选：替换前把原文件备份到这个目录
```

重启服务器后生效：启动时全量扫描一次，之后按 `intervalMs` 周期维护。

## 独立 CLI（无需 DSH 服务）

```bash
node tools/repack.mjs <root>                  # 扫描并重打包
node tools/repack.mjs <root> --dry-run        # 只统计不落盘
node tools/repack.mjs <root> --backup-dir <dir>  # 替换前备份原文件（建议总是带上）
node tools/repack.mjs <root> --file <path>    # 只处理指定文件（可多次）
node tools/repack.mjs <root> --min-frames 64 --min-age-ms 60000
```

## 安全设计

1. 只处理规范命名的 zstd 会话日志（v0 的 `session.jsonl.zstd` 与 vN 的 `session.v<N>.jsonl.zstd`）；跳过 mtime 距今 < `minAgeMs` 的文件（避开活跃写入）
2. 跳过当前 live 会话（注入 `sessions` 服务时用 `ctx.sessions.list()`）
3. 替换前做**字节级等价校验**（新文件解压明文 == 原始明文）
4. 替换前 stat 比对 size/mtime，文件被并发追加则放弃本次
5. 换名窗口内挂硬链接盯住旧 inode：交换后它长过 ⇒ **把旧 inode 换回去**（`changed-during-swap`，不丢追加数据）；挂不上硬链接则 fail-closed 跳过
6. 新文件继承原文件权限（不把 `0600` 变成 `0644`）
7. 可选 `backupDir`：把校验过的那份原始字节备份一份，路径写进结果
8. torn tail（未完成帧）跳过，交给 dsh 自身的修复路径
9. 临时文件 + fsync + 原子 rename（Windows 下与官方后端同构）

服务器对重打包后的文件追加新帧（`session/end-seed` 等）实测兼容，后续可被再次重打包（幂等）。

## 构建

```bash
npm i
npx -y tsdown@0.22.2 --config ./tsdown.config.ts   # 产物 lib/index.mjs
```

## 注意事项

- 已产生的旧文件帧数定形后无法回溯压缩，只能靠本插件/CLI 重打包；**老版本**（如 0.1.2-rc.1）配置里还有 `writeBatchMaxDelayMs`（默认 200ms，就是"每 200ms 一批 = 一个帧"的来源），调大可减少未来日志的帧数（属耐久性权衡）——**0.2.0-rc.2 已删掉这个参数**（配置只剩 `root`/`compression`），新版机器上只能靠重打包
- 插件只做维护，不改变任何读取 API 与数据语义

## License

MIT
