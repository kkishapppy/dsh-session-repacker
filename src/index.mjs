// dsh-session-repacker
// 背景：dsh 的 JSONL 会话后端（dsh-session-persistence-jsonl）每次写入批处理
// 都会单独压成一个 zstd 帧再追加到 session.jsonl.zstd 尾部。流式输出期间每
// ~200ms 一批 → 一个长会话动辄上万帧；而读取时（readZstdPrefix）必须逐帧解压，
// 帧数直接决定打开旧会话的耗时（实测 5.49MB/14467 帧 ≈ 660ms 纯解压，
// 加上逐行 decodeStorageRecord 与两次 structuredClone 全量拷贝 ≈ 1.5~3s）。
//
// 本插件把"万帧"日志重打包成官方写入时的原生形态：头部帧（header 一行）+
// 单个事件帧（其余全部明文拼接后重压一帧）。明文逐字节不变，dsh 读取路径
// 完全兼容（readZstdPrefix 只要求第一帧恰为一行 header，其余帧任意）。
//
// 安全护栏：
//   1. 只处理规范命名的 zstd 会话日志（v0 的 session.jsonl.zstd / vN 的 session.v<N>.jsonl.zstd）；
//   2. 跳过 mtime 距今不足 minAgeMs 的文件（避免与正在写入的会话竞争）；
//   3. 跳过当前 live 会话（注入 sessions 服务时用 ctx.sessions.list()）；
//   4. 重打包前解压全部帧，替换前做字节级等价校验；
//   5. 替换前再次 stat 比对 size/mtime，文件被并发追加则放弃本次；
//   6. 换名窗口内再挂一个硬链接盯住旧 inode：交换后发现它长过 ⇒ 立刻把旧 inode 换回去（回滚，不丢数据）；
//   7. 新文件继承原文件权限（dsh 写的是 0600，临时文件默认会是 0644）；
//   8. 可选 backupDir：先把原字节（校验过的那份快照）备份一份，路径写进结果；
//   9. torn tail（未完成帧）交由 dsh 自身的修复路径处理，这里跳过。
import { readFile, stat, rename, unlink, open, readdir, mkdir, link } from 'node:fs/promises'
import { join } from 'node:path'
import { randomBytes } from 'node:crypto'
import z from 'node:zlib'
import { performance } from 'node:perf_hooks'

export const name = 'session-repacker'
// ctx.setInterval is provided through Cordis' timer service, so both
// dependencies must be declared before the plugin is instantiated.
export const inject = ['sessions', 'timer']

const ZSTD_MAGIC = 4247762216 // 0xFD2FB528 little-endian magic
const CHECKSUM_PARAMS = { params: { [z.constants.ZSTD_c_checksumFlag]: 1 } }

/**
 * 扫描完整 zstd 帧边界（与 dsh 后端同构的字节级扫描，不解压数据块）。
 * @returns {{ frames: {start:number,end:number}[], tornStart?: number }}
 */
export function scanZstdFrames(buffer) {
  const frames = []
  let offset = 0
  let tornStart
  while (offset < buffer.length) {
    const start = offset
    if (buffer.length - offset < 4) { tornStart = start; break }
    if (buffer.readUInt32LE(offset) !== ZSTD_MAGIC) throw new Error(`corrupt zstd frame magic at byte ${offset}`)
    offset += 4
    if (offset === buffer.length) { tornStart = start; break }
    const descriptor = buffer.readUInt8(offset)
    offset += 1
    if ((descriptor & 24) !== 0) throw new Error(`reserved frame-header bit at byte ${offset - 1}`)
    const contentSizeFlag = descriptor >>> 6
    const singleSegment = (descriptor & 32) !== 0
    const checksum = (descriptor & 4) !== 0
    const dictionaryFlag = descriptor & 3
    const dictionaryBytes = dictionaryFlag === 3 ? 4 : dictionaryFlag
    const contentSizeBytes = contentSizeFlag === 0 ? (singleSegment ? 1 : 0) : 1 << contentSizeFlag
    const remainingHeaderBytes = (singleSegment ? 0 : 1) + dictionaryBytes + contentSizeBytes
    if (buffer.length - offset < remainingHeaderBytes) { tornStart = start; break }
    offset += remainingHeaderBytes
    for (;;) {
      if (buffer.length - offset < 3) { tornStart = start; break }
      const blockHeader = buffer.readUIntLE(offset, 3)
      offset += 3
      const lastBlock = (blockHeader & 1) !== 0
      const blockType = blockHeader >>> 1 & 3
      const blockSize = blockHeader >>> 3
      if (blockType === 3) throw new Error(`reserved block type at byte ${offset - 3}`)
      const payloadBytes = blockType === 1 ? 1 : blockSize
      if (buffer.length - offset < payloadBytes) { tornStart = start; break }
      offset += payloadBytes
      if (lastBlock) break
      if (tornStart !== undefined) break
    }
    if (tornStart !== undefined) break
    if (checksum) {
      if (buffer.length - offset < 4) { tornStart = start; break }
      offset += 4
    }
    frames.push({ start, end: offset })
  }
  return tornStart === undefined ? { frames } : { frames, tornStart }
}

function compressFrame(bytes) {
  return z.zstdCompressSync(bytes, CHECKSUM_PARAMS)
}

function decompressFrame(bytes) {
  return z.zstdDecompressSync(bytes)
}

const yieldToEventLoop = () => new Promise((resolve) => setImmediate(resolve))

/**
 * 重打包一个会话日志文件（幂等：帧数已很少或形态已标准时自动跳过）。
 * @param {string} file session.jsonl.zstd 绝对路径
 * @param {object} [opts]
 * @param {number} [opts.minFrames=200] 帧数低于此值不处理
 * @param {number} [opts.minAgeMs=600000] mtime 距今不足此值不处理（避开活跃写入）
 * @param {Set<string>} [opts.liveIds] 当前 live 会话 id 集合（跳过）
 * @param {number} [opts.now=Date.now()]
 * @param {number} [opts.yieldEvery=200] 每解压多少帧让出一次事件循环
 * @param {boolean} [opts.dryRun] 只统计不落盘
 * @param {string} [opts.backupDir] 替换前把原始字节（已校验过的那份内存快照）落一份到这里
 * @returns {Promise<object>} 结果或跳过原因
 */
export async function repackFile(file, {
  minFrames = 200,
  minAgeMs = 600000,
  liveIds = new Set(),
  now = Date.now(),
  yieldEvery = 200,
  dryRun = false,
  backupDir,
} = {}) {
  let st
  try {
    st = await stat(file)
  } catch (error) {
    if (error.code === 'ENOENT') return { file, skipped: 'missing' }
    throw error
  }
  if (!st.isFile()) return { file, skipped: 'not-a-file' }
  if (now - st.mtimeMs < minAgeMs) return { file, skipped: 'too-recent' }

  const buffer = await readFile(file)
  const { frames, tornStart } = scanZstdFrames(buffer)
  if (tornStart !== undefined) return { file, skipped: 'torn-tail' }
  if (frames.length < minFrames) return { file, skipped: 'few-frames' }

  // 首帧必须是"恰好一行 header"（dsh 读取器的硬性要求）
  const header = decompressFrame(buffer.subarray(frames[0].start, frames[0].end))
  if (header.length === 0 || header.indexOf(10) !== header.length - 1) {
    return { file, skipped: 'bad-header' }
  }

  // 逐帧解压其余部分
  const chunks = []
  let bodyLength = 0
  for (let i = 1; i < frames.length; i++) {
    const plain = decompressFrame(buffer.subarray(frames[i].start, frames[i].end))
    chunks.push(plain)
    bodyLength += plain.length
    if (yieldEvery > 0 && i % yieldEvery === 0) await yieldToEventLoop()
  }
  const body = Buffer.concat(chunks, bodyLength)
  const repacked = Buffer.concat([compressFrame(header), compressFrame(body)])

  // 字节级等价校验：解压后的新文件明文必须与原始明文完全一致
  const firstFrameEnd = findFrameEnd(repacked)
  const plainA = decompressFrame(repacked.subarray(0, firstFrameEnd))
  const plainB = decompressFrame(repacked.subarray(firstFrameEnd))
  if (!Buffer.concat([plainA, plainB]).equals(Buffer.concat([header, body]))) {
    return { file, skipped: 'verify-failed' }
  }

  if (dryRun) {
    return {
      file,
      dryRun: true,
      framesBefore: frames.length,
      framesAfter: 2,
      bytesBefore: buffer.length,
      bytesAfter: repacked.length,
    }
  }

  // 替换前复查：文件被并发追加则放弃（保留 dsh 正在写入的数据）
  const st2 = await stat(file)
  if (st2.size !== st.size || st2.mtimeMs !== st.mtimeMs) return { file, skipped: 'changed' }

  // 原始权限要跟着走：dsh 自己用 0600 写会话日志，而新建临时文件会落成 0644(umask 022)
  // ⇒ 直接 rename 会把会话日志变成同机其他用户可读。
  const mode = st.mode & 0o7777

  // 可选备份：写的就是刚校验过的那份内存快照（不再重读，免掉二次竞态）
  let backup
  if (backupDir) {
    await mkdir(backupDir, { recursive: true })
    const parent = file.split(/[\\/]/).slice(-2, -1)[0] ?? 'session'
    const stem = file.split(/[\\/]/).pop()
    backup = join(backupDir, `${parent}.${stem}`)
    if (await fileExists(backup)) {
      // 同名备份已存在：内容一致就复用；不一致就换带时间戳的名字 —— 绝不覆盖旧备份
      const same = await readFile(backup).then((old) => old.equals(buffer)).catch(() => false)
      if (!same) backup = join(backupDir, `${parent}.${stem}.${Date.now()}`)
    }
    if (!(await fileExists(backup))) await writeFileSynced(backup, buffer, mode)
  }

  const tmp = `${file}.${randomBytes(6).toString('hex')}.repack.tmp`
  try {
    await writeFileSynced(tmp, repacked, mode)
  } catch (error) {
    await unlink(tmp).catch(() => {})
    throw error
  }

  // 换名窗口（写 tmp + fsync + rename）里 writer 仍可能往旧 inode 追加。rename 之后再 stat 原文件
  // 已经看不到它了 —— 所以先把旧 inode 挂一个硬链接，交换后看它有没有长：长了就把旧 inode 换回去
  // （它的内容含并发追加的数据），保证"发现即回滚"，不是打印一句请手工恢复。
  // 挂不上硬链接（不支持的文件系统）就 fail-closed，不去冒这个险。
  const shadow = `${file}.${randomBytes(6).toString('hex')}.repack.preswap`
  try {
    await link(file, shadow)
  } catch (error) {
    await unlink(tmp).catch(() => {})
    return { file, skipped: `no-hardlink (${error.code ?? error.message})` }
  }
  try {
    await rename(tmp, file)
  } catch (error) {
    await unlink(tmp).catch(() => {})
    await unlink(shadow).catch(() => {})
    throw error
  }
  const shadowStat = await stat(shadow)
  if (shadowStat.size !== st.size || shadowStat.mtimeMs !== st.mtimeMs) {
    try {
      await rename(shadow, file) // 回滚：恢复含追加数据的旧 inode
    } catch (error) {
      throw new Error(
        `swap 期间检测到并发写入，回滚失败：旧文件（含追加数据）仍在 ${shadow}，重打包结果在 ${file}。原因：${error.message}`,
      )
    }
    return { file, skipped: 'changed-during-swap' }
  }
  await unlink(shadow).catch(() => {})
  return {
    file,
    backup,
    framesBefore: frames.length,
    framesAfter: 2,
    bytesBefore: buffer.length,
    bytesAfter: repacked.length,
    headerBytes: header.length,
    bodyBytes: bodyLength,
  }
}

/** 文件是否存在（不跟随错误的类型判断）。 */
async function fileExists(path) {
  try {
    await stat(path)
    return true
  } catch {
    return false
  }
}

/**
 * 独占创建 + 落盘 + fsync，并按给定权限建文件。
 * POSIX 上 chmod 失败要炸（否则会话日志可能变成 0644），Windows 上忽略。
 */
async function writeFileSynced(path, bytes, mode) {
  const handle = await open(path, 'wx')
  try {
    try {
      await handle.chmod(mode)
    } catch (error) {
      if (process.platform !== 'win32') throw error
    }
    await handle.writeFile(bytes)
    await handle.sync()
  } finally {
    await handle.close()
  }
}

/** 定位第一个 zstd 帧的结束偏移（用于校验时拆分新文件的两个帧）。 */
function findFrameEnd(buffer) {
  const { frames } = scanZstdFrames(buffer)
  if (frames.length < 1) throw new Error('repacked output has no frames')
  return frames[0].end
}

/**
 * 该文件名是不是要处理的会话日志世代。
 * DSH 有两代规范命名（见 dsh-session-format 的 parseSessionFormatLogFilename）：
 *   - v0 → `session.jsonl.zstd`（老版本，如 0.1.2-rc.1；实测一台老机器上 139 个文件全是这个名）
 *   - vN → `session.v<N>.jsonl.zstd`（N ≥ 1，无前导零；0.2.0-rc.2 上是 v3/v4）
 * 临时文件、备份、大写、带前导零、`session.v0.*` 这些非规范名一律不碰。
 * ⚠️ 只认 v0 名是原版的死判据：在新版本机器上实测 65 个文件命中 0 —— 装上也一个文件都不处理。
 */
function isSessionLogName(name) {
  return name === 'session.jsonl.zstd' || /^session\.v[1-9]\d*\.jsonl\.zstd$/.test(name)
}

/**
 * 扫描 root 下的全部会话日志（v0 的 `session.jsonl.zstd` 与 vN 的 `session.v*.jsonl.zstd`）并重打包符合条件者。
 * 目录形态：root/<project-key>/<session-id>/session[.vN].jsonl.zstd（深度 ≤ 4）。
 */
export async function repackAll(root, opts = {}) {
  const results = []
  const seen = new Set()
  async function walk(dir, depth) {
    if (depth > 4) return
    let entries
    try {
      entries = await readdir(dir, { withFileTypes: true })
    } catch {
      return // 目录不可读/不存在：跳过
    }
    for (const entry of entries) {
      const full = join(dir, entry.name)
      if (entry.isDirectory()) {
        await walk(full, depth + 1)
      } else if (entry.isFile() && isSessionLogName(entry.name)) {
        if (seen.has(full)) continue
        seen.add(full)
        // 提取 session id（父目录名）
        const sessionId = full.split(/[\\/]/).slice(-2, -1)[0] ?? ''
        if (opts.liveIds?.has(sessionId)) {
          results.push({ file: full, skipped: 'live' })
          continue
        }
        try {
          const r = await repackFile(full, opts)
          results.push(r)
        } catch (error) {
          results.push({ file: full, skipped: `error: ${error.message}` })
        }
      }
    }
  }
  const t0 = performance.now()
  await walk(root, 0)
  const repacked = results.filter((r) => !r.skipped)
  const summary = {
    root,
    scanned: results.length,
    repacked: repacked.length,
    skipped: results.length - repacked.length,
    totalMs: Math.round(performance.now() - t0),
    framesBefore: repacked.reduce((s, r) => s + (r.framesBefore ?? 0), 0),
    framesAfter: repacked.reduce((s, r) => s + (r.framesAfter ?? 0), 0),
    bytesBefore: repacked.reduce((s, r) => s + (r.bytesBefore ?? 0), 0),
    bytesAfter: repacked.reduce((s, r) => s + (r.bytesAfter ?? 0), 0),
  }
  return { results, summary }
}

/** cordis 插件入口：启动时 + 周期维护。 */
export function apply(ctx, config = {}) {
  const root = config.root || join(process.cwd(), 'sessions')
  const logger = ctx.logger('session-repacker')
  const minFrames = config.minFrames ?? 200
  const minAgeMs = config.minAgeMs ?? 600000
  const dryRun = config.dryRun === true

  const liveIds = () => {
    try {
      return new Set(ctx.sessions.list().map((s) => s.id))
    } catch {
      return new Set()
    }
  }

  let running = false
  async function run(reason) {
    if (running) return
    running = true
    const t0 = performance.now()
    try {
      const { summary } = await repackAll(root, {
        minFrames,
        minAgeMs,
        liveIds: liveIds(),
        dryRun,
        backupDir: config.backupDir,
        now: Date.now(),
      })
      // summary.repacked 是数字，不是数组：原来写 .length 恒为 undefined ⇒ 永远走 else 分支、
      // 真干了活也打印"nothing to repack"。
      if (summary.repacked > 0) {
        const saved = summary.framesBefore - summary.framesAfter
        logger.info(
          `[${reason}] repacked ${summary.repacked}/${summary.scanned} logs: ` +
            `${summary.framesBefore} -> ${summary.framesAfter} frames (${saved} fewer), ` +
            `${(summary.bytesBefore / 1048576).toFixed(1)}MB -> ${(summary.bytesAfter / 1048576).toFixed(1)}MB, ` +
            `${summary.totalMs}ms`,
        )
      } else {
        logger.info(`[${reason}] nothing to repack (${summary.scanned} logs scanned, ${summary.totalMs}ms)`)
      }
    } catch (error) {
      logger.warn(`[${reason}] repack failed: ${error.message}`)
    } finally {
      running = false
    }
  }

  if (config.runOnStart !== false) void run('startup')
  const intervalMs = config.intervalMs ?? 3600000
  if (intervalMs > 0) {
    ctx.setInterval(() => void run('interval'), intervalMs)
  }
}
