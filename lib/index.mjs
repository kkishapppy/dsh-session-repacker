import { link, mkdir, open, readFile, readdir, rename, stat, unlink } from "node:fs/promises";
import { join } from "node:path";
import { randomBytes } from "node:crypto";
import z from "node:zlib";
import { performance } from "node:perf_hooks";
//#region src/index.mjs
const name = "session-repacker";
const inject = ["sessions", "timer"];
const ZSTD_MAGIC = 4247762216;
const CHECKSUM_PARAMS = { params: { [z.constants.ZSTD_c_checksumFlag]: 1 } };
/**
* 扫描完整 zstd 帧边界（与 dsh 后端同构的字节级扫描，不解压数据块）。
* @returns {{ frames: {start:number,end:number}[], tornStart?: number }}
*/
function scanZstdFrames(buffer) {
	const frames = [];
	let offset = 0;
	let tornStart;
	while (offset < buffer.length) {
		const start = offset;
		if (buffer.length - offset < 4) {
			tornStart = start;
			break;
		}
		if (buffer.readUInt32LE(offset) !== ZSTD_MAGIC) throw new Error(`corrupt zstd frame magic at byte ${offset}`);
		offset += 4;
		if (offset === buffer.length) {
			tornStart = start;
			break;
		}
		const descriptor = buffer.readUInt8(offset);
		offset += 1;
		if ((descriptor & 24) !== 0) throw new Error(`reserved frame-header bit at byte ${offset - 1}`);
		const contentSizeFlag = descriptor >>> 6;
		const singleSegment = (descriptor & 32) !== 0;
		const checksum = (descriptor & 4) !== 0;
		const dictionaryFlag = descriptor & 3;
		const dictionaryBytes = dictionaryFlag === 3 ? 4 : dictionaryFlag;
		const contentSizeBytes = contentSizeFlag === 0 ? singleSegment ? 1 : 0 : 1 << contentSizeFlag;
		const remainingHeaderBytes = (singleSegment ? 0 : 1) + dictionaryBytes + contentSizeBytes;
		if (buffer.length - offset < remainingHeaderBytes) {
			tornStart = start;
			break;
		}
		offset += remainingHeaderBytes;
		for (;;) {
			if (buffer.length - offset < 3) {
				tornStart = start;
				break;
			}
			const blockHeader = buffer.readUIntLE(offset, 3);
			offset += 3;
			const lastBlock = (blockHeader & 1) !== 0;
			const blockType = blockHeader >>> 1 & 3;
			const blockSize = blockHeader >>> 3;
			if (blockType === 3) throw new Error(`reserved block type at byte ${offset - 3}`);
			const payloadBytes = blockType === 1 ? 1 : blockSize;
			if (buffer.length - offset < payloadBytes) {
				tornStart = start;
				break;
			}
			offset += payloadBytes;
			if (lastBlock) break;
			if (tornStart !== void 0) break;
		}
		if (tornStart !== void 0) break;
		if (checksum) {
			if (buffer.length - offset < 4) {
				tornStart = start;
				break;
			}
			offset += 4;
		}
		frames.push({
			start,
			end: offset
		});
	}
	return tornStart === void 0 ? { frames } : {
		frames,
		tornStart
	};
}
function compressFrame(bytes) {
	return z.zstdCompressSync(bytes, CHECKSUM_PARAMS);
}
function decompressFrame(bytes) {
	return z.zstdDecompressSync(bytes);
}
const yieldToEventLoop = () => new Promise((resolve) => setImmediate(resolve));
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
async function repackFile(file, { minFrames = 200, minAgeMs = 6e5, liveIds = /* @__PURE__ */ new Set(), now = Date.now(), yieldEvery = 200, dryRun = false, backupDir } = {}) {
	let st;
	try {
		st = await stat(file);
	} catch (error) {
		if (error.code === "ENOENT") return {
			file,
			skipped: "missing"
		};
		throw error;
	}
	if (!st.isFile()) return {
		file,
		skipped: "not-a-file"
	};
	if (now - st.mtimeMs < minAgeMs) return {
		file,
		skipped: "too-recent"
	};
	const buffer = await readFile(file);
	const { frames, tornStart } = scanZstdFrames(buffer);
	if (tornStart !== void 0) return {
		file,
		skipped: "torn-tail"
	};
	if (frames.length < minFrames) return {
		file,
		skipped: "few-frames"
	};
	const header = decompressFrame(buffer.subarray(frames[0].start, frames[0].end));
	if (header.length === 0 || header.indexOf(10) !== header.length - 1) return {
		file,
		skipped: "bad-header"
	};
	const chunks = [];
	let bodyLength = 0;
	for (let i = 1; i < frames.length; i++) {
		const plain = decompressFrame(buffer.subarray(frames[i].start, frames[i].end));
		chunks.push(plain);
		bodyLength += plain.length;
		if (yieldEvery > 0 && i % yieldEvery === 0) await yieldToEventLoop();
	}
	const body = Buffer.concat(chunks, bodyLength);
	const repacked = Buffer.concat([compressFrame(header), compressFrame(body)]);
	const firstFrameEnd = findFrameEnd(repacked);
	const plainA = decompressFrame(repacked.subarray(0, firstFrameEnd));
	const plainB = decompressFrame(repacked.subarray(firstFrameEnd));
	if (!Buffer.concat([plainA, plainB]).equals(Buffer.concat([header, body]))) return {
		file,
		skipped: "verify-failed"
	};
	if (dryRun) return {
		file,
		dryRun: true,
		framesBefore: frames.length,
		framesAfter: 2,
		bytesBefore: buffer.length,
		bytesAfter: repacked.length
	};
	const st2 = await stat(file);
	if (st2.size !== st.size || st2.mtimeMs !== st.mtimeMs) return {
		file,
		skipped: "changed"
	};
	const mode = st.mode & 4095;
	let backup;
	if (backupDir) {
		await mkdir(backupDir, { recursive: true });
		const parent = file.split(/[\\/]/).slice(-2, -1)[0] ?? "session";
		const stem = file.split(/[\\/]/).pop();
		backup = join(backupDir, `${parent}.${stem}`);
		if (await fileExists(backup)) {
			if (!await readFile(backup).then((old) => old.equals(buffer)).catch(() => false)) backup = join(backupDir, `${parent}.${stem}.${Date.now()}`);
		}
		if (!await fileExists(backup)) await writeFileSynced(backup, buffer, mode);
	}
	const tmp = `${file}.${randomBytes(6).toString("hex")}.repack.tmp`;
	try {
		await writeFileSynced(tmp, repacked, mode);
	} catch (error) {
		await unlink(tmp).catch(() => {});
		throw error;
	}
	const shadow = `${file}.${randomBytes(6).toString("hex")}.repack.preswap`;
	try {
		await link(file, shadow);
	} catch (error) {
		await unlink(tmp).catch(() => {});
		return {
			file,
			skipped: `no-hardlink (${error.code ?? error.message})`
		};
	}
	try {
		await rename(tmp, file);
	} catch (error) {
		await unlink(tmp).catch(() => {});
		await unlink(shadow).catch(() => {});
		throw error;
	}
	const shadowStat = await stat(shadow);
	if (shadowStat.size !== st.size || shadowStat.mtimeMs !== st.mtimeMs) {
		try {
			await rename(shadow, file);
		} catch (error) {
			throw new Error(`swap 期间检测到并发写入，回滚失败：旧文件（含追加数据）仍在 ${shadow}，重打包结果在 ${file}。原因：${error.message}`);
		}
		return {
			file,
			skipped: "changed-during-swap"
		};
	}
	await unlink(shadow).catch(() => {});
	return {
		file,
		backup,
		framesBefore: frames.length,
		framesAfter: 2,
		bytesBefore: buffer.length,
		bytesAfter: repacked.length,
		headerBytes: header.length,
		bodyBytes: bodyLength
	};
}
/** 文件是否存在（不跟随错误的类型判断）。 */
async function fileExists(path) {
	try {
		await stat(path);
		return true;
	} catch {
		return false;
	}
}
/**
* 独占创建 + 落盘 + fsync，并按给定权限建文件。
* POSIX 上 chmod 失败要炸（否则会话日志可能变成 0644），Windows 上忽略。
*/
async function writeFileSynced(path, bytes, mode) {
	const handle = await open(path, "wx");
	try {
		try {
			await handle.chmod(mode);
		} catch (error) {
			if (process.platform !== "win32") throw error;
		}
		await handle.writeFile(bytes);
		await handle.sync();
	} finally {
		await handle.close();
	}
}
/** 定位第一个 zstd 帧的结束偏移（用于校验时拆分新文件的两个帧）。 */
function findFrameEnd(buffer) {
	const { frames } = scanZstdFrames(buffer);
	if (frames.length < 1) throw new Error("repacked output has no frames");
	return frames[0].end;
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
	return name === "session.jsonl.zstd" || /^session\.v[1-9]\d*\.jsonl\.zstd$/.test(name);
}
/**
* 扫描 root 下的全部会话日志（v0 的 `session.jsonl.zstd` 与 vN 的 `session.v*.jsonl.zstd`）并重打包符合条件者。
* 目录形态：root/<project-key>/<session-id>/session[.vN].jsonl.zstd（深度 ≤ 4）。
*/
async function repackAll(root, opts = {}) {
	const results = [];
	const seen = /* @__PURE__ */ new Set();
	async function walk(dir, depth) {
		if (depth > 4) return;
		let entries;
		try {
			entries = await readdir(dir, { withFileTypes: true });
		} catch {
			return;
		}
		for (const entry of entries) {
			const full = join(dir, entry.name);
			if (entry.isDirectory()) await walk(full, depth + 1);
			else if (entry.isFile() && isSessionLogName(entry.name)) {
				if (seen.has(full)) continue;
				seen.add(full);
				const sessionId = full.split(/[\\/]/).slice(-2, -1)[0] ?? "";
				if (opts.liveIds?.has(sessionId)) {
					results.push({
						file: full,
						skipped: "live"
					});
					continue;
				}
				try {
					const r = await repackFile(full, opts);
					results.push(r);
				} catch (error) {
					results.push({
						file: full,
						skipped: `error: ${error.message}`
					});
				}
			}
		}
	}
	const t0 = performance.now();
	await walk(root, 0);
	const repacked = results.filter((r) => !r.skipped);
	return {
		results,
		summary: {
			root,
			scanned: results.length,
			repacked: repacked.length,
			skipped: results.length - repacked.length,
			totalMs: Math.round(performance.now() - t0),
			framesBefore: repacked.reduce((s, r) => s + (r.framesBefore ?? 0), 0),
			framesAfter: repacked.reduce((s, r) => s + (r.framesAfter ?? 0), 0),
			bytesBefore: repacked.reduce((s, r) => s + (r.bytesBefore ?? 0), 0),
			bytesAfter: repacked.reduce((s, r) => s + (r.bytesAfter ?? 0), 0)
		}
	};
}
/** cordis 插件入口：启动时 + 周期维护。 */
function apply(ctx, config = {}) {
	const root = config.root || join(process.cwd(), "sessions");
	const logger = ctx.logger("session-repacker");
	const minFrames = config.minFrames ?? 200;
	const minAgeMs = config.minAgeMs ?? 6e5;
	const dryRun = config.dryRun === true;
	const liveIds = () => {
		try {
			return new Set(ctx.sessions.list().map((s) => s.id));
		} catch {
			return /* @__PURE__ */ new Set();
		}
	};
	let running = false;
	async function run(reason) {
		if (running) return;
		running = true;
		performance.now();
		try {
			const { summary } = await repackAll(root, {
				minFrames,
				minAgeMs,
				liveIds: liveIds(),
				dryRun,
				backupDir: config.backupDir,
				now: Date.now()
			});
			if (summary.repacked > 0) {
				const saved = summary.framesBefore - summary.framesAfter;
				logger.info(`[${reason}] repacked ${summary.repacked}/${summary.scanned} logs: ${summary.framesBefore} -> ${summary.framesAfter} frames (${saved} fewer), ${(summary.bytesBefore / 1048576).toFixed(1)}MB -> ${(summary.bytesAfter / 1048576).toFixed(1)}MB, ${summary.totalMs}ms`);
			} else logger.info(`[${reason}] nothing to repack (${summary.scanned} logs scanned, ${summary.totalMs}ms)`);
		} catch (error) {
			logger.warn(`[${reason}] repack failed: ${error.message}`);
		} finally {
			running = false;
		}
	}
	if (config.runOnStart !== false) run("startup");
	const intervalMs = config.intervalMs ?? 36e5;
	if (intervalMs > 0) ctx.setInterval(() => void run("interval"), intervalMs);
}
//#endregion
export { apply, inject, name, repackAll, repackFile, scanZstdFrames };
