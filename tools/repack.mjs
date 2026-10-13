#!/usr/bin/env node
// dsh-session-repacker 独立 CLI：不依赖 dsh 服务，直接扫描并重打包会话日志。
// 用法：
//   node tools/repack.mjs [root]                     # 默认 root = ./sessions
//   node tools/repack.mjs E:\DeepSeekHarness\sessions
//   node tools/repack.mjs <root> --dry-run           # 只统计不落盘
//   node tools/repack.mjs <root> --file <path> [--file <path> ...]   # 只处理指定文件
//   node tools/repack.mjs <root> --min-frames 64 --min-age-ms 60000
//   node tools/repack.mjs <root> --backup-dir <dir>  # 替换前把原文件原样备份（建议总是带上）
// 认两代命名：v0 的 session.jsonl.zstd 与 vN 的 session.v<N>.jsonl.zstd。
import { repackAll, repackFile, scanZstdFrames } from '../lib/index.mjs'
import { readFile } from 'node:fs/promises'
import { performance } from 'node:perf_hooks'

function flagValue(args, name) {
  const i = args.indexOf(name)
  return i >= 0 && i + 1 < args.length ? args[i + 1] : undefined
}

const args = process.argv.slice(2)
const root = args.find((a) => !a.startsWith('-') && !a.startsWith('--')) || process.cwd()
const dryRun = args.includes('--dry-run')
const files = []
for (let i = 0; i < args.length; i++) {
  if (args[i] === '--file' && i + 1 < args.length) files.push(args[i + 1])
}
const minFrames = Number(flagValue(args, '--min-frames') ?? 200)
const minAgeMs = Number(flagValue(args, '--min-age-ms') ?? 600000)
// 替换前把原文件原样备份一份到这里（强烈建议：会话日志没有第二份）
const backupDir = flagValue(args, '--backup-dir')
const now = Date.now()

function sizeLabel(bytes) {
  return bytes >= 1048576 ? `${(bytes / 1048576).toFixed(2)}MB` : `${(bytes / 1024).toFixed(1)}KB`
}

async function main() {
  const t0 = performance.now()
  const results = []
  if (files.length > 0) {
    for (const file of files) {
      // 干跑模式下直接显示文件当前帧数
      if (dryRun) {
        try {
          const { frames } = scanZstdFrames(await readFile(file))
          console.log(`dry-run ${file}: ${frames.length} frames (${sizeLabel((await readFile(file)).length)})`)
          continue
        } catch (error) {
          console.log(`dry-run ${file}: error ${error.message}`)
          continue
        }
      }
      results.push(await repackFile(file, { minFrames, minAgeMs, now, backupDir }))
    }
  } else {
    const { results: all, summary } = await repackAll(root, { minFrames, minAgeMs, now, dryRun, backupDir })
    console.log(`\n=== ${dryRun ? 'DRY RUN' : 'REPACK'} ${summary.root} ===`)
    console.log(`scanned ${summary.scanned}, repacked ${summary.repacked}, skipped ${summary.skipped}, total ${summary.totalMs}ms`)
    if (summary.repacked > 0) {
      console.log(
        `frames ${summary.framesBefore} -> ${summary.framesAfter} (-${summary.framesBefore - summary.framesAfter}); ` +
          `size ${sizeLabel(summary.bytesBefore)} -> ${sizeLabel(summary.bytesAfter)}`,
      )
    }
    results.push(...all)
  }

  const lines = []
  for (const r of results) {
    if (r.skipped) lines.push(`skip  ${r.file.split(/[\\/]/).slice(-2).join('/')}  (${r.skipped})`)
    else {
      lines.push(
        `OK    ${r.file.split(/[\\/]/).slice(-2).join('/')}  ` +
          `${r.framesBefore}->${r.framesAfter} frames  ${sizeLabel(r.bytesBefore)}->${sizeLabel(r.bytesAfter)}${r.dryRun ? '  (dry)' : ''}` +
          `${r.backup ? `  backup: ${r.backup}` : ''}`,
      )
    }
  }
  if (lines.length) console.log(lines.join('\n'))
  console.log(`\nfinished in ${(performance.now() - t0).toFixed(0)}ms`)
}

main().catch((error) => {
  console.error('repack failed:', error)
  process.exitCode = 1
})
