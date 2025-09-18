/**
 * Utilities to write files (with and without Prettier).
 *
 * Design:
 * - writeFileToDir: pure file writer (no formatting).
 * - writeFormattedFileToDir: formats content with Prettier, then writes via writeFileToDir.
 *
 * Both:
 *  - Ensure target directory exists.
 *  - Append trailing newline to avoid git warnings.
 *  - Skip writing when content is unchanged (for 'w'/'wx').
 *  - Return absolute file path.
 */

import fs from 'node:fs'
import path from 'node:path'

import { ESLint } from 'eslint'
import prettier from 'prettier'

import defaultPrettierConfig from '../constants/prettier-config.ts'

import type { RequiredOptions } from 'prettier'

type WriteFlag = 'w' | 'a' | 'wx' | 'ax'

export const writeFileToDir = (
  targetDir: string,
  filename: string,
  content: string,
  flag: WriteFlag = 'w',
): string => {
  if (!fs.existsSync(targetDir)) {
    fs.mkdirSync(targetDir, { recursive: true })
  }

  const fullPath = path.resolve(targetDir, filename)
  const normalized = content.endsWith('\n') ? content : `${content}\n`

  if (fs.existsSync(fullPath) && (flag === 'w' || flag === 'wx')) {
    try {
      const existing = fs.readFileSync(fullPath, { encoding: 'utf-8' })
      if (existing === normalized) return fullPath
    } catch {
      // ignore read errors, proceed to write
    }
  }

  fs.writeFileSync(fullPath, normalized, { encoding: 'utf-8', flag })
  return fullPath
}

/**
 * Format content with Prettier, then write.
 * - Pass your shared prettierConfig here (e.g. constants/prettier-config.ts)
 * - This keeps I/O concerns separate while making call sites concise.
 */
export const writeFormattedFileToDir = async (
  targetDir: string,
  filename: string,
  content: string,
  prettierConfig: Partial<RequiredOptions> = defaultPrettierConfig,
  flag: WriteFlag = 'w',
): Promise<string> => {
  const formatted = await prettier.format(content, {
    ...prettierConfig,
    filepath: filename,
  })
  const fullPath = writeFileToDir(targetDir, filename, formatted, flag)
  const eslint = new ESLint({ fix: true, cwd: process.cwd() })
  const results = await eslint.lintFiles([fullPath])
  await ESLint.outputFixes(results)

  return fullPath
}

export default writeFileToDir
