import { BlobServiceClient } from '@azure/storage-blob'
import { filter, floor, isEmpty, isNil, mapKeys, max, min, split, includes } from 'lodash-es'
import sharp from 'sharp'
import { v4 as uuidv4 } from 'uuid'

import BlobError from '../errors/blob.error.ts'
import ensureErrorMessage from '../utils/ensure-error-message.ts'
import ensureLowerCase from '../utils/ensure-lower-case.ts'

import type { EntityParameters, WithID } from '../interfaces/base.dao.interface.ts'
import type { BlockBlobClient, ContainerClient } from '@azure/storage-blob'

export type BlobDataType = 'log' | 'photo'
export const imageFormats = ['jpeg', 'png', 'webp', 'avif', 'gif', 'tiff', 'heif', 'bmp'] as const
export type ImageFormat = (typeof imageFormats)[number]

export type LogBlob<Types extends Record<string, WithID>> = {
  user?: string
  role?: string
  createdAt?: string
  logType?: string
  actionType?: string
  message?: string
  payload: {
    rawData?: {
      [K in keyof Types]?: EntityParameters<Types[K]>[]
    }
    processedData?: {
      [K in keyof Types]?: EntityParameters<Types[K]>[]
    }
    photo?: Buffer
  }
  extra?: {
    ip?: string
    userAgent?: string
    [k: string]: unknown
  }
}

export type UploadOptions = {
  filename?: string
  imageWidth?: number
  imageQuality?: number
  progressive?: boolean
  metadata?: Record<string, string>
  background?: string
  stripMetadata?: boolean
  allowFormats?: ImageFormat[]
  onUnsupported?: 'convert' | 'reject'
}

export class BlobService {
  private static client?: BlobServiceClient
  private containerClient: ContainerClient
  protected blobDataType: BlobDataType
  private static logsInstance: BlobService
  private static photosInstance: BlobService

  private constructor(containerName: string, dataType: BlobDataType = 'log') {
    this.blobDataType = dataType
    const blobServiceClient = BlobService.getClient(dataType)
    this.containerClient = blobServiceClient.getContainerClient(containerName)
  }

  private static getClient(blobDataType: BlobDataType): BlobServiceClient {
    const conn = process.env.AZURE_BLOB_CONN
    if (!conn) {
      throw BlobError.configMissing(blobDataType, 'AZURE_BLOB_CONN')
    }
    if (!BlobService.client) {
      BlobService.client = BlobServiceClient.fromConnectionString(conn)
    }
    return BlobService.client
  }
  public static logs(): BlobService {
    const name = process.env.AZURE_BLOB_LOGS_CONTAINER
    if (!name) throw BlobError.configMissing('log', 'AZURE_BLOB_LOGS_CONTAINER')
    if (!BlobService.logsInstance) BlobService.logsInstance = new BlobService(name, 'log')

    return BlobService.logsInstance
  }

  public static photos(): BlobService {
    const name = process.env.AZURE_BLOB_PHOTOS_CONTAINER
    if (!name) throw BlobError.configMissing('photo', 'AZURE_BLOB_PHOTOS_CONTAINER')
    if (!BlobService.photosInstance) BlobService.photosInstance = new BlobService(name, 'photo')

    return BlobService.photosInstance
  }

  public get defaultFilename(): string {
    const iso = new Date().toISOString().replace(/:/g, '-')

    return `${iso}-${uuidv4()}`
  }

  /**
   * Generic upload method that returns blob URL.
   */
  public async upload<Types extends Record<string, WithID>>(
    content: LogBlob<Types>,
    options: UploadOptions = {},
  ): Promise<string> {
    try {
      if (this.blobDataType === 'log') {
        return await this.uploadLog(content, options)
      } else {
        return await this.uploadPhoto(content, options)
      }
    } catch (error: unknown) {
      throw BlobError.uploadFailed(this.blobDataType, ensureErrorMessage(error))
    }
  }

  /**
   * Delete a blob by its full URL
   * @returns true if blob existed and was deleted, false if blob didn't exist
   */
  public async deleteByUrl(blobUrl: string): Promise<boolean> {
    try {
      // Extract blob path from URL: remove domain part
      const url = new URL(blobUrl)
      // pathname: /containerName/blobName → remove leading slash and containerName
      const pathParts = filter(split(url.pathname, '/'), Boolean) // ['', container, ...blobSegments] -> ['container', ...]
      const [containerFromUrl, ...blobSegments] = pathParts
      if (!containerFromUrl || containerFromUrl !== this.containerClient.containerName) {
        throw BlobError.crossContainerDelete(
          this.blobDataType,
          this.containerClient.containerName,
          containerFromUrl ?? '',
        )
      }

      const encodedBlobName = blobSegments.join('/')
      const blobName = decodeURIComponent(encodedBlobName)
      const blockBlobClient = this.containerClient.getBlockBlobClient(blobName)
      const deleteResponse = await blockBlobClient.deleteIfExists()

      return deleteResponse.succeeded
    } catch (error: unknown) {
      throw BlobError.deleteFailed(this.blobDataType, ensureErrorMessage(error))
    }
  }

  private safeMetadata(metadata?: Record<string, string>): Record<string, string> {
    if (isNil(metadata) || isEmpty(metadata)) return {}

    return mapKeys(metadata, (_v, k) => ensureLowerCase(k))
  }

  /** Ensure container exists (idempotent). */
  private async ensureContainer(): Promise<void> {
    await this.containerClient.createIfNotExists()
  }

  private async sniffImageFormat(buf: Buffer): Promise<ImageFormat | null> {
    try {
      const meta = await sharp(buf, { failOn: 'none' }).metadata()
      const fmt = ensureLowerCase(meta.format ?? '') as ImageFormat | ''
      return includes(imageFormats, fmt) ? (fmt as ImageFormat) : null
    } catch {
      return null
    }
  }

  private extFromFormat(fmt: ImageFormat): string {
    return fmt === 'jpeg' ? 'jpg' : fmt === 'bmp' ? 'png' : fmt
  }

  private contentTypeFromFormat(fmt: ImageFormat): string {
    switch (fmt) {
      case 'jpeg':
        return 'image/jpeg'
      case 'png':
      case 'bmp':
        return 'image/png'
      case 'webp':
        return 'image/webp'
      case 'avif':
        return 'image/avif'
      case 'gif':
        return 'image/gif'
      case 'tiff':
        return 'image/tiff'
      case 'heif':
        return 'image/heif'
      default:
        return 'application/octet-stream'
    }
  }

  private ensureExt(filename: string, ext: string): string {
    const lastSlash = filename.lastIndexOf('/')
    const beforeSlash = filename.slice(0, lastSlash + 1)
    const afterSlash = filename.slice(lastSlash + 1)
    const hasSlash = lastSlash >= 0
    const dir = hasSlash ? beforeSlash : ''
    const base = hasSlash ? afterSlash : filename
    const clean = includes(base, '.') ? base.slice(0, base.lastIndexOf('.')) : base

    return `${dir}${clean}.${ext}`
  }

  private buildBlobPath<Types extends Record<string, WithID>>(
    filename?: string,
    content?: Pick<LogBlob<Types>, 'logType' | 'actionType'>,
    ext?: string,
  ) {
    const logType = content?.logType ?? 'Info'
    const actionType = content?.actionType ?? 'insert_table_item'
    const base = filename ?? `${logType}/${actionType}/${this.defaultFilename}`

    return ext ? this.ensureExt(base, ext) : base
  }

  private async uploadImagePassthrough(
    buf: Buffer,
    opt: UploadOptions,
    filename?: string,
  ): Promise<string> {
    const allowed = opt.allowFormats ?? imageFormats
    const onUnsup = opt.onUnsupported ?? 'convert'
    const fmt = await this.sniffImageFormat(buf)
    if (!fmt || !includes(allowed, fmt)) {
      if (onUnsup === 'reject') throw BlobError.uploadFailed('photo', 'unsupported image format')
      const jpegBuf = await this.toFormattedBuffer(buf, opt, 'jpeg')

      return await this.uploadImageBuffer(jpegBuf, 'jpeg', filename, opt)
    }

    const processed = await this.toFormattedBuffer(buf, opt, fmt)

    return await this.uploadImageBuffer(processed, fmt, filename, opt)
  }

  private async uploadImageBuffer(
    buf: Buffer,
    fmt: ImageFormat,
    filename: string | undefined,
    opt: UploadOptions,
  ): Promise<string> {
    const ext = this.extFromFormat(fmt)
    const blobName = this.buildBlobPath(filename, undefined, ext)
    const bbc: BlockBlobClient = this.containerClient.getBlockBlobClient(blobName)
    await this.ensureContainer()
    await bbc.uploadData(buf, {
      blobHTTPHeaders: { blobContentType: this.contentTypeFromFormat(fmt) },
      metadata: this.safeMetadata(opt.metadata),
    })

    return bbc.url
  }

  private async uploadLog<Types extends Record<string, WithID>>(
    content: LogBlob<Types>,
    options: UploadOptions = {},
  ): Promise<string> {
    const logContent = { ...content, createdAt: content.createdAt ?? new Date().toISOString() }
    const blobName = this.buildBlobPath(options.filename, content, 'json')
    const bbc: BlockBlobClient = this.containerClient.getBlockBlobClient(blobName)
    await this.ensureContainer()
    await bbc.uploadData(Buffer.from(JSON.stringify(logContent, null, 2)), {
      blobHTTPHeaders: { blobContentType: 'application/json; charset=utf-8' },
      metadata: this.safeMetadata(options.metadata),
    })

    return bbc.url
  }

  private async uploadPhoto<Types extends Record<string, WithID>>(
    content: LogBlob<Types>,
    options: UploadOptions = {},
  ): Promise<string> {
    const buf = content.payload?.photo
    if (!buf) throw BlobError.noPhotoBuffer('photo')

    return await this.uploadImagePassthrough(buf, options, options.filename)
  }

  private async toFormattedBuffer(
    input: Buffer,
    opts: UploadOptions,
    fmt: ImageFormat,
  ): Promise<Buffer> {
    const width = max([1, floor(opts.imageWidth ?? 1024)])
    const quality = min([100, max([1, floor(opts.imageQuality ?? 80)])])
    const progressive = opts.progressive ?? true
    const background = opts.background ?? '#ffffff'
    const strip = opts.stripMetadata ?? true

    let p = sharp(input, { failOn: 'none' }).rotate()
    if (!strip) p = p.withMetadata({ exif: undefined, icc: undefined })
    if (fmt === 'jpeg') {
      p = p.ensureAlpha().flatten({ background })
    }
    p = p.resize({ width, withoutEnlargement: true })
    switch (fmt) {
      case 'jpeg':
        p = p.jpeg({ quality, progressive, mozjpeg: true })
        break
      case 'png':
      case 'bmp':
        p = p.png({ compressionLevel: 9, progressive })
        break
      case 'webp':
        p = p.webp({ quality })
        break
      case 'avif':
        p = p.avif({ quality })
        break
      case 'heif':
        p = p.heif({ quality })
        break
      case 'tiff':
        p = p.tiff({ quality })
        break
      case 'gif':
        p = p.gif({ progressive })
        break
      default:
        p = p.jpeg({ quality, progressive, mozjpeg: true })
    }

    return p.toBuffer()
  }
}

export const blobLogs = BlobService.logs()
export const blobPhoto = BlobService.photos()
