import type { BlobDataType } from '../lib/blob.ts'

const noPhotoBuffer = 'No photo buffer provided'
const noDataBuffer = 'No data buffer provided'
const uploadFailed = 'Blob upload failed'
const deleteFailed = 'Blob delete failed'
const configMissing = 'Blob config missing'
const crossContainerDelete = (expected: string, got: string) =>
  `Cross-container delete attempt detected: expected "${expected}" but got "${got}" for URL`

export default class BlobError extends Error {
  constructor(blobDataType: BlobDataType, message: string) {
    super(`[Blob: ${blobDataType}] ${message}`)
    this.name = 'BlobError'
  }

  static noDataBuffer(blobDataType: BlobDataType, message: string = noDataBuffer): BlobError {
    return new BlobError(blobDataType, message)
  }

  static noPhotoBuffer(blobDataType: BlobDataType): BlobError {
    return new BlobError(blobDataType, noPhotoBuffer)
  }

  static uploadFailed(blobDataType: BlobDataType, message: string): BlobError {
    return new BlobError(blobDataType, `${uploadFailed}: ${message}`)
  }

  static deleteFailed(blobDataType: BlobDataType, message: string): BlobError {
    return new BlobError(blobDataType, `${deleteFailed}: ${message}`)
  }

  static configMissing(blobDataType: BlobDataType, message: string): BlobError {
    return new BlobError(blobDataType, `${configMissing}: ${message}`)
  }

  static crossContainerDelete(
    blobDataType: BlobDataType,
    expected: string,
    got: string,
  ): BlobError {
    return new BlobError(blobDataType, crossContainerDelete(expected, got))
  }
}
