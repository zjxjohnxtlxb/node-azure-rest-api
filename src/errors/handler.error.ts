const deleteFailed = 'Failed to delete log or blob'

export default class HandlerError extends Error {
  constructor(className: string, message: string) {
    super(`[Hander: ${className}] ${message}`)
    this.name = 'HanderError'
  }

  static deleteFailed(className: string): HandlerError {
    return new HandlerError(className, deleteFailed)
  }
}
