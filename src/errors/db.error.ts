const runtimeError = 'RUNTIME ERROR'
const connectionFailed = 'CONNECTION FAILED'
const retryMessage = (delay: number, retries: number) =>
  `Retrying in ${delay}ms... (${retries} retries left)`
const noRetriesLeft = 'No retries left'

export default class DbError extends Error {
  constructor(errorType: string, message: string) {
    super(`[Db: ${errorType}] ${message}`)
    this.name = 'DbError'
  }

  static runtimeError(message: string): DbError {
    return new DbError(runtimeError, message)
  }

  static connectionError(message: string): DbError {
    return new DbError(connectionFailed, `${noRetriesLeft}: ${message}`)
  }

  static connectionWarn(delay: number, retries: number): DbError {
    return new DbError(connectionFailed, retryMessage(delay, retries))
  }
}
