import type { Method } from '../lib/base.dao.ts'

const invalidOrderBy = 'Invalid orderBy'
const unknownColumn = 'Unknown column name'
const invalidBetween = 'Invalid between values'

export default class SqlError extends Error {
  constructor(tableName: string, message: string) {
    super(`[SQL: ${tableName}] ${message}`)
    this.name = 'SqlError'
  }

  static actionError(tableName: string, action: Method, message: string): SqlError {
    return new SqlError(tableName, `${message} for ${action}`)
  }

  static invalidOrderBy(tableName: string, action: Method, item: string = 'item'): SqlError {
    return SqlError.actionError(tableName, action, `${invalidOrderBy} ${item}`)
  }

  static invalidOrderByColumn(tableName: string, action: Method): SqlError {
    return SqlError.invalidOrderBy(tableName, action, 'column')
  }

  static invalidBetween(tableName: string, action: Method): SqlError {
    return SqlError.actionError(tableName, action, invalidBetween)
  }

  static unknownColumn(tableName: string, action: Method, item: string): SqlError {
    return SqlError.actionError(tableName, action, `${unknownColumn} ${item}`)
  }
}
