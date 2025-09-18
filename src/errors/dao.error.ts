import { join, map, toPairs } from 'lodash-es'

import type { Method } from '../lib/base.dao.ts'

const noColumnError = 'No columns'
const noIdError = 'Missing id'
const sqlTypeNotFound = 'SQL type not found'

export default class DaoError extends Error {
  constructor(className: string, message: string) {
    super(`[DAO: ${className}] ${message}`)
    this.name = 'DaoError'
  }

  static validateField(className: string, errors: Record<number, string>[]): DaoError {
    const formatted = map(errors, (err) => {
      const result = []
      for (const [key, value] of toPairs(err)) {
        result.push(`[${key}]: ${value}`)
      }

      return join(result, '\n')
    })

    return new DaoError(className, `\n${join(formatted, '\n')}`)
  }

  static actionError(className: string, action: Method, message: string = noColumnError): DaoError {
    return new DaoError(className, `${message} for ${action}`)
  }

  static noColumn(className: string, action: Method): DaoError {
    return DaoError.actionError(className, action, noColumnError)
  }

  static noId(className: string, action: Method): DaoError {
    return DaoError.actionError(className, action, noIdError)
  }

  static sqlTypeNotFound(className: string, action: Method, paramKey: string): DaoError {
    return DaoError.actionError(className, action, `${sqlTypeNotFound} for ${paramKey}`)
  }
}
