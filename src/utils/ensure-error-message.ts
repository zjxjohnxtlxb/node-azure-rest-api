import { attempt, isError, isObject } from 'lodash-es'

/**
 * Ensure error is converted to a human-readable message.
 */
const ensureErrorMessage = (error: unknown, defaultValue?: string): string => {
  if (error instanceof Error) return error.message
  if (isObject(error) && 'message' in error) {
    const m = error.message
    if (typeof m === 'string') return m
  }
  const str = attempt(() => String(error))
  if (!isError(str)) return str as string

  const json = attempt(() => JSON.stringify(error))
  if (!isError(json)) return json as string

  return defaultValue ?? 'Unknown error'
}

export default ensureErrorMessage
