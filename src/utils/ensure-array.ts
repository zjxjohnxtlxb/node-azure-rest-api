import { isArray, isNil } from 'lodash-es'

import type { MaybeArray } from '../interfaces/base.dao.interface.ts'

/**
 * Normalize a MaybeArray<T> to T[].
 * - null/undefined -> []
 * - T -> [T]
 * - T[] -> T[]
 */
const ensureArray = <T>(input: MaybeArray<T>): T[] => {
  if (isNil(input)) return []

  return isArray(input) ? input : [input]
}

export default ensureArray
