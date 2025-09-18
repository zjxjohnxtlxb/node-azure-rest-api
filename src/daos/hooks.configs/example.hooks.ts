/* import type { EntityParameters, MaybeArray, WithID } from '../../interfaces/base.dao.interface.ts'
import type { Hooks, Method } from '../../lib/base.dao.ts'


const beforeHookForLog = async <T extends WithID>(
  data: EntityParameters<T>,
  action: Method,
  columnFiltered?: MaybeArray<string>,
): Promise<EntityParameters<T>> => {

  return data
}

export const logsHooksConfig: Hooks<Example> = {
  before: {
    insert: beforeHookForLog,
    delete: beforeHookForLog,
  },
} */

