import { fromPairs, map } from 'lodash-es'

import daoHooksConfigs from '../daos/hooks.configs/index.ts'
import { DaoRegistry, type DaoName, type EntityOfDao, DaoNames } from '../daos/index.ts'
import { createDao, type Hooks, type BaseDao, type DaoConstructor } from '../lib/base.dao.ts'

/** Create a single DAO instance by name, wiring hooks if present. */
export function getDao<K extends DaoName>(name: K): BaseDao<EntityOfDao<K>> {
  // Resolve DAO class from the generated registry
  const DaoClass = DaoRegistry[name] as unknown as DaoConstructor<EntityOfDao<K>>

  // Resolve hooks (optional)
  const hooks = daoHooksConfigs[name] as Partial<Hooks<EntityOfDao<K>>> | undefined

  // Delegate to the BaseDao factory to attach hooks; fallback could be: `return new DaoClass()`
  return createDao<EntityOfDao<K>>(DaoClass, hooks)
}

/** Create instances for all DAOs. */
export function getAllDaos(): { [P in DaoName]: BaseDao<EntityOfDao<P>> } {
  const out = map(DaoNames, (name) => [name, getDao(name)])

  return fromPairs(out) as { [P in DaoName]: BaseDao<EntityOfDao<P>> }
}
