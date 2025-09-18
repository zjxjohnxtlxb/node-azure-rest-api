//import { exampleHooksConfig } from './example.hooks.ts'

import type { Hooks } from '../../lib/base.dao.ts'
import type { DaoConstructorMap, EntityOfDao } from '../index.ts'

const daoHooksConfigs: Partial<{ [K in keyof DaoConstructorMap]?: Hooks<EntityOfDao<K>> }> = {
  //exampleDao: exampleConfig,
}

export default daoHooksConfigs
