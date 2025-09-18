import { includes } from 'lodash-es'

import { type DaoName, DaoNames } from '../daos/index.ts'

import toPascalCase from './to-pascal-case.ts'

export const tableToDaoName = (table: string): DaoName | null => {
  const guess = `${toPascalCase(table)}Dao` as DaoName
  return includes(DaoNames, guess) ? guess : null
}

export default tableToDaoName
