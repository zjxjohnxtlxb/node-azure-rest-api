import {
  size,
  filter,
  join,
  map,
  isArray,
  forEach,
  every,
  groupBy,
  mapValues,
  omit,
  values,
  sortBy,
  pick,
  isNil,
  flatMap,
  uniq,
  includes,
  chunk,
} from 'lodash-es'

import SqlError from '../errors/sql.error.ts'
import ensureArray from '../utils/ensure-array.ts'

import type {
  EntityID,
  EntityParameter,
  EntityParameters,
  FieldQuery,
  LikeMode,
  MaybeArray,
  Operator,
  WithID,
} from '../interfaces/base.dao.interface.ts'
import type { Method } from '../lib/base.dao.ts'

export type ValueKey<T> = { value: T[keyof T]; key: keyof T }
export type KeyValuePair<T> = [number | string, T[keyof T]] | [number | string, ValueKey<T>]
export type QueryType<T> = { sql: string; params: Record<string, ValueKey<T>> }

const DEFAULT_TAKE = Number(process.env.AZURE_SQL_LIMIT ?? 100) // pagination default
const DEFAULT_IN_CHUNK = Number(process.env.AZURE_SQL_IN_CHUNK ?? 800) // IN(...) chunk size default
const DEFAULT_LIKE_MODE = 'raw'
const DEFAULT_UNKNOWN_COLUMN: 'ignore' | 'throw' = 'ignore'

const schemaInfo = `
WITH PK_UQ AS (
    SELECT 
        kcu.TABLE_SCHEMA,
        kcu.TABLE_NAME,
        kcu.COLUMN_NAME,
        tc.CONSTRAINT_TYPE
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
    JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
      ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
),
FK AS (
    SELECT 
        rc.CONSTRAINT_NAME,
        fkcu.TABLE_NAME AS FK_Table,
        fkcu.COLUMN_NAME AS FK_Column,
        fkcu2.TABLE_NAME AS Ref_Table,
        fkcu2.COLUMN_NAME AS Ref_Column
    FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fkcu
      ON rc.CONSTRAINT_NAME = fkcu.CONSTRAINT_NAME
    JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fkcu2
      ON rc.UNIQUE_CONSTRAINT_NAME = fkcu2.CONSTRAINT_NAME
)
SELECT 
    c.TABLE_SCHEMA,
    c.TABLE_NAME,
    c.COLUMN_NAME,
    c.DATA_TYPE,
    c.IS_NULLABLE,
    c.CHARACTER_MAXIMUM_LENGTH,
    COLUMNPROPERTY(object_id(c.TABLE_SCHEMA+'.'+c.TABLE_NAME), c.COLUMN_NAME, 'IsIdentity') AS IsIdentity,
    CASE WHEN pk.CONSTRAINT_TYPE='PRIMARY KEY' THEN 1 ELSE 0 END AS IsPrimary,
    CASE WHEN pk.CONSTRAINT_TYPE='UNIQUE' THEN 1 ELSE 0 END AS IsUnique,
    fk.Ref_Table AS FK_RefTable,
    fk.Ref_Column AS FK_RefColumn
FROM INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN PK_UQ pk
  ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA
 AND c.TABLE_NAME = pk.TABLE_NAME
 AND c.COLUMN_NAME = pk.COLUMN_NAME
LEFT JOIN FK fk
  ON c.TABLE_NAME = fk.FK_Table
 AND c.COLUMN_NAME = fk.FK_Column
WHERE c.TABLE_NAME NOT LIKE '%schema_history%'
AND c.TABLE_SCHEMA = 'dbo'
ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION;
`
const applyLikeValue = (raw: unknown, likeMode: LikeMode, escapeChar?: string): string => {
  // Coerce to string; undefined/null -> empty string (safe no-match unless wildcarded)
  let v = String(raw ?? '')
  if (escapeChar) {
    // escape the escape char itself, then % and _
    v = v
      .replaceAll(escapeChar, escapeChar + escapeChar)
      .replaceAll('%', escapeChar + '%')
      .replaceAll('_', escapeChar + '_')
  }
  switch (likeMode) {
    case 'contains':
      return `%${v}%`
    case 'startsWith':
      return `${v}%`
    case 'endsWith':
      return `%${v}`
    default:
      return v // 'raw'
  }
}

/**
 * Map over each entity item and its properties with strict types
 * Flattens results so that the return type is KeyValuePair<T>[], even if callback returns KeyValuePair<T> or KeyValuePair<T>[]
 *
 * @param entityParameters - single entity or array of entities
 * @param callback - function called for each property of each entity
 * @returns flat array of results
 */
export const mapEntityItems = <T extends WithID>(
  entityParameters: EntityParameters<T>,
  callback: (
    value: MaybeArray<T[keyof T]>,
    key: keyof T,
    operator: Operator,
    itemIndex: number,
  ) => KeyValuePair<T> | KeyValuePair<T>[],
): KeyValuePair<T>[] => {
  // Ensure input is always an array
  const items = ensureArray(entityParameters)
  const result: KeyValuePair<T>[] = []

  forEach(items, (item, itemIndex) => {
    forEach(item, (itemValue, itemKey) => {
      const keyTyped = itemKey as keyof T
      let maybeValuesArray: MaybeArray<T[keyof T]> = itemValue as T[keyof T]
      let operator: Operator = '='

      if (itemValue && typeof itemValue === 'object' && 'value' in itemValue) {
        const fq = itemValue as FieldQuery<T[keyof T]>
        maybeValuesArray = fq.value
        operator = fq.operator ?? '='
      }

      const callbackResult = callback(maybeValuesArray, keyTyped, operator, itemIndex)

      if (isArray(callbackResult) && every(callbackResult, isArray)) {
        result.push(...(callbackResult as KeyValuePair<T>[]))
      } else {
        result.push(callbackResult as KeyValuePair<T>)
      }
    })
  })

  return result
}

/**
 * Generate SQL fragment strings for each entity property
 * Example output: ["[name_0] = @name_0", "[tags_0] IN (tags_0_0, tags_0_1)"]
 */
export const toGroupParametersSql = <T extends WithID>(
  tableName: string,
  action: Method,
  entityParameters: EntityParameters<T>,
  options?: {
    escapeChar?: string
    columnsList?: string[] // whitelist of valid columns
    onUnknownColumn?: 'ignore' | 'throw'
    inChunkSize?: number // split large IN lists
  },
): Record<string, string[]> => {
  const escapeChar = options?.escapeChar
  const columnsList = options?.columnsList
  const onUnknown = options?.onUnknownColumn ?? DEFAULT_UNKNOWN_COLUMN
  const inChunkSize = options?.inChunkSize ?? DEFAULT_IN_CHUNK

  const keyValueArray: KeyValuePair<T>[] = mapEntityItems(
    entityParameters,
    (itemValue, itemKey, operator, itemIndex) => {
      const keyStr = String(itemKey)
      if (columnsList && !includes(columnsList, keyStr)) {
        if (onUnknown === 'ignore') return [itemIndex, '1=1'] as KeyValuePair<T>
        throw SqlError.unknownColumn(tableName, action, keyStr)
      }
      const groupKey = `${keyStr}_${itemIndex}`
      const esc = escapeChar ? ` ESCAPE '${escapeChar}'` : ''

      if (isArray(itemValue)) {
        if (!size(itemValue)) {
          if (operator === '!=' || operator === 'NOT IN') {
            return [itemIndex, '1=1'] as KeyValuePair<T> // always true
          }

          return [itemIndex, '1=0'] as KeyValuePair<T> // always false
        }
        if (operator === 'BETWEEN') {
          if (size(itemValue) !== 2) {
            throw SqlError.invalidBetween(tableName, action)
          }

          return [
            itemIndex,
            `[${keyStr}] BETWEEN IIF(@${groupKey}_0 < @${groupKey}_1, @${groupKey}_0, @${groupKey}_1) AND IIF(@${groupKey}_0 > @${groupKey}_1, @${groupKey}_0, @${groupKey}_1)`,
          ] as KeyValuePair<T>
        } else if (operator === 'LIKE') {
          const likes = join(
            map(itemValue, (_, i) => `[${keyStr}] LIKE @${groupKey}_${i}${esc}`),
            ' OR ',
          )

          return [itemIndex, `(${likes})`] as KeyValuePair<T>
        } else {
          const chunks = chunk(itemValue, inChunkSize)
          const chunkClauses = map(chunks, (arr, cidx) => {
            const placeholders = join(
              map(arr, (_v, i) => `@${groupKey}_${cidx}_${i}`),
              ', ',
            )
            if (operator === '!=' || operator === 'NOT IN') {
              return `[${keyStr}] NOT IN (${placeholders})`
            }

            return `[${keyStr}] IN (${placeholders})` // default includes explicit 'IN'
          })
          // For IN -> OR the chunks; for NOT IN -> AND the chunks
          const glue = operator === 'NOT IN' ? ' AND ' : ' OR '

          return [itemIndex, `(${join(chunkClauses, glue)})`] as KeyValuePair<T>
        }
      } else {
        if (isNil(itemValue)) {
          if (operator === '!=' || operator === 'NOT IN') {
            return [itemIndex, `[${keyStr}] IS NOT NULL`] as KeyValuePair<T>
          }

          return [itemIndex, `[${keyStr}] IS NULL`] as KeyValuePair<T>
        }

        if (operator === 'LIKE') {
          return [itemIndex, `[${keyStr}] LIKE @${groupKey}${esc}`] as KeyValuePair<T>
        }

        return [itemIndex, `[${keyStr}] ${operator} @${groupKey}`] as KeyValuePair<T>
      }
    },
  )

  const grouped: Record<string, KeyValuePair<T>[]> = groupBy(keyValueArray, 0)

  return mapValues(grouped, (arr) => map(arr, ([, value]) => String(value)))
}

export const getColumnLists = <T extends WithID>(
  entityParameters: EntityParameters<T>,
): Record<string, string[]> => {
  const keyValueArray: KeyValuePair<T>[] = mapEntityItems(
    entityParameters,
    (itemValue, itemKey, _, itemIndex) => {
      const keyStr = String(itemKey)

      return [itemIndex, keyStr] as KeyValuePair<T>
    },
  )

  const grouped: Record<string, KeyValuePair<T>[]> = groupBy(keyValueArray, 0)

  return mapValues(grouped, (arr) => map(arr, ([, value]) => String(value)))
}

// Group entityParameters by sorted column list (to allow batch process phrases by structure)
export const groupEntityParametersBySortedColumns = <T extends WithID>(
  entityParameters: EntityParameters<T>,
): Record<string, string[]> => {
  const columnLists = getColumnLists(entityParameters)
  const reversedGroups: Record<string, string[]> = {}

  forEach(columnLists, (columns, key) => {
    const sortedKey = JSON.stringify(sortBy(columns))
    if (!reversedGroups[sortedKey]) reversedGroups[sortedKey] = []
    reversedGroups[sortedKey].push(key)
  })

  return reversedGroups
}

/**
 * Convert entity parameters into a flat array of [key, value] tuples
 * Suitable for Object.fromEntries or setting parameters in a request object
 */
export const toGroupParametersKeyValue = <T extends WithID>(
  tableName: string,
  action: Method,
  entityParameters: EntityParameters<T>,
  options?: { inChunkSize?: number; likeMode?: LikeMode; escapeChar?: string },
): KeyValuePair<T>[] => {
  const inChunkSize = options?.inChunkSize ?? DEFAULT_IN_CHUNK
  const likeMode = options?.likeMode ?? DEFAULT_LIKE_MODE
  const escapeChar = options?.escapeChar

  return mapEntityItems(entityParameters, (itemValue, itemKey, operator, itemIndex) => {
    const keyStr = String(itemKey)
    const groupKey = `${keyStr}_${itemIndex}`

    if (isArray(itemValue)) {
      if (!size(itemValue)) {
        return [] as KeyValuePair<T>[] // empty arrays contribute no params
      }

      if (operator === 'BETWEEN') {
        if (size(itemValue) !== 2) {
          throw SqlError.invalidBetween(tableName, action)
        }

        return map(itemValue, (v, i) => [
          `${groupKey}_${i}`,
          { value: v, key: keyStr },
        ]) as KeyValuePair<T>[]
      } else if (operator === 'LIKE') {
        return map(itemValue, (v, i) => [
          `${groupKey}_${i}`,
          { value: applyLikeValue(v, likeMode, escapeChar), key: keyStr },
        ]) as KeyValuePair<T>[]
      } else {
        const chunksArr = chunk(itemValue, inChunkSize)
        const kvs: KeyValuePair<T>[] = []
        forEach(chunksArr, (arr, cidx) => {
          forEach(arr, (v, i) => {
            kvs.push([`${groupKey}_${cidx}_${i}`, { value: v, key: keyStr }] as KeyValuePair<T>)
          })
        })

        return kvs
      }
    } else {
      if (isNil(itemValue)) {
        return [] as KeyValuePair<T>[] // IS NULL/IS NOT NULL requires no params
      }

      if (operator === 'LIKE') {
        return [
          [groupKey, { value: applyLikeValue(itemValue, likeMode, escapeChar), key: keyStr }],
        ] as KeyValuePair<T>[]
      }

      return [[groupKey, { value: itemValue, key: keyStr }]] as KeyValuePair<T>[]
    }
  })
}

export const prepareUpdateEntityParamsByKey = <T extends WithID>(
  entityParameters: EntityParameters<T>,
  keyColumn: keyof T = 'id',
): EntityParameters<T> => {
  const items = ensureArray(entityParameters)

  return flatMap(items, (item) => {
    const keyItem = item[keyColumn]
    if (isNil(keyItem)) return []

    const keyColumns = ensureArray(keyItem)

    return map(keyColumns, (k) => ({ ...item, [keyColumn]: k }))
  })
}

export const entityParametersFilter = <T extends WithID>(
  entityParameters: EntityParameters<T>,
  columnFiltered: MaybeArray<string> = 'id',
  operator: 'IN' | 'OUT' = 'OUT',
): EntityParameters<T> => {
  const items = ensureArray(entityParameters)
  const filterFunc: (obj: EntityParameters<T>, keys: string | string[]) => EntityParameters<T> =
    operator === 'IN' ? pick : omit
  if (isNil(columnFiltered)) return items

  return map(items, (item) => filterFunc(item, columnFiltered)) as EntityParameters<T>
}

export const mergeEntityParametersById = <T extends WithID>(
  entityParameters: EntityParameters<T>,
): EntityParameters<T> => {
  const ids: MaybeArray<T['id']>[] = []
  const items = ensureArray(entityParameters)
  forEach(items, (item) => {
    const keyItem = item['id']
    if (!isNil(keyItem)) {
      const keyColumns = ensureArray<EntityID>(keyItem as EntityID)
      ids.push(...keyColumns)
    }
  })
  const uniqIds = uniq(ids)
  const entityParameter = { id: uniqIds } as EntityParameter<T>

  return [entityParameter]
}

/** Generate columns et values statements SQL from grouped column structure
 *  From grouped columns produce "([c1], [c2]) VALUES (...)" segments.
 */
export const buildColumnsAndValuesSqlStatements = (
  grouped: Record<string, string[]>,
): { columnSql: string; valuesSql: string; columns: string[] }[] => {
  return map(grouped, (ids, sortedKey) => {
    const columns: string[] = JSON.parse(sortedKey)
    const columnSql = `([${join(columns, '],  [')}])`
    const valuesSql = map(ids, (id) => {
      const placeholders = map(columns, (col) => `@${col}_${id}`)

      return `(${join(placeholders, ', ')})`
    })

    return { columnSql, valuesSql: join(valuesSql, ', '), columns }
  })
}

// Generate insert SQL columns et values statements from grouped column structure
export const buildInsertSqlStatements = (grouped: Record<string, string[]>): string[] => {
  const columnsAndValuesSqlStatements = buildColumnsAndValuesSqlStatements(grouped)

  return map(columnsAndValuesSqlStatements, ({ columnSql, valuesSql }) => {
    return `${columnSql} VALUES ${valuesSql}`
  })
}

// Generate update SQL columns et values statements from grouped column structure
export const buildUpdateSqlStatements = <T extends WithID>(
  grouped: Record<string, string[]>,
  keyColumn: keyof T = 'id',
): string[] => {
  const columnsAndValuesSqlStatements = buildColumnsAndValuesSqlStatements(grouped)

  return map(columnsAndValuesSqlStatements, ({ columnSql, valuesSql, columns }) => {
    const keyStr = String(keyColumn)
    const columnForSet = map(
      filter(columns, (c) => c !== keyStr),
      (col) => `${col} = source.${col}`,
    )

    return `USING (VALUES
    ${valuesSql}
  ) AS source${columnSql} 
   ON target.${keyStr} = source.${keyStr}
   WHEN MATCHED THEN
  UPDATE SET ${join(columnForSet, ', ')}`
  })
}

export const buildOrderBySql = <T extends WithID>(
  tableName: string,
  columnsList: string[],
  options?: {
    skip?: number
    take?: number
    orderBy?: (keyof T | string | { column: keyof T | string; direction?: 'ASC' | 'DESC' })[]
    defaultOrderBy?: string
  },
) => {
  let orderBySql = ''
  if (options?.orderBy && size(options.orderBy)) {
    orderBySql =
      'ORDER BY ' +
      join(
        map(options.orderBy, (o) => {
          let column: string
          let direction: 'ASC' | 'DESC' = 'ASC'

          if (typeof o === 'string') {
            column = o
          } else if (typeof o === 'object' && 'column' in o) {
            column = String(o.column)
            direction = o.direction ?? 'ASC'
          } else {
            throw SqlError.invalidOrderBy(tableName, 'find')
          }
          if (!includes(columnsList, column)) {
            throw SqlError.invalidOrderByColumn(tableName, 'find')
          }

          return `[${column}] ${direction}`
        }),
        ', ',
      )
  } else if (options?.skip || options?.take) {
    orderBySql = `ORDER BY [${options?.defaultOrderBy ?? 'created_at'}] ASC`
  }

  return orderBySql
}

export const getParams = <T extends WithID>(
  tableName: string,
  action: Method,
  entityParameters: EntityParameters<T>,
  options?: { inChunkSize?: number; likeMode?: LikeMode; escapeChar?: string },
): Record<string, ValueKey<T>> => {
  const keyValueArray: KeyValuePair<T>[] = toGroupParametersKeyValue(
    tableName,
    action,
    entityParameters,
    options,
  )
  const params: Record<string, ValueKey<T>> = {}
  forEach(keyValueArray, ([key, v]) => {
    const { value, key: paramKey } = v as ValueKey<T>
    params[key] = { value, key: paramKey }
  })

  return params
}

export const insertSql = <T extends WithID>(
  tableName: string,
  entityParameters: EntityParameters<T>,
  columnFiltered: MaybeArray<string> = 'id',
  options?: { inChunkSize?: number; likeMode?: LikeMode; escapeChar?: string },
): QueryType<T> => {
  const entityParametersFiltered = entityParametersFilter(entityParameters, columnFiltered)
  const baseSql = `INSERT INTO [${tableName}]`
  const params = getParams(tableName, 'insert', entityParametersFiltered, options)
  const reversedColumnLists = groupEntityParametersBySortedColumns(entityParametersFiltered)
  const sqlStatements = buildInsertSqlStatements(reversedColumnLists)
  const insertSqlStatement = map(sqlStatements, (sqlStatement) => `${baseSql} ${sqlStatement};`)

  return { sql: `${join(insertSqlStatement, '\n')}`, params }
}

export const updateByKeySql = <T extends WithID>(
  tableName: string,
  entityParameters: EntityParameters<T>,
  keyColumn: keyof T = 'id',
  options?: { inChunkSize?: number; likeMode?: LikeMode; escapeChar?: string },
): QueryType<T> => {
  let preparedUpdateEntityParams
  if (keyColumn !== 'id') {
    const entityParametersFiltered = entityParametersFilter(entityParameters, 'id')
    preparedUpdateEntityParams = prepareUpdateEntityParamsByKey(entityParametersFiltered, keyColumn)
  } else {
    preparedUpdateEntityParams = prepareUpdateEntityParamsByKey(entityParameters)
  }
  const baseSql = `MERGE INTO [${tableName}] AS target`
  const params = getParams(tableName, 'update', preparedUpdateEntityParams, options)
  const reversedColumnLists = groupEntityParametersBySortedColumns(preparedUpdateEntityParams)
  const sqlStatements = buildUpdateSqlStatements(reversedColumnLists, keyColumn)
  const insertSqlStatement = map(sqlStatements, (sqlStatement) => `${baseSql} ${sqlStatement};`)

  return { sql: `${join(insertSqlStatement, '\n')}`, params }
}

export const deleteByIdSql = <T extends WithID>(
  tableName: string,
  entityParameters: EntityParameters<T>,
  options?: { inChunkSize?: number },
): QueryType<T> => {
  const entityParametersFiltered = entityParametersFilter(entityParameters, 'id', 'IN')
  // merge all entity ids in an array uniq
  const mergedEntityParameter = mergeEntityParametersById(entityParametersFiltered)
  const baseSql = `DELETE FROM [${tableName}]`
  const params = getParams(tableName, 'delete', mergedEntityParameter, {
    inChunkSize: options?.inChunkSize,
  })
  const groupedParametersSql = toGroupParametersSql(tableName, 'delete', mergedEntityParameter, {
    inChunkSize: options?.inChunkSize,
  })
  const sqlStatements = map(
    values(groupedParametersSql),
    (conditions) => `${baseSql} WHERE (${join(conditions, ' AND ')});`,
  )

  return { sql: `${join(sqlStatements, '\n')}`, params }
}

export const selectByColumnsSql = <T extends WithID>(
  tableName: string,
  columnsList: string[],
  entityParameters: EntityParameters<T> = [],
  options?: {
    skip?: number
    take?: number
    orderBy?: (keyof T | string | { column: keyof T | string; direction?: 'ASC' | 'DESC' })[]
    defaultOrderBy?: string
    inChunkSize?: number
    likeMode?: LikeMode
    escapeChar?: string
  },
): QueryType<T> => {
  const baseSql = `SELECT * FROM [${tableName}]`
  const orderBySql = buildOrderBySql(tableName, columnsList, {
    skip: options?.skip,
    take: options?.take,
    orderBy: options?.orderBy,
    defaultOrderBy: options?.defaultOrderBy,
  })
  const skip = options?.skip ?? 0
  const take = options?.take ?? DEFAULT_TAKE
  const pagingSql =
    !isNil(options?.skip) || !isNil(options?.take)
      ? `OFFSET ${skip} ROWS FETCH NEXT ${take} ROWS ONLY`
      : ''
  let sqlText = ''
  if (!size(entityParameters)) {
    sqlText = `
    ${baseSql}
    ${orderBySql}
    ${pagingSql};
  `

    return { sql: sqlText, params: {} }
  }

  const params = getParams(tableName, 'find', entityParameters, {
    inChunkSize: options?.inChunkSize,
    likeMode: options?.likeMode,
    escapeChar: options?.escapeChar,
  })
  const groupedParametersSql = toGroupParametersSql(tableName, 'find', entityParameters, {
    inChunkSize: options?.inChunkSize,
    columnsList,
    onUnknownColumn: 'throw',
    escapeChar: options?.escapeChar,
  })
  const sqlStatements = map(
    values(groupedParametersSql),
    (conditions) => `(${join(conditions, ' AND ')})`,
  )
  const whereSql = `WHERE ${join(sqlStatements, ' OR ')}`
  sqlText = `
    ${baseSql}
    ${whereSql}
    ${orderBySql}
    ${pagingSql};
  `

  return { sql: sqlText, params }
}

export const sqlUtils = {
  mapEntityItems,
  toGroupParametersKeyValue,
  toGroupParametersSql,
  getColumnLists,
  groupEntityParametersBySortedColumns,
  prepareUpdateEntityParamsByKey,
  entityParametersFilter,
  mergeEntityParametersById,
  buildColumnsAndValuesSqlStatements,
  buildInsertSqlStatements,
  buildUpdateSqlStatements,
  buildOrderBySql,
  getParams,
}

export const sqlBuild = {
  insertSql,
  updateByKeySql,
  deleteByIdSql,
  selectByColumnsSql,
}

export const sqlQuery = {
  schemaInfo,
}

const sql = {
  build: sqlBuild,
  query: sqlQuery,
}

export default sql
