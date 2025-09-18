import {
  isNil,
  size,
  pickBy,
  negate,
  keys,
  filter,
  map,
  isEmpty,
  includes,
  every,
  toPairs,
  get,
  head,
  forEach,
  isArray,
  omitBy,
  flatMap,
  snakeCase,
  uniq,
} from 'lodash-es'
import sql from 'mssql'
import { prettifyError } from 'zod'

import DaoError from '../errors/dao.error.ts'
import { sqlBuild } from '../sql/sql.ts'
import ensureArray from '../utils/ensure-array.ts'
import ensureLowerCase from '../utils/ensure-lower-case.ts'

import db from './db.ts'

import type {
  EntityParameters,
  IBaseDAO,
  WithID,
  MaybeArray,
  EntityParameter,
  EntityID,
} from '../interfaces/base.dao.interface.ts'
import type { ValueKey } from '../sql/sql.ts'
import type { ConnectionPool, IResult, Request, Transaction } from 'mssql'
import type { ZodObject, ZodSafeParseError, ZodSafeParseResult, ZodType } from 'zod'

/**
 * Base Data Access Object (DAO) class
 * Provides generic CRUD operations for any table
 * All DAOs should extend this class
 */
export type DaoConstructor<T extends WithID> = new () => BaseDao<T>
export type BeforeHook<T extends WithID> = (
  data: EntityParameters<T>,
  action: Method,
  columnFiltered?: MaybeArray<string>,
) => Promise<EntityParameters<T>>
export type AfterHook<T> = (result: IResult<T>) => Promise<void>
export type Method = 'find' | 'insert' | 'update' | 'delete'
export type HookType = 'before' | 'after'
export type Hooks<T extends WithID> = {
  before?: Partial<Record<Method, BeforeHook<T>>>
  after?: Partial<Record<Method, AfterHook<T>>>
}
export type Relations = {
  primary: string[]
  unique: string[]
  foreignKey: {
    column: string
    table: string
    key: string
  }[]
  description: string
}

type RelationFetchOptions<T extends WithID, R extends string = string> = {
  relations?: readonly R[]
  extraWhere?: Record<string, unknown>
  skip?: number
  take?: number
  orderBy?: (keyof T | string | { column: keyof T | string; direction?: 'ASC' | 'DESC' })[]
}

const { insertSql, updateByKeySql, deleteByIdSql, selectByColumnsSql } = sqlBuild

export abstract class BaseDao<T extends WithID> implements IBaseDAO<T> {
  protected tableName: string
  protected entity: ZodObject
  protected shape: Record<string, ZodType>
  protected hooks: Hooks<T> = {}

  constructor(entity: ZodObject) {
    this.entity = entity
    this.shape = entity.shape
    this.tableName = snakeCase(entity.description ?? '')

    // DEFAULT BEFORE HOOKS
    this.hooks.before = {
      insert: async (data, action = 'insert', columnFiltered) =>
        this.defaultBeforeHook(data, action, columnFiltered),
      update: async (data, action = 'update', columnFiltered = 'id') =>
        this.defaultBeforeHook(data, action, columnFiltered),
      delete: async (data, action = 'delete', columnFiltered = 'id') =>
        this.defaultBeforeHook(data, action, columnFiltered),
      find: async (data, action = 'find', columnFiltered) =>
        this.defaultBeforeHook(data, action, columnFiltered),
    }
  }

  protected get className(): string {
    return this.constructor.name
  }

  public get columnsList(): string[] {
    return keys(this.shape)
  }

  public getRelations(): Relations {
    return this.entity.meta() as Relations
  }

  public async defaultBeforeHook(
    data: EntityParameters<T>,
    action: Method,
    columnFiltered?: MaybeArray<string>,
  ): Promise<EntityParameters<T>> {
    const cleanedParameters = this.cleanParameters(data, columnFiltered)

    if (!size(cleanedParameters) && action !== 'find') {
      if (columnFiltered === 'id') {
        throw DaoError.noId(this.className, action)
      } else {
        throw DaoError.noColumn(this.className, action)
      }
    }

    return this.validateField(cleanedParameters)
  }

  /** Get a new SQL request */
  protected async getRequest(transaction?: Transaction): Promise<Request> {
    if (transaction) {
      return transaction.request()
    }
    const pool: ConnectionPool = await db.getPool()

    return pool.request()
  }
  // -------------------- Hook Management --------------------

  protected getHook<K extends Method>(hookType: 'before', action: K): BeforeHook<T> | undefined
  protected getHook<K extends Method>(hookType: 'after', action: K): AfterHook<T> | undefined
  protected getHook(hookType: HookType, action: Method): BeforeHook<T> | AfterHook<T> | undefined {
    return get(this.hooks, [hookType, action])
  }

  public registerHook<K extends Method>(
    hookType: HookType,
    action: K,
    fn: BeforeHook<T> | AfterHook<T>,
    validate: boolean = false,
  ): void {
    if (!this.hooks[hookType]) {
      this.hooks[hookType] = {}
    }

    if (validate && hookType === 'before') {
      const originalFn = fn as BeforeHook<T>
      this.hooks[hookType]![action] = async (
        data: EntityParameters<T>,
        action: Method,
        columnFiltered?: MaybeArray<string>,
      ) => {
        const cleaned = await this.defaultBeforeHook(data, action, columnFiltered)

        return originalFn(cleaned, action, columnFiltered)
      }
    } else {
      this.hooks[hookType]![action] = fn
    }
  }

  protected async runBefore<K extends Method>(
    data: EntityParameters<T>,
    action: K,
    columnFiltered?: MaybeArray<string>,
  ): Promise<EntityParameters<T>> {
    const hook = this.getHook('before', action)

    return hook ? await hook(data, action, columnFiltered) : data
  }

  protected async runAfter<K extends Method>(action: K, result: IResult<T>): Promise<void> {
    const hook = this.getHook('after', action)
    if (hook) await hook(result)
  }

  /** Insert a record */
  public async insert(data: EntityParameters<T>, transaction?: Transaction): Promise<void> {
    const cleanedParameters = await this.runBefore(data, 'insert')
    const request = await this.getRequest(transaction)
    const insertQuery = insertSql(this.tableName, cleanedParameters)
    const { sql, params } = insertQuery
    this.bindParams(request, params, 'insert')
    const result: IResult<T> = await request.query(sql)
    await this.runAfter('insert', result)
  }

  /** Update a record by ID */
  public async update(data: EntityParameters<T>, transaction?: Transaction): Promise<void> {
    const cleanedParameters = await this.runBefore(data, 'update', 'id')
    const request = await this.getRequest(transaction)
    const updateQuery = updateByKeySql(this.tableName, cleanedParameters)
    const { sql, params } = updateQuery
    this.bindParams(request, params, 'update')
    const result: IResult<T> = await request.query(sql)
    await this.runAfter('update', result)
  }

  /** Delete a record by ID */
  public async delete(data: EntityParameters<T>, transaction?: Transaction): Promise<void> {
    const cleanedParameters = await this.runBefore(data, 'delete', 'id')
    const request = await this.getRequest(transaction)
    const deleteQuery = deleteByIdSql(this.tableName, cleanedParameters)
    const { sql, params } = deleteQuery
    this.bindParams(request, params, 'delete')
    const result: IResult<T> = await request.query(sql)
    await this.runAfter('delete', result)
  }

  /** Find all records */
  public async findAll(
    options?: {
      skip?: number
      take?: number
      orderBy?: (keyof T | string | { column: keyof T | string; direction?: 'ASC' | 'DESC' })[]
    },
    transaction?: Transaction,
  ): Promise<T[]> {
    const request = await this.getRequest(transaction)
    const findQuery = selectByColumnsSql(this.tableName, this.columnsList, undefined, options)
    const { sql } = findQuery
    const result: IResult<T> = await request.query(sql)
    await this.runAfter('find', result)

    return result.recordset
  }

  /** Find records by condition (simple WHERE clause) */
  public async find(
    data: EntityParameters<T> = {},
    options?: {
      skip?: number
      take?: number
      orderBy?: (keyof T | string | { column: keyof T | string; direction?: 'ASC' | 'DESC' })[]
    },
    transaction?: Transaction,
  ): Promise<T[]> {
    const cleanedParameters = await this.runBefore(data, 'find')
    if (!size(cleanedParameters)) return this.findAll(options, transaction)

    const request = await this.getRequest(transaction)
    const findByColumnsQuery = selectByColumnsSql(
      this.tableName,
      this.columnsList,
      cleanedParameters,
      options,
    )
    const { sql, params } = findByColumnsQuery
    this.bindParams(request, params, 'find')
    const result: IResult<T> = await request.query(sql)
    await this.runAfter('find', result)

    return result.recordset
  }

  /**
   * Fetch main table records and automatically load related data
   * based on foreign key definitions (from entity.meta()).
   *
   * - By default, all foreign keys defined in meta.foreignKey will be loaded
   * - You can restrict to specific relations via options.relations
   * - You can add extra filters for related tables via options.extraWhere
   * - Returns: (T & { related: Record<string, WithID[]> })[]
   */
  public async fetchWithRelations<R extends string = string>(
    data: EntityParameters<T> = {},
    options: RelationFetchOptions<T, R> = {},
    transaction?: Transaction,
  ): Promise<Array<T & { related: Record<R, WithID[]> }>> {
    const { relations, extraWhere, skip, take, orderBy } = options
    const mainRecords = await this.find(data, { skip, take, orderBy }, transaction)
    if (!size(mainRecords)) return [] as Array<T & { related: Record<R, WithID[]> }>
    /**
     * relations :
     * Restrict which relations to load.
     * - Omit: load all relations defined in meta.foreignKey
     * - Provide: only load these tables
     *
     * extraWhere :
     * Extra conditions for related tables.
     * key = related table name
     * value = EntityQuery for that DAO
     *
     * Example:
     * extraWhere: {
     *   photos: { is_deleted: 0 },
     *   users: { is_active: 1 }
     * }
     */
    const { foreignKey = [] } = this.getRelations() ?? { foreignKey: [] }
    let fks = foreignKey
    if (size(relations)) {
      const allow = uniq(relations)
      fks = filter(foreignKey, (fk) => includes(allow, fk.table))
    }
    if (!size(fks)) {
      return map(mainRecords, (rec) => ({ ...rec, related: {} as Record<R, WithID[]> }))
    }

    const tableToValues: Record<string, Set<EntityID>> = {}
    for (const fk of fks) {
      const setForTable = get(tableToValues, fk.table) ?? new Set<EntityID>()
      for (const rec of mainRecords) {
        const v = get(rec, fk.column)
        if (!isNil(v)) setForTable.add(v)
      }
      if (size(setForTable)) tableToValues[fk.table] = setForTable
    }

    const related: Record<string, WithID[]> = {}
    const { getDao } = await import('./dao-factory.ts')
    const { tableToDaoName } = await import('../utils/table-to-dao-name.ts')

    await Promise.all(
      map(fks, async (fk) => {
        const values = get(tableToValues, fk.table)
        if (!size(values)) {
          related[fk.table] = []
          return
        }
        const daoName = tableToDaoName(fk.table) // e.g. 'users' -> 'UsersDao'
        if (!daoName) {
          related[fk.table] = []
          return
        }

        const dao = getDao(daoName)
        const where = this.buildInQuery(fk.key, [...values])
        const extra = (extraWhere && extraWhere[fk.table]) || {}
        const merged = size(extra) ? { ...where, ...extra } : where

        related[fk.table] = await dao.find(merged, undefined, transaction)
      }),
    )

    return map(mainRecords, (rec) => {
      const out = { ...rec, related: {} as Record<R, WithID[]> }
      for (const fk of fks) {
        const arr = get(related, fk.table, [])
        const val = get(rec, fk.column)
        const matched = isNil(val) ? [] : filter(arr, (record) => get(record, fk.key) === val)

        out.related[fk.table as R] = matched
      }
      return out as T & { related: Record<R, WithID[]> }
    })
  }

  public buildInQuery(column: string, values: Iterable<unknown>) {
    return { [column as string]: { operator: 'IN', value: values } }
  }

  public validateField(data: EntityParameters<T>): EntityParameters<T> {
    const partialEntity = this.entity.partial()
    const items = ensureArray(data)
    const validated: EntityParameter<T>[] = []
    const errorMessages: Record<number, string>[] = []
    forEach(items, (item, index) => {
      const arrayFields = pickBy(item, isArray)
      const rest = omitBy(item, isArray)
      const restResult = partialEntity.safeParse(rest)
      const arrayResult = flatMap(arrayFields, (field, key) =>
        map(field, (value) => partialEntity.safeParse({ [key]: value })),
      )
      const results = [restResult, ...arrayResult]
      const failedResults = filter(
        results,
        (result: ZodSafeParseResult<EntityParameters<T>>) => !result.success,
      ) as ZodSafeParseError<T>[]
      if (size(failedResults) === 0) {
        validated.push(item)
      } else {
        const messages = map(failedResults, (failedResult) => ({
          [index]: prettifyError(failedResult.error),
        }))

        errorMessages.push(...messages)
      }
    })
    if (size(errorMessages) > 0) {
      throw DaoError.validateField(this.className, errorMessages)
    }

    return validated
  }

  private bindParams(request: Request, params: Record<string, ValueKey<T>>, action: Method) {
    // Assign all flattened key-value pairs into the request object
    for (const [key, v] of toPairs(params)) {
      const { value, key: paramKey } = v as ValueKey<T>
      const sqlType = this.getSqlType(paramKey)

      const sqlKeys = keys(sql.TYPES)

      const typeKey = head(filter(sqlKeys, (t) => ensureLowerCase(t) === sqlType))
      if (!typeKey) throw DaoError.sqlTypeNotFound(this.className, action, String(paramKey))

      request.input(key, get(sql.TYPES, typeKey), value)
    }

    return request
  }

  private getZodColumn(key: keyof T): ZodType {
    return get(this.shape, key)
  }

  private getSqlType(key: keyof T): string {
    const zodColumn = this.getZodColumn(key)

    return get(zodColumn.meta(), 'sqlType') as string
  }
  /**
   * Remove `null` and `undefined` values from entity or array of entities
   */
  private cleanParameters(
    data: EntityParameters<T>,
    columnFiltered?: MaybeArray<string>,
  ): EntityParameters<T> {
    const items = ensureArray(data)
    const filtered: EntityParameters<T> = filter(
      map(items, (item) => pickBy(item, negate(isNil))),
      negate(isEmpty),
    )
    if (columnFiltered) {
      const columnsFiltered = ensureArray(columnFiltered)
      return filter(filtered, (item) => {
        const columns = keys(item)
        return every(columnsFiltered, (c) => includes(columns, c))
      }) as EntityParameters<T>
    }

    return filtered
  }
}

export const createDao = <T extends WithID>(
  daoClass: DaoConstructor<T>,
  hooksConfig?: Partial<Hooks<T>>,
): BaseDao<T> => {
  const dao = new daoClass()
  if (hooksConfig) {
    for (const hookType of ['before', 'after'] as const) {
      const hooks = hooksConfig[hookType]
      if (!hooks) continue

      for (const [action, fn] of toPairs(hooks)) {
        dao.registerHook(hookType, action as Method, fn)
      }
    }
  }
  return dao
}
