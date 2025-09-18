/** Allowed types for an entity ID */
export type EntityID = string | number

/** Entity with optional `id` field */
export interface WithID {
  id?: EntityID | null
}

/** Value or array of values */
export type MaybeArray<T> = T | T[] | null | undefined

/** Supported SQL operators */
export type Operator = '=' | '!=' | '<' | '<=' | '>' | '>=' | 'LIKE' | 'IN' | 'NOT IN' | 'BETWEEN'
export type LikeMode = 'raw' | 'contains' | 'startsWith' | 'endsWith'

export type FieldQueryBase<T> = {
  operator?: Exclude<Operator, 'BETWEEN' | 'IN' | 'NOT IN'>
  value: MaybeArray<T>
}

export type FieldQueryBetween<T> = {
  operator: 'BETWEEN'
  value: [T, T]
}

export type FieldQueryIn<T> = {
  operator: 'IN' | 'NOT IN'
  value: T[]
}

/** Field query object with value and optional operator */
export type FieldQuery<T> = FieldQueryBase<T> | FieldQueryBetween<T> | FieldQueryIn<T>

/** Partial entity: each field is optional */
export type Entity<T extends WithID> = Partial<T>

/** Query object: each field can be simple value, array, or FieldQuery */
export type EntityQuery<T extends WithID> = {
  [K in keyof T]?: MaybeArray<T[K]> | FieldQuery<T[K]>
}

/** Single parameter for DAO methods: either an entity or a query */
export type EntityParameter<T extends WithID> = EntityQuery<T> | Entity<T>

/**
 * Parameters accepted by DAO methods:
 * - single entity or query
 * - array of entities or queries
 * This allows flexible usage in insert/update/find methods
 */
export type EntityParameters<T extends WithID> = EntityParameter<T> | EntityParameter<T>[]

export interface IBaseDAO<T extends WithID> {
  findAll(): Promise<T[]>
  find(data: EntityParameters<T>): Promise<T[]>
  insert(data: EntityParameters<T>): Promise<void>
  update(data: EntityParameters<T>): Promise<void>
  delete(data: EntityParameters<T>): Promise<void>
}
