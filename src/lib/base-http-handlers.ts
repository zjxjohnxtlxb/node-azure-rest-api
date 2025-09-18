/* Base HTTP handlers for DAO routes (shared by all generated functions) */

import {
  app,
  type HttpRequest,
  type HttpResponseInit,
  type InvocationContext,
} from '@azure/functions'
import { filter, get, head, isNil, map, size, split, trim, upperCase } from 'lodash-es'

import ensureErrorMessage from '../utils/ensure-error-message.ts'
import ensureLowerCase from '../utils/ensure-lower-case.ts'

import { getDao } from './dao-factory.ts'

import type { BaseDao } from './base.dao.ts'
import type { DaoName, EntityOfDao } from '../daos/index.ts'
import type { EntityParameters } from '../interfaces/base.dao.interface.ts'

const ok = (body: unknown, status = 200): HttpResponseInit => ({ status, jsonBody: body })
const bad = (message: string, status = 400): HttpResponseInit => ({
  status,
  jsonBody: { error: message },
})

const parseJson = async <T>(req: HttpRequest): Promise<T | null> => {
  try {
    return (await req.json()) as T
  } catch {
    return null
  }
}

const parseBool = (q: string | null, def: boolean): boolean => {
  if (isNil(q)) return def
  const v = ensureLowerCase(q)
  if (v === 'true') return true
  if (v === 'false') return false
  return def
}

const cleanSplit = (q: string | null, separator: string = ',') =>
  filter(
    map(split(q ?? '', separator), (s) => trim(s)),
    Boolean,
  )

// orderBy=created_at:DESC,name:ASC
const parseOrderBy = (q: string | null) => {
  if (!q) return undefined

  const parts = cleanSplit(q, ',')

  if (!size(parts)) return undefined

  return map(parts, (p) => {
    const [col, dir] = split(p, ':')
    if (!dir) return col

    const d = upperCase(dir) === 'DESC' ? 'DESC' : 'ASC'

    return { column: col, direction: d as 'ASC' | 'DESC' }
  })
}

/**
 * CRUD HTTP endpoints for a specific DAO.
 *
 * Routes:
 *   - GET/POST/PUT/DELETE /api/{routeBase}
 *   - GET/PUT/DELETE     /api/{routeBase}/{id}
 *
 * Query (GET):
 *   - withRelations=true|false   // default: true
 *   - relations=a,b,c            // only when withRelations=true; missing => load all FK
 *   - skip, take, orderBy
 */
export function BaseHttpHandlers<K extends DaoName>(daoName: K, routeBase: string) {
  type E = EntityOfDao<K>
  const dao = getDao(daoName as DaoName) as unknown as BaseDao<E>

  app.http(`${routeBase}-router`, {
    methods: ['GET', 'POST', 'PUT', 'DELETE'],
    authLevel: 'function',
    route: `${routeBase}/{id?}`,
    handler: async (req: HttpRequest, ctx: InvocationContext): Promise<HttpResponseInit> => {
      try {
        const { params, query } = req
        const id = get(params, 'id')
        const withRelations = parseBool(query.get('withRelations'), true)
        const relations = cleanSplit(query.get('relations'), ',')
        const skip = query.get('skip') ? Number(query.get('skip')) : undefined
        const take = query.get('take') ? Number(query.get('take')) : undefined
        const orderBy = parseOrderBy(query.get('orderBy'))

        switch (upperCase(req.method)) {
          case 'GET': {
            if (id) {
              if (withRelations) {
                const rows = await dao.fetchWithRelations({ id: [id] } as EntityParameters<E>, {
                  relations,
                  skip,
                  take,
                  orderBy,
                })
                return ok(head(rows) ?? null)
              } else {
                const rows = await dao.find({ id: [id] } as EntityParameters<E>, {
                  skip,
                  take,
                  orderBy,
                })
                return ok(head(rows) ?? null)
              }
            } else {
              if (withRelations) {
                const rows = await dao.fetchWithRelations({}, { relations, skip, take, orderBy })
                return ok(rows)
              } else {
                const rows = await dao.find({}, { skip, take, orderBy })
                return ok(rows)
              }
            }
          }

          case 'POST': {
            const body: EntityParameters<E> | null = await parseJson<EntityParameters<E>>(req)
            if (!body) return bad('Invalid JSON')
            await dao.insert(body)
            return ok({ ok: true }, 201)
          }

          case 'PUT': {
            const body = await parseJson<EntityParameters<E>>(req)
            if (!body) return bad('Invalid JSON')
            const payload: EntityParameters<E> = id
              ? [{ ...(Array.isArray(body) ? body[0] : body), id }]
              : body
            await dao.update(payload)
            return ok({ ok: true })
          }

          case 'DELETE': {
            if (!id) return bad('Missing id in route')
            await dao.delete({ id: [id] } as EntityParameters<E>)
            return ok({ ok: true })
          }

          default:
            return bad('Method Not Allowed', 405)
        }
      } catch (e: unknown) {
        ctx.error(e)
        return { status: 500, jsonBody: { error: ensureErrorMessage(e, 'Internal error') } }
      }
    },
  })
}
