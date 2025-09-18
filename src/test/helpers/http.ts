import { isNil, toPairs } from 'lodash-es'

import type { HttpRequest, InvocationContext } from '@azure/functions'

export type TestHttpRequest<T = unknown> = Pick<
  HttpRequest,
  'method' | 'url' | 'params' | 'query'
> & {
  json(): Promise<T>
}

export function makeReq<T = unknown>(
  opts: {
    method?: string
    url?: string
    params?: Record<string, string>
    query?: Record<string, string | number | boolean | null | undefined>
    body?: T
  } = {},
): TestHttpRequest<T> {
  const { method = 'GET', url = 'http://localhost/api', params = {}, query = {}, body } = opts
  const usp = new URLSearchParams()
  for (const [k, v] of toPairs(query)) {
    if (!isNil(v)) usp.append(k, String(v))
  }

  return {
    method,
    url,
    params,
    query: usp,
    async json() {
      if (body === undefined) throw new Error('no body')
      return body
    },
  }
}

export function makeCtx(): Pick<InvocationContext, 'log' | 'error' | 'warn' | 'invocationId'> {
  return {
    log: () => {},
    error: () => {},
    warn: () => {},
    invocationId: 'test-invoke-1',
  }
}
