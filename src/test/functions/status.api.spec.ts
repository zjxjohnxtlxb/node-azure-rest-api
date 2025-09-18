import { describe, it, expect, vi, beforeEach } from 'vitest'

import { makeReq, makeCtx } from '../helpers/http.ts'

import type { HttpRequest, InvocationContext } from '@azure/functions'

const { existsMock, queryMock, fromConnMock, connectMock } = vi.hoisted(() => ({
  existsMock: vi.fn<() => Promise<boolean>>(), //container.exists()
  queryMock: vi.fn<([]: string[]) => Promise<{ recordset: string[] }>>(), // request.query
  fromConnMock: vi.fn<([]: string) => object>(), // BlobServiceClient.fromConnectionString
  connectMock: vi.fn<() => Promise<unknown>>(), // sql.connect
}))

// ---- Services Mocks
// @azure/storage-blob
vi.mock('@azure/storage-blob', () => {
  return {
    BlobServiceClient: {
      fromConnectionString: fromConnMock,
    },
  }
})
//mssql
vi.mock('mssql', () => {
  return {
    default: {
      connect: connectMock,
    },
  }
})

vi.resetModules()
const { statusHandler, statusFullHandler } = await import('../../functions/api/status.api.ts')

beforeEach(() => {
  vi.clearAllMocks()

  process.env.AZURE_BLOB_CONN = 'UseDevelopmentConnection'
  process.env.AZURE_BLOB_LOGS_CONTAINER = 'logs'
  process.env.npm_package_version = '0.0.0-test'

  // Blob mock：container.exists() -> true by default
  existsMock.mockResolvedValue(true)
  fromConnMock.mockReturnValue({
    getContainerClient: () => ({
      exists: existsMock,
    }),
  })

  const fakePool = {
    connected: true,
    on: vi.fn(),
    request: () => ({ query: queryMock }),
    close: vi.fn(),
  }
  connectMock.mockResolvedValue(fakePool)
  queryMock.mockResolvedValue({
    recordset: ['ok'],
  })
})

describe('status handlers (minimal)', () => {
  it('statusHandler returns 200 with basic info', async () => {
    const res = await statusHandler(makeReq() as HttpRequest, makeCtx() as InvocationContext)
    expect(res.status).toBe(200)

    // eslint-disable-next-line @typescript-eslint/naming-convention
    expect(res.headers).toEqual({ 'Cache-Control': 'no-store' })
    expect(res.jsonBody).toMatchObject({
      status: 'ok',
      version: '0.0.0-test',
      requestId: 'test-invoke-1',
    })
  })

  it('statusFullHandler → 200 ok when DB & Blob are good', async () => {
    const res = await statusFullHandler(makeReq() as HttpRequest, makeCtx() as InvocationContext)
    expect(res.status).toBe(200)
    expect(res.jsonBody).toMatchObject({
      status: 'ok',
      deps: { database: 'good', blob: 'good' },
    })
    expect(connectMock).toHaveBeenCalledTimes(1)
    expect(fromConnMock).toHaveBeenCalledTimes(1)
    expect(queryMock).toHaveBeenCalledWith('SELECT 1')
  })

  it('statusFullHandler → 503 degraded when Blob missing', async () => {
    existsMock.mockResolvedValue(false)
    const res = await statusFullHandler(makeReq() as HttpRequest, makeCtx() as InvocationContext)
    expect(res.status).toBe(503)
    expect(res.jsonBody).toMatchObject({
      status: 'degraded',
      deps: { blob: 'bad' },
    })
  })

  it('statusFullHandler → 503 degraded when DB query fails', async () => {
    queryMock.mockRejectedValueOnce(new Error('db down'))
    const res = await statusFullHandler(makeReq() as HttpRequest, makeCtx() as InvocationContext)
    expect(res.status).toBe(503)
    expect(res.jsonBody).toMatchObject({
      status: 'degraded',
      deps: { database: 'bad' },
    })
  })

  it('full → both DB & Blob bad → 503 degraded', async () => {
    existsMock.mockResolvedValue(false)
    queryMock.mockRejectedValueOnce(new Error('db down'))
    const res = await statusFullHandler(makeReq() as HttpRequest, makeCtx() as InvocationContext)
    expect(res.status).toBe(503)
    expect(res.jsonBody.deps).toEqual({ database: 'bad', blob: 'bad' })
  })
})
