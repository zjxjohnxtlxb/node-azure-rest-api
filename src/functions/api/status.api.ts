import {
  app,
  type HttpRequest,
  type HttpResponseInit,
  type InvocationContext,
} from '@azure/functions'
import { BlobServiceClient } from '@azure/storage-blob'
import { size } from 'lodash-es'

import db from '../../lib/db.ts'

async function checkBlob(): Promise<'good' | 'bad'> {
  const conn = process.env.AZURE_BLOB_CONN
  const containerName = process.env.AZURE_BLOB_LOGS_CONTAINER ?? ''
  try {
    if (!conn) {
      return 'bad'
    }
    const blobServiceClient = BlobServiceClient.fromConnectionString(conn)
    const container = blobServiceClient.getContainerClient(containerName)
    const exists = await container.exists()

    return exists ? 'good' : 'bad'
  } catch {
    return 'bad'
  }
}

async function checkDb(): Promise<'good' | 'bad'> {
  try {
    const pool = await db.getPool(1, 500)
    const request = pool.request()
    const res = await request.query('SELECT 1')

    return size(res.recordset) ? 'good' : 'bad'
  } catch {
    return 'bad'
  }
}

export async function statusHandler(
  _req: HttpRequest,
  ctx: InvocationContext,
): Promise<HttpResponseInit> {
  return {
    status: 200,
    jsonBody: {
      status: 'ok',
      version: process.env.npm_package_version,
      requestId: ctx.invocationId,
    },
    // eslint-disable-next-line @typescript-eslint/naming-convention
    headers: { 'Cache-Control': 'no-store' },
  }
}

export async function statusFullHandler(
  req: HttpRequest,
  ctx: InvocationContext,
): Promise<HttpResponseInit> {
  const [dbStatus, blobStatus] = await Promise.all([checkDb(), checkBlob()])
  const allGood = dbStatus === 'good' && blobStatus === 'good'

  const body = {
    status: allGood ? 'ok' : 'degraded',
    version: process.env.npm_package_version,
    deps: { database: dbStatus, blob: blobStatus },
    timestamp: new Date().toISOString(),
    requestId: ctx.invocationId,
  }

  ctx.log('status', body)

  return {
    status: allGood ? 200 : 503,
    jsonBody: body,
    // eslint-disable-next-line @typescript-eslint/naming-convention
    headers: { 'Cache-Control': 'no-store' },
  }
}

app.http('status', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'status/',
  handler: statusHandler,
})

app.http('status-full', {
  route: 'status/full/',
  methods: ['GET'],
  authLevel: 'anonymous',
  handler: statusFullHandler,
})
