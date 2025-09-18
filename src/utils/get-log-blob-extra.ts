import { head, split, trim } from 'lodash-es'

import type { HttpRequest } from '@azure/functions'

/**
 * Extract client IP and user agent from request headers.
 */
const getLogBlobExtra = (req: HttpRequest): { ip: string | null; userAgent: string | null } => {
  const xff = req.headers.get('x-forwarded-for') || ''
  const xci = req.headers.get('x-client-ip') || ''
  const ua = req.headers.get('user-agent') || ''
  const ip = trim(head(split(xff, ',')) || xci) || null
  const userAgent = trim(ua) || null
  return { ip, userAgent }
}

export default getLogBlobExtra
