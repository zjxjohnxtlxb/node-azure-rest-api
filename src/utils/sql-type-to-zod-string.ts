import ensureLowerCase from './ensure-lower-case.ts'

/**
 * Convert SQL type to Zod expression string (as code).
 */
const sqlTypeToZodString = (type: string, maxLength: number | null, nullable: boolean): string => {
  const t = ensureLowerCase(type)
  let z: string

  switch (t) {
    // Integer types
    case 'tinyint':
    case 'smallint':
    case 'int':
      z = 'z.number().int()'
      break
    case 'bigint':
      z = 'z.string()' // stored as string to avoid precision issues
      break
    // Decimal / Money types
    case 'decimal':
    case 'numeric':
    case 'money':
    case 'smallmoney':
      z = 'z.string()' // stored as string to avoid precision issues
      break
    // Floating point types
    case 'float':
    case 'real':
      z = 'z.number()'
      break
    // Boolean type
    case 'bit':
      z = 'z.boolean()'
      break
    // Date / Time types
    case 'date':
    case 'datetime':
    case 'datetime2':
    case 'smalldatetime':
    case 'datetimeoffset':
      z = 'z.iso.datetime()' // use ISO string instead of z.date() for JSON Schema compatibility
      break
    case 'time':
      z = 'z.string()'
      break
    // Unique identifier (GUID)
    case 'uniqueidentifier':
      z = 'z.uuid()'
      break
    // Binary types
    case 'varbinary':
    case 'binary':
    case 'image':
      z = 'z.string()' // represented as Base64 or hex string
      break
    // Default: character types (char, varchar, nchar, nvarchar, nvarchar2, text, xml, etc.)
    default:
      z = maxLength ? `z.string().max(${maxLength})` : 'z.string()'
      break
  }
  if (nullable) z += '.nullable()'
  z += `.meta({sqlType: '${t}'})`

  return z
}

export default sqlTypeToZodString
