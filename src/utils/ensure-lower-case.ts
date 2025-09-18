import { lowerCase, replace } from 'lodash-es'

/**
 * Convert to lower case and remove all whitespace.
 * e.g. "HelloWorld" -> "helloworld"
 */
const ensureLowerCase = (str: string): string => {
  return replace(lowerCase(str), /\s+/g, '')
}

export default ensureLowerCase
