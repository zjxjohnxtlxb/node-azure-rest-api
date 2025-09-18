import { camelCase, replace, startCase } from 'lodash-es'

const toPascalCase = (str: string): string => {
  return replace(startCase(camelCase(str)), /\s+/g, '')
}

export default toPascalCase
