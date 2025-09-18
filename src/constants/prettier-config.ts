import type { RequiredOptions } from 'prettier'

const prettierConfig: Partial<RequiredOptions> = {
  singleQuote: true,
  semi: false,
  trailingComma: 'all',
  printWidth: 100,
  endOfLine: 'lf',
}

export default prettierConfig
