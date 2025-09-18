import eslintPluginTs from '@typescript-eslint/eslint-plugin'
import unicorn from 'eslint-plugin-unicorn'
import tsParser from '@typescript-eslint/parser'
import prettierPlugin from 'eslint-plugin-prettier'
import prettierConfig from 'eslint-config-prettier'
import eslintPluginImport from 'eslint-plugin-import'

export default [
  {
    files: ['**/*.ts', '**/*.tsx'],
    languageOptions: {
      parser: tsParser,
      parserOptions: {
        project: './tsconfig.json',
      },
      ecmaVersion: 'latest',
      sourceType: 'module',
    },
    plugins: {
      '@typescript-eslint': eslintPluginTs,
      prettier: prettierPlugin,
      unicorn,
      import: eslintPluginImport,
    },
    rules: {
      ...eslintPluginTs.configs.recommended.rules,
      ...prettierConfig.rules,
      'import/no-duplicates': 'error',
      'import/no-unresolved': 'error',
      'import/first': 'error',
      'import/newline-after-import': 'error',
      'import/order': [
        'error',
        {
          groups: ['builtin', 'external', 'internal', 'parent', 'sibling', 'index', 'type'],
          'newlines-between': 'always',
          alphabetize: { order: 'asc', caseInsensitive: true },
        },
      ],
      '@typescript-eslint/consistent-type-imports': ['error', { prefer: 'type-imports' }],
      'no-unused-vars': 'off',
      '@typescript-eslint/no-unused-vars': ['error'],
      'prettier/prettier': ['error', { semi: false }],
      '@typescript-eslint/naming-convention': [
        'error',
        {
          selector: ['variableLike', 'variable'],
          format: ['camelCase', 'UPPER_CASE', 'PascalCase'],
          leadingUnderscore: 'allow',
        },
        {
          selector: ['typeLike', 'class', 'enum', 'interface'],
          format: ['PascalCase'],
        },
        {
          selector: 'property',
          format: ['camelCase', 'snake_case', 'PascalCase'],
        },
      ],
      'unicorn/filename-case': [
        'error',
        {
          case: 'kebabCase',
        },
      ],
    },
    ignores: ['dist/', 'node_modules/'],
  },
  {
    files: ['src/schemas/**', 'src/scripts/**'],
    rules: {
      '@typescript-eslint/naming-convention': 'off',
    },
  },
]
