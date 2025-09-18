import sql from 'mssql'

import DbError from '../errors/db.error.ts'
import ensureErrorMessage from '../utils/ensure-error-message.ts'

import type { config as SqlConfig, ConnectionPool, Transaction } from 'mssql'

// Database configuration
const dBConnectedSuccessfully = '[Db: CONNECTION] DB connected successfully'
const dBPoolClosed = '[Db: CONNECTION] DB pool closed'

const dbConfig: SqlConfig = {
  server: process.env.AZURE_SERVER!,
  port: Number(process.env.AZURE_PORT ?? 1433),
  database: process.env.AZURE_DATABASE!,
  options: {
    encrypt: true,
    trustServerCertificate: false,
  },
  authentication: {
    type: 'azure-active-directory-password',
    options: {
      userName: process.env.AZURE_USER!,
      password: process.env.AZURE_PASSWORD!,
      clientId: process.env.AZURE_CLIENT_ID!,
      tenantId: process.env.AZURE_TENANT_ID!,
    },
  },
}

class Database {
  private static instance: Database // Singleton instance
  private pool: sql.ConnectionPool | null = null // SQL connection pool

  private constructor() {} // Private constructor to enforce singleton

  // Get the singleton instance
  public static getInstance(): Database {
    if (!Database.instance) {
      Database.instance = new Database()
    }

    return Database.instance
  }

  /**
   * Get the SQL connection pool
   * Reuses existing pool if connected
   */
  public async getPool(retries = 5, delay = 3000): Promise<ConnectionPool> {
    if (this.pool && this.pool.connected) {
      return this.pool
    }

    try {
      this.pool = await sql.connect(dbConfig)
      // Handle runtime pool errors
      this.pool.on('error', (error) => {
        this.pool = null
        //toDo log
        console.error(DbError.runtimeError(ensureErrorMessage(error)).message)
      })
      console.log(dBConnectedSuccessfully)

      return this.pool
    } catch (err) {
      if (retries > 0) {
        console.warn(DbError.connectionWarn(delay, retries).message)
        await new Promise((res) => setTimeout(res, delay))

        return this.getPool(retries - 1, delay)
      }
      this.pool = null

      throw DbError.connectionError(ensureErrorMessage(err))
    }
  }

  public async getTransaction(): Promise<Transaction> {
    const pool = await this.getPool()

    return new sql.Transaction(pool)
  }

  /**
   * Utility method: run code inside a transaction
   * Automatically handles begin/commit/rollback
   */
  public async withTransaction<T>(callback: (transaction: Transaction) => Promise<T>): Promise<T> {
    const pool = await this.getPool()
    const transaction = new sql.Transaction(pool)

    try {
      await transaction.begin()
      const result = await callback(transaction)
      await transaction.commit()

      return result
    } catch (err) {
      await transaction.rollback()

      throw err
    }
  }

  /** Close the connection pool */
  public async close(): Promise<void> {
    if (this.pool) {
      await this.pool.close()
      this.pool = null
      console.log(dBPoolClosed)
    }
  }
}

// Export the singleton instance
const db = Database.getInstance()

export default db
