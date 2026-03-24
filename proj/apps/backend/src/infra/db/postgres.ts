import { Pool } from 'pg';

export type DatabaseClient = Pick<Pool, 'query' | 'end'>;

export type CreateDatabaseClientInput = {
  connectionString: string;
  sslEnabled: boolean;
};

export const createDatabaseClient = ({
  connectionString,
  sslEnabled
}: CreateDatabaseClientInput): DatabaseClient =>
  new Pool({
    connectionString,
    ssl: sslEnabled ? { rejectUnauthorized: false } : undefined
  });
