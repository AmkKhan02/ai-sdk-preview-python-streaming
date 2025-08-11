import { Message } from "ai";
import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";
import { type AsyncDuckDB } from '@duckdb/duckdb-wasm';

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function sanitizeUIMessages(messages: Array<Message>): Array<Message> {
  const messagesBySanitizedToolInvocations = messages.map((message) => {
    if (message.role !== "assistant") return message;

    if (!message.toolInvocations) return message;

    const toolResultIds: Array<string> = [];

    for (const toolInvocation of message.toolInvocations) {
      if (toolInvocation.state === "result") {
        toolResultIds.push(toolInvocation.toolCallId);
      }
    }

    const sanitizedToolInvocations = message.toolInvocations.filter(
      (toolInvocation) =>
        toolInvocation.state === "result" ||
        toolResultIds.includes(toolInvocation.toolCallId),
    );

    return {
      ...message,
      toolInvocations: sanitizedToolInvocations,
    };
  });

  return messagesBySanitizedToolInvocations.filter(
    (message) =>
      message.content.length > 0 ||
      (message.toolInvocations && message.toolInvocations.length > 0),
  );
}

// DuckDB WASM utilities
let duckdbPromise: Promise<typeof import('@duckdb/duckdb-wasm')> | null = null;
let duckdbInstance: AsyncDuckDB | null = null;

export const getDuckDB = async () => {
  if (typeof window === 'undefined') {
    throw new Error('DuckDB can only be used in the browser');
  }

  if (!duckdbPromise) {
    duckdbPromise = import('@duckdb/duckdb-wasm');
  }

  return duckdbPromise;
};

export const initializeDuckDB = async (): Promise<AsyncDuckDB> => {
  if (duckdbInstance) {
    return duckdbInstance;
  }

  try {
    const duckdb = await getDuckDB();
    
    const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
    
    const worker_url = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], {
        type: 'text/javascript'
      })
    );
    
    const worker = new Worker(worker_url);
    const logger = new duckdb.ConsoleLogger();
    const db = new duckdb.AsyncDuckDB(logger, worker);
    
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    
    duckdbInstance = db;
    console.log('DuckDB initialized successfully');
    return db;
  } catch (error) {
    console.error('Failed to initialize DuckDB:', error);
    throw error;
  }
};

export const processDuckDBFile = async (file: File): Promise<{
  data: any[];
  tableName: string;
  totalTables: number;
  rowCount: number;
}> => {
  const db = await initializeDuckDB();
  
  try {
    // Read file as ArrayBuffer
    const arrayBuffer = await file.arrayBuffer();
    const uint8Array = new Uint8Array(arrayBuffer);
    
    // Create a connection
    const connection = await db.connect();
    
    try {
      // Register the database file
      await db.registerFileBuffer(file.name, uint8Array);
      
      // Get list of tables in the database
      const tablesResult = await connection.query(`
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main'
      `);
      
      const tables = tablesResult.toArray().map((row: any) => row.table_name);
      
      if (tables.length === 0) {
        throw new Error('No tables found in the DuckDB file');
      }
      
      console.log('Found tables:', tables);
      
      // For simplicity, we'll query the first table
      const tableName = tables[0];
      
      // Get sample data from the first table (limit to 1000 rows for performance)
      const dataResult = await connection.query(`
        SELECT * FROM "${tableName}" LIMIT 1000
      `);
      
      const data = dataResult.toArray();
      
      if (data.length === 0) {
        throw new Error(`Table "${tableName}" is empty`);
      }
      
      console.log(`Parsed DuckDB data from table "${tableName}":`, data);
      
      return {
        data,
        tableName,
        totalTables: tables.length,
        rowCount: data.length
      };
      
    } finally {
      await connection.close();
    }
  } catch (error) {
    console.error('DuckDB processing error:', error);
    throw error;
  }
};

export const terminateDuckDB = () => {
  if (duckdbInstance) {
    duckdbInstance.terminate();
    duckdbInstance = null;
  }
};