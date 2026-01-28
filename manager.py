import asyncpg
import pandas as pd
import numpy as np
from typing import List, Optional
from contextlib import asynccontextmanager
from database.info import DB_URL

class DatabaseManager:
    """
    Database connection manager with connection pooling.
    """
    
    def __init__(
        self,
        min_size: int = 10,
        max_size: int = 20
    ):
        self.min_size = min_size
        self.max_size = max_size
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Initialize the connection pool."""
        if self.pool is None:
            self.pool = await asyncpg.create_pool(
                DB_URL
            )
            print("Database connection pool created")
    
    async def close(self):
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()
            print("Database connection pool closed")
    
    @asynccontextmanager
    async def acquire(self):
        """Context manager for acquiring a connection from the pool."""
        async with self.pool.acquire() as connection:
            yield connection
    
    @staticmethod
    def convert_value(val, target_type=None):
        """
        Convert pandas types to Python native types.
        
        Parameters:
        -----------
        val : any
            Value to convert
        target_type : str, optional
            Target type hint ('str', 'int', 'float', etc.)
        """
        if pd.isna(val):
            return None
        elif isinstance(val, (pd.Timestamp, np.datetime64)):
            return pd.Timestamp(val).to_pydatetime()
        elif isinstance(val, (np.integer, np.floating)):
            val = val.item()  # Convert to Python native
        elif isinstance(val, np.bool_):
            val = bool(val)
        
        # Apply target type conversion if specified
        if target_type == 'str':
            return str(val) if val is not None else None
        
        return val
    
    async def upsert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        key_columns: List[str]
    ) -> int:
        """
        Upsert DataFrame to PostgreSQL table.
        
        Parameters:
        -----------
        df : pd.DataFrame
            The DataFrame containing the data to upsert
        table_name : str
            Name of the table to upsert into
        key_columns : List[str]
            List of column names that form the unique constraint
        
        Returns:
        --------
        int : Number of rows upserted
        """
        columns = df.columns.tolist()
        update_columns = [col for col in columns if col not in key_columns]
        
        # Build the query
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        column_names = ", ".join(columns)
        conflict_columns = ", ".join(key_columns)
        update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        
        query = f"""
            INSERT INTO {table_name} ({column_names})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_columns})
            DO UPDATE SET {update_clause}
        """
        
        # Prepare data with type conversion
        data = []
        for _, row in df.iterrows():
            values = tuple([self.convert_value(row[col]) for col in columns])
            data.append(values)
        
        # Execute batch upsert
        async with self.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(query, data)
        
        print(f"Successfully upserted {len(data)} rows to {table_name}")
        return len(data)
    
    async def insert_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str
    ) -> int:
        """
        Insert DataFrame rows to PostgreSQL table.
        """
        columns = df.columns.tolist()
        placeholders = ", ".join([f"${i+1}" for i in range(len(columns))])
        column_names = ", ".join(columns)
        
        query = f"""
            INSERT INTO {table_name} ({column_names})
            VALUES ({placeholders})
        """
        
        data = []
        for _, row in df.iterrows():
            values = tuple([self.convert_value(row[col]) for col in columns])
            data.append(values)
        
        async with self.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(query, data)
        
        print(f"Successfully inserted {len(data)} rows to {table_name}")
        return len(data)
    
    async def update_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        key_columns: List[str]
    ) -> int:
        """
        Update PostgreSQL table using DataFrame.
        """
        update_columns = [col for col in df.columns if col not in key_columns]
        
        set_clause = ", ".join([f"{col} = ${i+1}" for i, col in enumerate(update_columns)])
        where_clause = " AND ".join([f"{key} = ${len(update_columns) + i + 1}" 
                                      for i, key in enumerate(key_columns)])
        
        query = f"""
            UPDATE {table_name}
            SET {set_clause}
            WHERE {where_clause}
        """
        
        data = []
        for _, row in df.iterrows():
            values = tuple([self.convert_value(row[col]) for col in update_columns] + 
                          [self.convert_value(row[key]) for key in key_columns])
            data.append(values)
        
        async with self.acquire() as conn:
            async with conn.transaction():
                await conn.executemany(query, data)
        
        print(f"Successfully updated {len(data)} rows in {table_name}")
        return len(data)
    
    async def fetch_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        limit: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Fetch data from PostgreSQL table into DataFrame.
        
        Parameters:
        -----------
        table_name : str
            Name of the table to fetch from
        columns : List[str], optional
            List of columns to fetch (defaults to all)
        where : str, optional
            WHERE clause (e.g., "age > 25")
        limit : int, optional
            Maximum number of rows to fetch
        
        Returns:
        --------
        pd.DataFrame
        """
        col_str = ", ".join(columns) if columns else "*"
        query = f"SELECT {col_str} FROM {table_name}"
        
        if where:
            query += f" WHERE {where}"
        if limit:
            query += f" LIMIT {limit}"
        
        async with self.acquire() as conn:
            rows = await conn.fetch(query)
        
        # Convert to DataFrame
        df = pd.DataFrame([dict(row) for row in rows])
        print(f"Fetched {len(df)} rows from {table_name}")
        return df
    
    async def execute_query(self, query: str, *args):
        """
        Execute a custom query.
        """
        async with self.acquire() as conn:
            result = await conn.execute(query, *args)
        return result
    
    async def fetch_query(self, query: str, *args) -> pd.DataFrame:
        """
        Fetch results from a custom query into DataFrame.
        """
        async with self.acquire() as conn:
            rows = await conn.fetch(query, *args)
        
        df = pd.DataFrame([dict(row) for row in rows])
        return df 