import asyncio
import asyncpg
import datetime


USER = "doadmin"
PASSWORD = "AVNS_3VIXdys5TqWXJoTE9IJ" 
DB_TYPE = "postgresql"
HOST = "db-postgresql-sgp1-05668-do-user-30220694-0.l.db.ondigitalocean.com"

# PORT = "25060"
# DB_NAME = "defaultdb"
# SSL_MODE = "require"
# HOST = "private-db-postgresql-sgp1-05668-do-user-30220694-0.l.db.ondigitalocean.com"

PORT = "25061"
DB_NAME = "pg_pool"
SSL_MODE = "require"

DB_URL = f"{DB_TYPE}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_NAME}?sslmode={SSL_MODE}"

QUERY = """
        SELECT
            o.sub,
            o.status,
            o.customer,
            o.customer_no,
            o.dept_sales,
            o.dept_fulfillment,
            o.dept_shipping,
            o.dept_pickup,
            o.transport,
            o.thermo,
            o.date_delivery,
            o.time_delivery_start,
            o.time_delivery_end,
            o.comment_order,
            o.comment_1,
            o.comment_2,
            o.comment_3,

            -- contact
            o.recipient,
            o.telephone_day,
            o.telephone_evening,
            o.mobile,

            -- address
            a.postal_code,
            a.city,
            a.district,
            a.section,
            a.address,
            a.postnumber,

            -- tracking
            t.obtnumbers,

            -- item
            i.sku,
            i.qty,

            -- catalogue
            c.brand,
            c.unit,
            c.product,
            c.category

        FROM orders o
        JOIN order_items i
            ON i.sub = o.sub
        LEFT JOIN order_address a
            ON a.sub = o.sub
        LEFT JOIN order_tracking t
            ON t.sub = o.sub
        LEFT JOIN catalogue c
            ON c.sku = i.sku

        WHERE o.dept_sales != '0001總倉(2F廠務辦公室)'
            AND o.status != 'delivered'
    """


async def main_pooled():
    # Create a connection pool
    pool = await asyncpg.create_pool(DB_URL)

    # Acquire a connection from the pool using a context manager
    async with pool.acquire() as conn:
        # Within the context manager, 'conn' is a Connection object
        result = await conn.fetchval('SELECT 2 ^ $1', 10)
        print(f'2^10 is: {result}')
    
    # The connection is automatically returned to the pool when the 'async with' block exits

    # Close the pool when the application shuts down
    await pool.close()

async def main():
    # Establish a connection to an existing database named "test"
    # as a "postgres" user.
    conn = await asyncpg.connect(DB_URL)
    # Execute a statement to create a new table.
    await conn.execute(QUERY)

    print(conn)

    # # Insert a record into the created table.
    # await conn.execute('''
    #     INSERT INTO users(name, dob) VALUES($1, $2)
    # ''', 'Bob', datetime.date(1984, 3, 1))

    # # Select a row from the table.
    # row = await conn.fetchrow(
    #     'SELECT * FROM users WHERE name = $1', 'Bob')
    # # *row* now contains
    # # asyncpg.Record(id=1, name='Bob', dob=datetime.date(1984, 3, 1))

    # Close the connection.
    await conn.close()

# asyncio.run(main_pooled())

import asyncio

import polars as pl
import pandas as pd

async def main():
    # 1. Set up an async SQLAlchemy engine
    # Replace with your actual database credentials
    pool = await asyncpg.create_pool(DB_URL)

    # The SQL query you want to execute

    # 2. Define the synchronous Polars reading function
    # Polars' read_database handles connection execution internally, 
    # and the connection/cursor will be closed automatically for this specific query.
    def read_polars_sync(conn):
        return pl.read_database(query=QUERY, connection=conn)

    # 3. Execute synchronously within the async connection context
    try:
        async with pool.acquire() as conn:
            # The partial ensures the function has the query argument pre-filled
            # df = await conn.fetch(read_polars_sync, conn)
            df = pd.DataFrame(await conn.fetch(QUERY))
        
        print(df.head())
        
    except Exception as e:
        print(f"An error occurred: {e}")
        
    finally:
        # Dispose of the engine resources
        await pool.close()

if __name__ == "__main__":
    asyncio.run(main())
