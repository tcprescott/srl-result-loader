import asyncio
import aiomysql
import os


async def create_pool(loop):
    global __pool
    __pool = await aiomysql.create_pool(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT")),
        user=os.getenv("DB_USER"),
        db=os.getenv("DB_NAME"),
        password=os.getenv("DB_PASS"),
        program_name='srl-result-loader',
        charset='utf8mb4',
        autocommit=True,
        maxsize=10,
        minsize=1,
        loop=loop
    )


async def select(sql, args=[], size=None):
    global __pool
    with (await __pool) as conn:
        cur = await conn.cursor(aiomysql.DictCursor)
        await cur.execute(sql.replace('?', '%s'), args or ())
        if size:
            rs = await cur.fecthmany(size)
        else:
            rs = await cur.fetchall()
        await cur.close()
        return rs


async def execute(sql, args=[]):
    global __pool
    with (await __pool) as conn:
        try:
            cur = await conn.cursor()
            await cur.execute(sql.replace('?', '%s'), args)
            affected = cur.rowcount
            await cur.close()
        except BaseException as e:
            raise
        return affected
