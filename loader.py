import asyncio
import aiohttp
import mysql_orm
import json
import datetime
from pymysql.err import IntegrityError

from dotenv import load_dotenv
load_dotenv()

async def load_srl_data(full_load=False):
    await mysql_orm.create_pool(loop=asyncio.get_running_loop())

    maxid = await mysql_orm.select('select max(id) as max from `srl-data`.races')

    page = 1
    try:
        while True:
            r = await get_races(
                params={
                    'page': page,
                    'pageSize': 1000 if full_load else 20,
                    'game': 'alttphacks'
                }
            )
            if r['pastraces'] == []:
                break

            for race in r['pastraces']:
                if int(race['id']) <= maxid[0]['max'] and not full_load:
                    raise StopIteration
                try:
                    await mysql_orm.execute(
                        'INSERT INTO races (id, game, goal, date, num_entrants) VALUES (%s, %s, %s, %s, %s)',
                        args=[
                            race['id'],
                            race['game']['abbrev'],
                            race['goal'],
                            datetime.datetime.fromtimestamp(int(race['date'])),
                            race['numentrants']
                        ])
                    print(race['id'])

                    for entrant in race['results']:
                        print(entrant['player'])
                        await mysql_orm.execute(
                            'INSERT INTO results (race_id, place, player, time) VALUES (%s, %s, %s, %s)',
                            args=[
                                entrant['race'],
                                entrant['place'],
                                entrant['player'],
                                entrant['time'] if not entrant['place'] == 9998 else None
                            ]
                        )
                except IntegrityError as e:
                    continue
            await asyncio.sleep(1)
            page += 1
    except StopIteration:
        print('Halting data load.')


async def request_generic(url, method='get', reqparams=None, data=None, header={}, auth=None, returntype='text'):
    async with aiohttp.ClientSession(auth=None, raise_for_status=True) as session:
        async with session.request(method.upper(), url, params=reqparams, data=data, headers=header, auth=auth) as resp:
            if returntype == 'text':
                return await resp.text()
            elif returntype == 'json':
                return json.loads(await resp.text())
            elif returntype == 'binary':
                return await resp.read()

async def get_races(params):
    return await request_generic(f'http://api.speedrunslive.com/pastraces', reqparams=params, returntype='json')

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # loop.create_task(load_srl_data())
    loop.run_until_complete(load_srl_data(full_load=False))