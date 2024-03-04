import json
import asyncio
import random
from aiohttp import ClientSession
from motor.motor_asyncio import AsyncIOMotorClient

from classes import Block, Transaction
from db import init_db, fill_database, find_certain_transactions

URL = "http://127.0.0.1:8545"
BLOCK_OFFSET = 100
NUMBER_OF_BLOCKS = 1000
BATCH_LIMIT = 100

def stream_range(r: range, chunk_size=BATCH_LIMIT):
    '''
        Divides given range into equal parts by chuck_size + leftovers
        Example: r = (1, 2, 3, 4, 5, 6, 7), chunk_size = 2 would yield
                (1, 2), (3, 4), (5, 6), (7)
    '''
    start, stop, step = r.start, r.stop, r.step
    current = start
    while current < stop:
        yield range(current, min(current + chunk_size, stop), step)
        current += chunk_size

def make_params(method: str, params: list, _id: int = 1):
    '''
        Generates request body for request
    '''
    return {
        'jsonrpc': '2.0',
        'method': method,
        'params': params,
        'id': _id
    }

async def make_aio_request(url: str, data: dict = {}):
    '''
        Makes async request
    '''
    async with ClientSession() as session:
        async with session.post(
                url=url,
                data=data,
                headers={"Content-Type": "application/json"},
        ) as resp:
            msg = await resp.json()
    return msg

def serialize_data(blocks_lists: list):
    '''
        Transforms raw data into python dataclasses instances
    '''
    blocks = []
    for block_list in blocks_lists:
        for block_data in block_list:
            block = Block(
                int(block_data['result']['number'], base=16), 
                tuple(Transaction(
                    int(transaction['blockNumber'], base=16), 
                    transaction['to'],
                    transaction['hash']) for transaction in block_data['result']['transactions']
                )
            )
            blocks.append(block)
    return blocks
    
async def prepare_blocks_data():
    '''
        Makes async requests to Eth Node to retrieve last 1000 blocks info from 100th block from the end
    '''
    latest_block_json = await make_aio_request(
        URL, 
        json.dumps([make_params('eth_getBlockByNumber', ["latest", True])])
    )
    latest_number = int(latest_block_json[0]['result']['number'], base=16)
    blocks_numbers_range = range(latest_number - NUMBER_OF_BLOCKS - BLOCK_OFFSET, latest_number - BLOCK_OFFSET)
    coroutines = []
    for rng in stream_range(blocks_numbers_range):
        request_message = [
            make_params(
                method='eth_getBlockByNumber',
                params=[f"{hex(block_number)}", True],
                _id=_id
            ) for _id, block_number in enumerate(rng)
        ]
        coroutines.append(make_aio_request(URL, json.dumps(request_message)))
    blocks_lists = await asyncio.gather(*coroutines)
    return serialize_data(blocks_lists)


if __name__ == "__main__":
    event_loop = asyncio.get_event_loop()
    event_loop.run_until_complete(init_db())
    print('DB engine started')
    print('Receiving blocks data')
    blocks = event_loop.run_until_complete(prepare_blocks_data())
    all_transactions = [
        transaction.to for block in blocks for transaction in block.transactions
        if transaction.to is not None
    ]
    print('Inserting data into database')
    event_loop.run_until_complete(fill_database(blocks))
    random_receiver = random.choice(all_transactions)
    print(f'Selected address: {random_receiver}')
    transactions = event_loop.run_until_complete(find_certain_transactions(random_receiver))
    for transaction in transactions:
        print(transaction)