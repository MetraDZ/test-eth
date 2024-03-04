import os
from typing import Annotated, Optional
from beanie import Document, Indexed, init_beanie
from motor.motor_asyncio import AsyncIOMotorClient

DB_URI = os.getenv('MONGODB_URI')

#Models for beanie
class Transaction(Document):
    block_number: int
    to: Annotated[Optional[str], Indexed()]
    hash: str


class Block(Document):
    number: int


async def init_db():
    '''
        Async init of mongodb client with beanie ODM
    '''
    client = AsyncIOMotorClient(DB_URI)
    await init_beanie(client.blockchain, document_models=[Block, Transaction])

async def fill_database(blocks: list):
    '''
        Filling database with Blocks and Transactions from blocks
    '''
    block_documents = []
    transaction_documents = []
    for block in blocks:
        block_documents.append(Block(number=block.number))
        for transaction in block.transactions:
            transaction_documents.append(
                Transaction(
                    block_number=transaction.blockNumber,
                    to=transaction.to,
                    hash=transaction.hash
                )
            )
    await Block.insert_many(block_documents)
    await Transaction.insert_many(transaction_documents)

async def find_certain_transactions(receiver: str):
    '''
        Getting all transactions with specified receiver
    '''
    return await Transaction.find(Transaction.to == receiver).to_list()