import datetime

from web3._utils.filters import construct_event_filter_params
from amb import AMBHTTPProvider
from dotenv import load_dotenv
from web3._utils.events import get_event_data
from web3 import Web3
import os
from event import EventScanner, JSONEventState
import json
import logging

logger = logging.getLogger("main")
logging.basicConfig(level=logging.INFO)
load_dotenv()

endpoint_uri = os.environ.get("URI")
aws_access_key_id = os.environ.get("ACCESS_KEY")
aws_secret_access_key = os.environ.get("SECRET_KEY")
CONTRACT_ADDRESS = "0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"
ABI = """
[{
    "anonymous": false,
    "inputs": [
        {
            "indexed": true,
            "name": "from",
            "type": "address"
        },
        {
            "indexed": true,
            "name": "to",
            "type": "address"
        },
        {
            "indexed": false,
            "name": "value",
            "type": "uint256"
        }
    ],
    "name": "Transfer",
    "type": "event"
}]
"""  # transfer event


def get_start_block(state):
    """
    get start block number from state file
    :param state:
    :return:
    """
    state = state.get_state()

    return state["block_num"] if state["block_num"] else 1


def get_end_block():
    """
    get latest block number
    :return:
    """
    latest_block = w3.eth.get_block("latest", True)
    latest_block_number = int(latest_block.transactions[0].blockNumber) - 2

    return latest_block_number


def get_token_transfer(w3, state, event, filters, start_block, end_block, chunk_size, max_chunk_increase_size):
    """
    Run the event scanner and process transfer data
    :param w3:
    :param state:
    :param event:
    :param filters:
    :param start_block:
    :param end_block:
    :param chunk_size:
    :param max_chunk_increase_size:
    :return:
    """
    scanner = EventScanner(w3=w3,
                           state=state,
                           event=event,
                           filters=filters,
                           chunk_size=chunk_size,
                           max_chunk_increase_size=max_chunk_increase_size)

    scanner.scan_block(start_block=start_block, end_block=end_block)
    return True


if __name__ == "__main__":
    provider = AMBHTTPProvider(endpoint_uri, aws_access_key_id, aws_secret_access_key)
    w3 = Web3(provider)
    valid_conn = w3.is_connected()

    if valid_conn:
        abi = json.loads(ABI)
        ERC20 = w3.eth.contract(abi=abi)
        state = JSONEventState("pyusd")

        start_block = get_start_block(state)
        end_block = get_end_block()

        rcc_scanner = get_token_transfer(w3=w3,
                                         state=state,
                                         event=ERC20.events.Transfer,
                                         filters={"address": CONTRACT_ADDRESS},
                                         chunk_size=5000,
                                         max_chunk_increase_size=20000,
                                         start_block=start_block,
                                         end_block=end_block)


