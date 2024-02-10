import datetime
import json
import time
import os
import logging
from utils import establish_connection
from abc import ABC, abstractmethod
from web3 import Web3
from utils import write_batch_to_db
from typing import Tuple,
from web3.exceptions import BlockNotFound
from eth_abi.codec import ABICodec
from web3._utils.events import get_event_data
from web3._utils.filters import construct_event_filter_params

logger = logging.getLogger(__name__)


class EventState(ABC):

    def state_entry(self, block_num: int = None, timestamp: datetime.datetime = None) -> dict:
        """
        Entry for  state manager
        :param block_num:
        :param timestamp:
        :return:
        """
        return {
            "block_num": block_num,
            "block_timestamp": timestamp
        }

    @abstractmethod
    def get_state(self):
        """
        get saved state
        :return:
        """
        pass

    @abstractmethod
    def store_state(self, block_num: int, timestamp: datetime.datetime):
        """
        store recent state
        :param block_num:
        :param timestamp:
        :return:
        """
        pass

    @abstractmethod
    def process_data(self, data: dict):
        """
        process data and store to storage of type
        :param data:
        :return:
        """
        pass


class JSONEventState(EventState):
    """
    JSON State manager
    """

    def __init__(self, fname: str):
        self.state_name = fname
        self.state_fname = f"{self.state_name}_state"
        self.host = os.environ.get("DB_HOST")
        self.user = str(os.environ.get("DB_USER"))
        self.port = str(os.environ.get("PORT"))
        self.db = os.environ.get("DB")
        self.password = os.environ.get("PASS")
        self.cur = establish_connection(host=self.host, user=self.user, port=self.port, db=self.db,
                                        password=self.password)

    def get_state(self):
        try:
            with open(f"{self.state_fname}.json", "rt") as state_file:
                return json.load(state_file)

        except FileNotFoundError:
            with open(f"{self.state_fname}.json", "wt") as state_file:
                state_data = self.state_entry()
                json.dump(state_data, state_file)

                return state_data
        except json.decoder.JSONDecodeError:
            logger.error("something went wrong when getting state... check file for corruption")

    def store_state(self, block_num: int, timestamp: datetime.datetime) -> int:
        state_data = self.state_entry(block_num, timestamp)

        with open(f"{self.state_fname}.json", "wt") as state_file:
            json.dump(state_data, state_file)

        return block_num

    def process_data(self, data: dict):

        logger.info(f" inserting {len(data)} transactions")
        query = \
            """
         INSERT INTO 
         public.transaction("address", "tx_hash", "from", "to", "value", "block_number", "block_timestamp", "log_index")
         VALUES(%(address)s, %(tx_hash)s, %(from)s, %(to)s, %(value)s, %(block_number)s, %(block_timestamp)s, %(log_index)s)
        """
        write_batch_to_db(self.cur, query, data)
        logger.info(f"completed insert of {len(data)} transactions")


#  https://web3py.readthedocs.io/en/stable/examples.html#advanced-example-fetching-all-token-transfer-events                                                      to_block=_end_block)
class EventScanner:
    """
    Scan the blockchain for event
    """

    def __init__(self, w3: Web3, event, filters: dict, state, chunk_size, max_chunk_increase_size,
                 max_request_retries=4,
                 request_retries_delay=3, chunk_increase_size=0.2):
        self.logger = logger
        self.w3 = w3
        self.state = state
        self.event = event
        self.filters = filters
        self.chunk_size = chunk_size
        self.max_request_retries = max_request_retries
        self.request_retries_delay = request_retries_delay
        self.chunk_increase_size = chunk_increase_size
        self.max_chunk_increase_size = max_chunk_increase_size

    def _retry_web_call(self, func, from_block: int, to_block: int) -> Tuple[int, list]:
        """A custom retry loop to throttle down block range.

        If our JSON-RPC server cannot serve all incoming `eth_getLogs` in a single request,
        we retry and throttle down block range for every retry.

        For example, Go Ethereum does not indicate what is an acceptable response size.
        It just fails on the server-side with a "context was cancelled" warning.

        :param func: A callable that triggers Ethereum JSON-RPC, as func(start_block, end_block)
        :param from_block: The initial start block of the block range
        :param to_block: The initial start block of the block range

        """

        for i in range(self.max_request_retries):

            try:
                return to_block, func(from_block, to_block)
            except Exception as e:
                if i < self.max_request_retries - 1:
                    logger.warning(f"Request failed for block {from_block} - {to_block}\n\n {e}")

                    to_block = from_block + ((to_block - from_block) // 2)
                    time.sleep(self.request_retries_delay)
                    logger.info(f"retrying for block {from_block} - {to_block}")
                else:
                    logger.warning("out of retries")

    def get_block_timestamp(self, block_num: int) -> datetime.datetime:
        """
        get timestamp for a particular block
        :param block_num:
        :return:
        """
        try:
            block_info = self.w3.eth.get_block(block_num)
            return datetime.datetime.fromtimestamp(block_info["timestamp"]).strftime("%Y-%m-%d")
        except BlockNotFound:
            # Block was not mined yet
            return None

    def scan_chunk(self, start_block: int, end_block: int) -> Tuple[int, datetime.datetime, dict]:
        """
        Scan for event in a particular block range
        :param start_block:
        :param end_block:
        :return:
        """
        end_block, event_log = self._retry_web_call(self.get_contract_event, start_block, end_block)
        end_block_timestamp = self.get_block_timestamp(end_block)
        event_data = self.process_event_logs(event_log)

        return end_block, end_block_timestamp, event_data

    def scan_block(self, start_block: int, end_block: int):
        """
        Scan event from start to end block
        :param start_block:
        :param end_block:
        :return:
        """

        logger.info(f"scanning from block {start_block} - {end_block} in chunks of {self.chunk_size}")
        current_block = int(start_block)
        current_chunk_size = self.chunk_size
        current_end_block = self.next_chunk(current_block, end_block, current_chunk_size)

        time_tracker = []
        while current_block < int(end_block):
            start = datetime.datetime.now()
            logger.info(
                f"scanning chunk range {current_chunk_size} from block {current_block} - {current_end_block} started!!")

            current_end_block, end_block_timestamp, transaction_data = self.scan_chunk(current_block, current_end_block)

            self.state.process_data(transaction_data)
            self.state.store_state(current_end_block, end_block_timestamp)
            logger.info(
                f"scanning chunk range {current_chunk_size} from block {current_block} - {current_end_block} completed!!")
            end = datetime.datetime.now() - start
            time_tracker.append(end.seconds)

            time_tracker, current_chunk_size = self.predict_next_chunk(current_chunk_size, time_tracker)
            current_block = current_end_block
            current_end_block = self.next_chunk(current_end_block, end_block, current_chunk_size)

        logger.info(f"Block scan completed!!!")

        return True

    def process_event_logs(self, event_logs: list) -> list:
        """
        Process event logs and return formatted data
        :param event_logs:
        :return:
        """
        block_timestamp = {}
        event_data = []
        for event in event_logs:
            block_number = event.blockNumber
            if block_number not in block_timestamp:
                block_timestamp[block_number] = self.get_block_timestamp(block_number)

            tx_hash = event.transactionHash.hex()

            args = event["args"]
            transfer = {
                "address": self.filters.get("address"),
                "tx_hash": tx_hash,
                "from": args["from"],
                "to": args.to,
                "value": args.value,
                "block_number": block_number,
                "block_timestamp": block_timestamp[block_number],
                "log_index": event.logIndex
            }
            event_data.append(transfer)

        return event_data

    def get_contract_event(self, from_block: int, to_block: int) -> list:
        """
        get event of type for a particular address
        :param from_block:
        :param to_block:
        :return:
        """

        abi = self.event._get_event_abi()
        codec: ABICodec = self.w3.codec
        data_filter_set, event_filter_params = construct_event_filter_params(
            event_abi=abi, abi_codec=codec,
            address=Web3.to_checksum_address(self.filters.get("address")),
            argument_filters=self.filters, fromBlock=from_block,
            toBlock=to_block
        )

        logger.debug(f"Querying eth_getLogs with the following parameters: {event_filter_params}")
        logger.info(f"Querying eth_getLogs with for the following block range: {from_block} - {to_block}")

        logs = self.w3.eth.get_logs(event_filter_params)
        all_event = []

        for log in logs:
            event = get_event_data(codec, abi, log)
            all_event.append(event)

        return all_event

    def next_chunk(self, current_chunk: int, end_block: int, chunk_size: int) -> int:
        """
        get the max block for the next chunk, always ignore the last two blocks
        :param current_chunk:
        :return:
        """

        chunk = current_chunk + chunk_size

        if chunk > int(end_block):
            return end_block

        return chunk

    def predict_next_chunk(self, chunk: int, time_list: list) -> Tuple[list, int]:
        """
        Increase or decrease chunk size according to time taken to process last 3 chunk
        :param chunk:
        :param time_list:
        :return:
        """
        new_chunk = int(chunk * self.chunk_increase_size)
        if 0 in time_list:
            time_list.remove(0)

        if len(time_list) > 2 and max(time_list) > 3:

            time_list = time_list[::-1][:3]

            if max(time_list) == time_list[0]:  # increase chunk

                return time_list[::-1], chunk - new_chunk

            elif min(time_list) == time_list[0]:  # decrease chunk
                chunk_size = self.max_chunk_increase_size if (new_chunk + chunk) > int(self.max_chunk_increase_size) \
                    else new_chunk + chunk

                return time_list[::-1], chunk_size
            else:
                return time_list[::-1], chunk

        elif len(time_list) > 2 and max(time_list) <= 3:  # double chunk increase  by ratio
            chunk_size = self.max_chunk_increase_size if (chunk + (new_chunk * 2)) > int(
                self.max_chunk_increase_size) else chunk + (new_chunk * 2)

            return time_list, chunk_size

        else:
            print(time_list, "1")
            return time_list, chunk
