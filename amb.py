from eth_typing import URI
from web3.providers.rpc import HTTPProvider
from web3.types import RPCEndpoint, RPCResponse
from typing import Any
from requests_auth_aws_sigv4 import AWSSigV4
import requests


class AMBHTTPProvider(HTTPProvider):
    def __init__(self,
                 endpoint_uri: URI, aws_access_key_id: str,
                 aws_secret_access_key: str, region: str = "us-east-1"):
        self.aws_auth = AWSSigV4(
            "managedblockchain",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region=region
        )

        self.session = requests.session()
        super().__init__(endpoint_uri)

    def get_request_kwargs(self) -> dict:
        return {
            "headers": {
                "Content-Type": "application/json",
                "X-Amz-Target": "ManagedBlockchain_v2018_09_24.CreateMember",
            }
        }

    def make_custom_post_request(self,
                                 endpoint_uri: URI, data: bytes,
                                 *args: Any, **kwargs: Any) -> bytes:
        kwargs.setdefault("timeout", 10)
        response = self.session.post(endpoint_uri, data=data, *args, **kwargs, auth=self.aws_auth)

        response.raise_for_status()
        return response.content

    def make_request(self, method: RPCEndpoint, params: Any) -> RPCResponse:
        request_data = self.encode_rpc_request(method, params).decode()
        raw_response = self.make_custom_post_request(
            self.endpoint_uri,
            request_data,
            **self.get_request_kwargs()
        )

        response = self.decode_rpc_response(raw_response)
        return response
