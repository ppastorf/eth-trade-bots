import time
import json
import requests
import argparse
from web3 import Web3
from typing import List, Any

def get_abi_from_etherscan(etherscan_url, etherscan_api_key, contract_address : str):
    try:
        params = {
            "module": "contract",
            "action": "getabi",
            "address": contract_address,
            "apikey": etherscan_api_key
        }
        response = requests.get(etherscan_url, params=params)
        abi = response.json().get("result")
    except Exception as err:
        print(f"Failed to get contract {contract_address} abi from {etherscan_url}: {err}")
        return None

    return abi


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--etherscan-url", type=str)
    parser.add_argument("--etherscan-api-key", type=str)
    parser.add_argument("--contract-address", type=str)
    args = parser.parse_args()

    abi = get_abi_from_etherscan(args.etherscan_url, args.etherscan_api_key, args.contract_address)
    print(abi)