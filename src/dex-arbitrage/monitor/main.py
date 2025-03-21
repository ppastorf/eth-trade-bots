import time
import yaml
import json
import argparse
import threading
from web3 import Web3
from typing import List, Any
import uvicorn
from fastapi import FastAPI
from collections import defaultdict
from fastapi.responses import JSONResponse

TOKEN_DECIMALS = 18
PRICES = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))


def defaultdict_to_dict(d):
    if isinstance(d, defaultdict):
        d = {key: defaultdict_to_dict(value) for key, value in d.items()}
    return d


def connect_to_rpc_node(rpc_url: str):
    rpc_conn = Web3(Web3.HTTPProvider(rpc_url))
    if not rpc_conn.is_connected():
        print("Failed to connect to Ethereum node.")
        return None

    return rpc_conn


def get_abi_from_file(abi_filepath: str):
    print(f"Getting ABI at {abi_filepath}...")
    abi = {}
    try:
      with open(abi_filepath, "r") as file:
        abi = json.load(file)
    except Exception as e:
      print(f"Error loading pair ABI at {abi_filepath}: {e}")
      return None

    return abi


def monitor_price(network_name: str, rpc_conn, pair_abi: Any, monitor_period_ms: int, exchange_name, pair_name, pair_address: str, token0_decimals, token1_decimals: int):
    print(f"Monitoring pair {pair_name} at {pair_address}...")

    pair_contract = rpc_conn.eth.contract(address=pair_address, abi=pair_abi)
    while True:
      price = get_price_uniswap_v3(pair_contract, token0_decimals, token1_decimals)
      if price:
          print(f"{network_name} - {exchange_name} - {pair_name}: {price:.18f}")
          PRICES[network_name][exchange_name][pair_name] = price
          PRICES['last_updated'] = int(time.time())
      else:
          print(f"{network_name} - {exchange_name} - {pair_name}: failed to fetch price.")
      time.sleep(monitor_period_ms / 1000)


def get_price_uniswap_v3(pair_contract, token0_decimals, token1_decimals):
    try:
        slot0 = pair_contract.functions.slot0().call()
        sqrt_price_x96 = slot0[0]
        sqrt_price = sqrt_price_x96 / (2 ** 96)
        price = (sqrt_price ** 2) * 10**(token0_decimals + token1_decimals)
        # price = (sqrt_price ** 2) 
        return price
    except Exception as e:
        print(f"Error fetching reserves: {e}")
        return None


def parse_config_file(filepath: str):
    data = {}
    try:
      with open(filepath, "r") as file:
        data = yaml.safe_load(file)
    except Exception as e:
      print(f"Error loading config file: {e}")
      return None

    return data


api = FastAPI()
@api.get("/prices")
def get_prices():
    response = JSONResponse(content=defaultdict_to_dict(PRICES))
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, proxy-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def run_api_srv(host, port: str):
    print(f"Api server running at {host}:{port}")
    uvicorn.run(api, host=host, port=port)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str)
    args = parser.parse_args()

    config = parse_config_file(args.config)

    print(f"Connecting to {config['network_name']} RPC Node...")
    rpc_conn = connect_to_rpc_node(config['rpc_url'])

    # API server thread
    thread = threading.Thread(target=run_api_srv, args=(
      config['api']['server_host'],
      config['api']['server_port']
    ))
    thread.start()

    # monitor threads
    monitor_threads = []

    for exchange_name, exchange_config in config['exchanges'].items():
      pair_abi = get_abi_from_file(f"abi/{exchange_config['pair_abi_file']}")
      for pair_name, pair_config in exchange_config['pairs'].items():
        monitor = threading.Thread(target=monitor_price, args=(
            config['network_name'],
            rpc_conn, pair_abi,
            config['monitor_period_ms'],
            exchange_name, pair_name,
            pair_config['address'],
            pair_config['token0_decimals'],
            pair_config['token1_decimals']
        ))
        monitor_threads.append(monitor)
        monitor.start()

    for t in monitor_threads:
      try:
        t.join()
      except KeyboardInterrupt:
        exit(1)

if __name__ == "__main__":
    main()
