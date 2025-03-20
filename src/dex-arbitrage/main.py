import time
import yaml
import json
import argparse
import threading
from web3 import Web3
from typing import List, Any

TOKEN_DECIMALS = 18

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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str)
    args = parser.parse_args()

    config = parse_config_file(args.config)

    print(f"Connecting to {config['network_name']} RPC Node...")
    rpc_conn = connect_to_rpc_node(config['rpc_url'])

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
