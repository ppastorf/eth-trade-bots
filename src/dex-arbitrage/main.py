import time
import yaml
import json
import uvicorn
import argparse
import threading
from web3 import Web3
from typing import List, Any
from fastapi import FastAPI
from datetime import datetime
from itertools import permutations
from collections import defaultdict
from fastapi.responses import JSONResponse

#############################
##### ARGS
#############################

def parse_config_file(filepath: str):
    data = {}
    try:
      with open(filepath, "r") as file:
        data = yaml.safe_load(file)
    except Exception as e:
      print(f"Error loading config file: {e}")
      return None

    return data


parser = argparse.ArgumentParser()
parser.add_argument("--config", type=str)
args = parser.parse_args()


#############################
##### GLOBALS
#############################

CONFIG = parse_config_file(args.config)
PRICES = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
PAIRS  = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
OPPORTUNITIES  = []
UNISWAP_V3_FEES = {
  # "abc": 0.0001,
  "ETH/USDC": 0.0005,
  # "def": 0.003,
  "xyz": 0.01
}


#############################
##### UTILS
#############################

def defaultdict_to_dict(d):
    if isinstance(d, defaultdict):
        d = {key: defaultdict_to_dict(value) for key, value in d.items()}
    return d

def get_now_timestamp():
  return datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d_%H:%M:%S.%f")


#############################
##### DEX ARBITRAGE
#############################

def get_gas_price():
  global GAS_PRICE
  while True:
    try:
      GAS_PRICE = 1
      time.sleep(CONFIG['price_query']['period_ms'] / 1000)
    except Exception as e:
      print(f"ERROR: Failure getting current gas price: {e}")


def pair_matching():
  global PAIRS
  print(f"Starting to match token pairs...")
  while True:
    try:
      for exchange_name, pairs in PRICES[CONFIG['network_name']].items():
        for pair, price in pairs.items():
          PAIRS[CONFIG['network_name']][pair][exchange_name] = price
      PAIRS['last_updated'] = get_now_timestamp()
      time.sleep(CONFIG['monitor_period_ms'] / 1000)
    except Exception as e:
      print(f"ERROR: Failed to match prices: {e}")


def dex_fees(dex_name, pair_name: str):
    match dex_name:
      case "uniswap-v2":
        return 0.003

      case "uniswap-v3":
        return UNISWAP_V3_FEES[pair_name]

      # case "sushiswap-v3":

      # case "curve":

      case _:
        raise Exception(f"Fee calculation for exchange '{dex_name}' not supported.")


def calc_min_investment(opp: Any):
  Pa = opp['priceBuy']
  Pb = opp['priceSell']
  Fa = dex_fees(opp['buy'], opp['pair'])
  Fb = dex_fees(opp['sell'], opp['pair'])
  I = ( Pa * (1 + Fa) ) / ( Pb * ((Pb/Pa) * (1 - Fb) - (1 + Fa)) )
  # return abs(I)
  return I


def arbitrage_engine_1on1():
  global OPPORTUNITIES
  while True:
    try:
      for pair_name, prices in PAIRS[CONFIG['network_name']].items():
        opportunities = []
        for dexA, dexB in permutations(prices.keys(), 2):
          opp = dict({
            'buy': dexA,
            'sell': dexB,
            'pair': pair_name,
            'priceBuy': prices[dexA],
            'priceSell': prices[dexB]
          })
          if opp['priceSell'] - opp['priceBuy'] > 0:
            opp['minInvestment'] = calc_min_investment(opp)
            opp['shouldPurchase'] = opp['minInvestment'] > 0
            print(f"New arbitrage opportunity: {opp}")
            opportunities.append(opp)

        OPPORTUNITIES = opportunities

      time.sleep(CONFIG['monitor_period_ms'] / 1000)

    except Exception as e:
      print(f"ERROR: Arbitrage engine failure: {e}")


def arbitrage_engine():
  print(f"Starting arbitrage engine...")

  match CONFIG['arbitrage']['strategy']:
    case "1on1":
      arbitrage_engine_1on1()
    case _:
      raise Exception("CONFIG 'arbitrage.strategy' not supported.")


#############################
##### PRICE MONITOR
#############################

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


def monitor_price(
                  rpc_conn, pair_abi: Any,
                  exchange_name, pair_name, pair_address: str,
                  token0_decimals, token1_decimals: int
  ):
    global PRICES
    print(f"Monitoring pair {pair_name} at {pair_address}...")

    pair_contract = rpc_conn.eth.contract(address=pair_address, abi=pair_abi)
    while True:
      match exchange_name:
        case "uniswap-v2":
          price = get_price_uniswap_v2(pair_contract, token0_decimals, token1_decimals)

        case "uniswap-v3":
          price = get_price_uniswap_v3(pair_contract, token0_decimals, token1_decimals)

        # case "sushiswap-v3":

        # case "curve":

        case _:
          raise Exception(f"Exchange '{exchange_name}' not supported.")

      if price == None:
          print(f"{CONFIG['network_name']} - {exchange_name} - {pair_name}: failed to fetch price.")
          continue

      print(f"{CONFIG['network_name']} - {exchange_name} - {pair_name}: {price:.18f}")
      PRICES[CONFIG['network_name']][exchange_name][pair_name] = price
      PRICES['last_updated'] = get_now_timestamp()
      time.sleep(CONFIG['monitor_period_ms'] / 1000)

#############################
##### DEX CALLS 
#############################

def get_price_uniswap_v3(pair_contract, token0_decimals, token1_decimals):
    try:
        slot0 = pair_contract.functions.slot0().call()
        sqrt_price_x96 = slot0[0]
        sqrt_price = sqrt_price_x96 / (2 ** 96)
        price = (sqrt_price ** 2) * 10**(token0_decimals - token1_decimals)
        # price = (sqrt_price ** 2) 
        return price
    except Exception as e:
        print(f"Error fetching uniswap-v3 price: {e}")
        return None


def get_price_uniswap_v2(pair_contract, token0_decimals, token1_decimals):
    try:
        reserves = pair_contract.functions.getReserves().call()
        price = reserves[1] / reserves[0] 
        price = price * 10**(token0_decimals - token1_decimals)
        return price
    except Exception as e:
        print(f"Error fetching uniswap-v2 price: {e}")
        return None


#############################
#### SERVER
#############################

api = FastAPI()
@api.get("/prices")
def get_prices():
    response = JSONResponse(content=defaultdict_to_dict(PRICES))
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, proxy-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@api.get("/pairs")
def get_pairs():
    response = JSONResponse(content=defaultdict_to_dict(PAIRS))
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, proxy-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

@api.get("/opportunities")
def get_opportunities():
    response = JSONResponse(content=defaultdict_to_dict(OPPORTUNITIES))
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, proxy-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

def run_api_srv():
    host = CONFIG['api']['server_host']
    port = CONFIG['api']['server_port']
    print(f"Api server running at {host}:{port}")
    uvicorn.run(api, host=host, port=port)


if __name__ == "__main__":
    print(f"Connecting to {CONFIG['network_name']} RPC Node...")
    rpc_conn = connect_to_rpc_node(CONFIG['rpc_url'])

    #############################
    ##### MAIN THREADS
    #############################
    threads = []

    # API server thread
    server = threading.Thread(target=run_api_srv)
    threads.append(server)

    # monitor threads
    for exchange_name, exchange_config in CONFIG['exchanges'].items():
      pair_abi = get_abi_from_file(f"abi/{exchange_config['pair_abi_file']}")
      for pair_name, pair_config in exchange_config['pairs'].items():
        monitor = threading.Thread(target=monitor_price, args=(
            rpc_conn,
            pair_abi,
            exchange_name,
            pair_name,
            pair_config['address'],
            pair_config['token0_decimals'],
            pair_config['token1_decimals'],
        ))
        threads.append(monitor)

    # token pair matching
    matching_thread = threading.Thread(target=pair_matching)
    threads.append(matching_thread)

    # identify opportunities
    engine_thread = threading.Thread(target=arbitrage_engine)
    threads.append(engine_thread)

    for t in threads:
      t.start()

    for t in threads:
      try:
        t.join()
      except KeyboardInterrupt:
        exit(1)
      except Exception as e:
        print(f"Error: {e}")
        exit(1)
