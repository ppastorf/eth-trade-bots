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
##### UTILS
#############################

def parse_config_file(filepath: str):
    data = {}
    if filepath == None:
      return data

    try:
      with open(filepath, "r") as file:
        data = yaml.safe_load(file)
    except Exception as e:
      print(f"Error loading config file: {e}")
      return None

    return data

def defaultdict_to_dict(d):
    if isinstance(d, defaultdict):
        d = {key: defaultdict_to_dict(value) for key, value in d.items()}
    return d

def get_now_timestamp():
  return datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d_%H:%M:%S.%f")

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

#############################
##### CLASSES
#############################

class ERC20Token:
  def __init__(self, name, address: str, decimals: int):
     self.decimals = decimals
     self.address = Web3.to_checksum_address(address)
     self.name = name
     self.abi = get_abi_from_file("abi/erc20.json")

class UniswapTokenPair:
  def __init__(self, type, exchange_name, token0, token1, contract_address, router_address=None):
    self.exchange_name = exchange_name
    self.type = type
    self.token0 = token0
    self.token1 = token1
    self.name = f"{token0.name}/{token1.name}"

    match self.type:
      case "uniswap-v2":
        self.router_contract = rpc_conn.eth.contract(
          address=Web3.to_checksum_address(router_address),
          abi=get_abi_from_file("abi/uniswap-v2-router02.json")
        )
        self.pair_contract = rpc_conn.eth.contract(
          address=Web3.to_checksum_address(contract_address),
          abi=get_abi_from_file("abi/uniswap-v2-pool.json")
        )
      case "uniswap-v3":
        self.router_contract = None
        self.pair_contract = rpc_conn.eth.contract(
          address=Web3.to_checksum_address(contract_address),
          abi=get_abi_from_file("abi/uniswap-v3-pool.json")
        )
      case _:
          self.router_contract = None
          self.pair_addr = None


# #############################
# ##### GLOBALS
# #############################

parser = argparse.ArgumentParser()
parser.add_argument("--config", type=str)
args = parser.parse_args()

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


def monitor_price(pair: UniswapTokenPair):
    global PRICES
    print(f"Monitoring pair {pair.name} of type {pair.type} at {pair.pair_contract.address}...")

    while True:
      match pair.type:
        case "uniswap-v2":
          price = get_price_uniswap_v2(pair)
        case "uniswap-v3":
          price = get_price_uniswap_v3(pair)
        case _:
          raise Exception(f"Token pair type '{pair.type}' not supported.")

      if price == None:
          print(f"{CONFIG['network_name']} - {pair.exchange_name} - {pair.name}: failed to fetch price.")
          continue

      print(f"{CONFIG['network_name']} - {pair.exchange_name} - {pair.name}: {price:.18f}")
      PRICES[CONFIG['network_name']][pair.exchange_name][pair.name] = price
      # PRICES['last_updated'] = get_now_timestamp()
      time.sleep(CONFIG['monitor_period_ms'] / 1000)

#############################
##### DEX CALLS 
#############################

def get_price_uniswap_v3(pair: UniswapTokenPair):
    try:
        slot0 = pair.pair_contract.functions.slot0().call()
        sqrt_price_x96 = slot0[0]
        sqrt_price = sqrt_price_x96 / (2 ** 96)
        price = (sqrt_price ** 2) * 10**(pair.token0.decimals - pair.token1.decimals)
        return price
    except Exception as e:
        print(f"Error fetching uniswap-v3 price: {e}")
        return None


def calculate_amount_out(amount_in, reserve_in, reserve_out):
    amount_in_with_fee = amount_in * 997
    numerator = amount_in_with_fee * reserve_out
    denominator = reserve_in * 1000 + amount_in_with_fee
    return numerator // denominator


def get_price_uniswap_v2(pair: UniswapTokenPair):
    try:
        reserves = pair.pair_contract.functions.getReserves().call()
        reserve_in = reserves[0]
        decimals_in = pair.token0.decimals
        reserve_out = reserves[1]
        decimals_out = pair.token1.decimals

        # base calculation
        price = reserve_out / reserve_in

        # getAmountOut caculation
        # amount_in_wei = Web3.to_wei(1, 'ether')
        # price = calculate_amount_out(amount_in_wei, reserve_in, reserve_out)
        # price = Web3.from_wei(price, 'ether')

        price = price * 10**(decimals_in - decimals_out)
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

    # parse config pairs
    pairs = []
    for exchange_name, exchange_config in CONFIG['exchanges'].items():
      token_type = exchange_config['token_type']
      try:
        router_address = exchange_config['router_address']
      except:
        router_address = None

      for pair_config in exchange_config['pairs']:
        contract_address = pair_config['address']
        token0 = ERC20Token(**pair_config['token0'])
        token1 = ERC20Token(**pair_config['token1'])
        pairs.append(UniswapTokenPair(
          token_type,
          exchange_name,
          token0,
          token1,
          contract_address,
          router_address=router_address
        ))


    #############################
    ##### MAIN THREADS
    #############################
    threads = []

    # API server thread
    server = threading.Thread(target=run_api_srv)
    threads.append(server)

    # monitor prices
    for pair in pairs:
      monitor = threading.Thread(target=monitor_price, args=(pair,))
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
