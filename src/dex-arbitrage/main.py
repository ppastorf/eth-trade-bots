import time
import yaml
import redis
import argparse
import requests
import threading
from web3 import Web3
from typing import List, Any
from datetime import datetime
from itertools import permutations
from collections import defaultdict


# parse config from file
def parse_config_file(filepath: str):
    data = {}
    try:
      with open(filepath, "r") as file:
        data = yaml.safe_load(file)
    except Exception as e:
      print(f"ERROR: Error loading config file: {e}")
      return None

    return data

parser = argparse.ArgumentParser()
parser.add_argument("--config", type=str)

# GLOBALS
ARGS = parser.parse_args()
CONFIG = parse_config_file(ARGS.config)
PRICES = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
PAIRS  = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
GAS_PRICE = 0


def defaultdict_to_dict(d):
    if isinstance(d, defaultdict):
        d = {key: defaultdict_to_dict(value) for key, value in d.items()}
    return d


def connect_to_redis(redis_host, redis_port: str):
  print(f"Connecting to redis db...")
  try:
    redis_client = redis.Redis.from_url(CONFIG['price_query']['redis_url'])
    redis_client.ping()
    print(f"Succesfully connected to redis db at {redis_host}:{redis_port}")
    return redis_client
  except Exception as e:
    print(f"ERROR: Failed to connect to redis db: {e}")
    return None


def query_prices_http():
  try:
    response = requests.get(f"{CONFIG['price_query']['http_url']}/prices", timeout=5)
    response.raise_for_status()
    return response.json()
  except Exception as http_err:
    print(f"ERROR: Error fetching prices: {http_err}")
    return None


def query_prices():
  global PRICES
  print(f"Starting to query prices...")

  match CONFIG['price_query']['type']:
    # case "redis":
    #   redis_client = connect_to_redis(CONFIG['price_db']['redis_host'], CONFIG['price_db']['redis_port'])

    case "http":
      while True:
        prices = query_prices_http()
        if prices != None: 
          PRICES = prices
        time.sleep(CONFIG['price_query']['period_ms'] / 1000)

    case _:
      raise Exception("config 'price_query.type' not supported.")


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
          PAIRS[pair][exchange_name] = price
      time.sleep(CONFIG['price_query']['period_ms'] / 1000)
    except Exception as e:
      print(f"ERROR: Failed to match prices: {e}")


def calc_min_investment(dex_name: str):
    match dex_name:
      case "uniswap-v2":
        return CONFIG['arbitrage']['tx_value'] * 0.003
      case "uniswap-v3":
        return CONFIG['arbitrage']['tx_value'] * 0.003

      # case "sushiswap-v3":

      # case "curve":

      case _:
        raise Exception(f"Exchange '{dex_name}' not supported.")


def arbitrage_engine_1on1():
  while True:
    try:
      for _, price in PAIRS.items():
        opportunities = []
        for dexA, dexB in permutations(price.keys(), 2):
          opp = dict({
            'buy': dexA,
            'sell': dexB,
            'value': (price[dexB] - price[dexA])
          })
          if opp['value'] > 0:
            opportunities.append(opp)
            print(f"Dex arbitrage opportunity: {opp}")
            min_invest, min_return = calc_min_investment(opp)
            print(f"Min investment: {min_invest} for {min_return} return")
            # print(f"Investing 'tx_value': {min_return} return")
      time.sleep(CONFIG['price_query']['period_ms'] / 1000)
    except Exception as e:
      print(f"ERROR: Arbitrage engine failure: {e}")


def arbitrage_engine():
  print(f"Starting arbitrage engine...")

  match CONFIG['arbitrage']['strategy']:
    case "1on1":
      arbitrage_engine_1on1()
    case _:
      raise Exception("config 'arbitrage.strategy' not supported.")


if __name__ == "__main__":
    threads = []

    # query prices db
    query_thread = threading.Thread(target=query_prices)
    threads.append(query_thread)

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
