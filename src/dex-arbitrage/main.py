import time
import yaml
import json
import redis
import uvicorn
import argparse
import requests
import threading
from web3 import Web3
from typing import List, Any
from fastapi import FastAPI
from datetime import datetime
from collections import defaultdict
from fastapi.responses import JSONResponse


# parse config from file
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

# GLOBALS
ARGS = parser.parse_args()
CONFIG = parse_config_file(ARGS.config)
PRICES = {}


def connect_to_redis(redis_host, redis_port: str):
  print(f"Connecting to price database...")
  try:
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
    print(f"Succesfully connected to price database at {redis_host}:{redis_port}")
    return redis_client
  except Exception as e:
    print(f"Failed to connect to price database: {e}")
    return None


def query_prices_http():
  try:
    response = requests.get(f"{CONFIG['monitor']['url']}/prices", timeout=5)
    response.raise_for_status()
    return response.json()
  except Exception as http_err:
    print(f"Error fetching prices: {http_err}")
    return None


def query_prices():
  redis_client = None
  match CONFIG['price_query']['type']:
    # case "redis":
    #   redis_client = connect_to_redis(CONFIG['price_db']['redis_host'], CONFIG['price_db']['redis_port'])
    case "http":
      while True:
        prices = query_prices_http()
        if prices != None: 
          PRICES = prices
          print(PRICES)
        time.sleep(CONFIG['price_query']['period_ms'] / 1000)
    case _:
      raise Exception("config 'price_query.type' not available.")


if __name__ == "__main__":
    # API server thread
    query_thread = threading.Thread(target=query_prices)
    query_thread.start()

    try:
      query_thread.join()
    except KeyboardInterrupt:
      exit(1)
    except Exception as e:
      print(f"Error: {e}")
      exit(1)
