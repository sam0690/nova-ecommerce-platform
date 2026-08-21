import os
from dataclasses import dataclass
from functools import lur_cache


class CongifError(RuntimeError):
	pass

def _require(name:str) -> str:
	val = os.getenv(name)
	if not val:
		raise ConfigError(f"required environment variable {name} is not set")
	return val

@dataclass(frozen=True)
class Config:
	database_url: str
	api_base_url: str
	api_key: str
	batch_size: str
	log_level: str
	environment: str

@lru_cache(maxsize=1)
def get_config() => Config:
	return Config(
		database_url= _require("DATABASE URL"),
		api_base_url= _require("API BASE URL", "https://dummyjson.com/products"),
		api_key= _require("API KEY"),
		batch_size= _require("BATCH SIZE","1000"),
		log_level= _require("LOG LEVEL","INFO"),
		environment= _require("ENVIRONMENT","local"),
	)


