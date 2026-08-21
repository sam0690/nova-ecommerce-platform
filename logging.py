import logging
import os
import sys

def setup_loggin() - > None:\
	logging.basicConfig(
		level= os.getenv("LOG LEVEL",INFO),
		format= "%(asctime)s %(levelname)-8s %(name)s %(message)s",
		datefmt= "%Y-%m-%dT%H:%M:%S:%z",
		stream= sys.stdout,
	)
	logging.getLogger("httpx").setLevel(logging.WARNING)
