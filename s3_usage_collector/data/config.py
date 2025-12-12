import asyncio
from pathlib import Path
import sys
from loguru import logger
from dotenv import load_dotenv
import os
from asyncio import Semaphore

load_dotenv()

semaphore = Semaphore(12)

### APP CONFIG ###

#SCRIPT RUNNING DIR
if getattr(sys, 'frozen', False):
    ROOT_DIR = Path(sys.executable).parent.absolute()
else:
    ROOT_DIR = Path(__file__).parent.parent.absolute()

class Config:
    S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
    S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
    S3_ENDPOINT = os.getenv("S3_ENDPOINT")

#DIRECTORY FOR PATTERN DIRECTORY
if getattr(sys, "frozen", False):
    RES_ROOT_DIR = Path(sys.executable).parent.absolute()
else:
    RES_ROOT_DIR = Path(__file__).resolve().parents[2]

# Directory for write summarized_data.json
RESULTS_DIR = os.path.join(RES_ROOT_DIR, 'results')

# Directory for write raw s3_usage chunks in .json
STATS_CHUNKS_DIR    = os.path.join(ROOT_DIR, 'results', 'stats', 'chunks')

# USAGE_SUMMARY_FILE NAME, default:  summarized_data.json
USAGE_SUMMARY_FILE  = os.path.join(RESULTS_DIR, 'summarized_data.json')

# USAGE_BACKUP_DIR for backups, {USAGE_SUMMARY_FILE}-{datetime}.json
USAGE_BACKUP_DIR    = os.path.join(ROOT_DIR, 'backups')

FILES_DIR = os.path.join(ROOT_DIR, 'logs')
LOG_FILE = os.path.join(FILES_DIR, 'log.log')
ERRORS_FILE = os.path.join(FILES_DIR, 'errors.log')

lock = asyncio.Lock()