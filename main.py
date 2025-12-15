
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import asyncio
import json

from s3_usage_collector.tasks.usage import UsageCollector


def get_params() -> dict:
    params: dict[str, str] = {}

    for arg in sys.argv[1:]:
        if '=' not in arg:
            continue
        key, value = arg.split('=', 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        params[key] = value

    return params


async def main():
    params = get_params()

    access_key = params.get('PUBLIC_S3_KEY')
    secret_key = params.get('SECRET_S3_KEY')
    host = params.get('S3_SERVERNOHTTPS')
    s3_usage_period_seconds = params.get('S3_USAGE_PERIOD', 3600)
    remove_items = params.get('S3_REMOVE_STATS_ITEMS', False)
    save_chunks = params.get('S3_SAVE_STATS_CHUNKS', False)

    s3_client = UsageCollector(
        access_key=access_key,
        secret_key=secret_key,
        host=host,
        s3_usage_period_seconds=int(s3_usage_period_seconds),
        remove_items=remove_items,
        save_chunks=save_chunks
    )

    results = await s3_client.ostor_usage()

    print(json.dumps(results, indent=4))

if __name__ == "__main__":
    asyncio.run(main())
