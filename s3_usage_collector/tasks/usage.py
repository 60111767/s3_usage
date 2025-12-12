import asyncio
import re
from datetime import datetime, timedelta
from typing import Optional

from loguru import logger

from s3_usage_collector.api.s3client import S3Client
from s3_usage_collector.data.config import semaphore
from s3_usage_collector.utils.upload_cache import UploadCache


class UsageCollector:
    __module__ = 'S3 Usage Collector'

    def __init__(self,
                 access_key: str,
                 secret_key: str,
                 host: str,
                 s3_usage_period_seconds: int = 3600,
                 s3_cache_timeout_seconds: int = 600,
                 save_chunks: bool = False,
                 remove_items: bool = False):

        self.s3_client = S3Client(access_key=access_key, secret_key=secret_key, endpoint=host)
        self.s3_usage_period_seconds = s3_usage_period_seconds
        self.s3_cache_timeout_seconds = s3_cache_timeout_seconds
        self.remove_items=remove_items
        self.save_chunks = save_chunks
        self.cache = UploadCache()

    async def get_stats(self, obj) -> list:
        async with semaphore:
            logger.debug(f'[{self.__module__}] | Started Collect {obj}')

            usage = await self.s3_client.get_ostor_usage(obj=obj)
            logger.debug(f'[{self.__module__}] | Usage - got {obj}')

            self.cache.add_raw_stats_for_object(obj, usage)

            if self.save_chunks:
                self.cache.save_object_stats(obj)

            data = usage.get("items") or []

            for item in data:
                key_data = item.get("key", {})
                bucket = key_data.get("bucket")
                user_id = key_data.get("user_id")

                if not bucket or not user_id:
                    continue

                counters = item.get("counters", {})
                if not counters:
                    continue

                self.cache.add_usage_item(
                    bucket=bucket,
                    user_id=user_id,
                    counters=counters,
                )

                logger.debug(
                    f"Aggregated stats: object={obj}, bucket={bucket}, "
                    f"user_id={user_id}, storage_types={list(counters.keys())}"
                )

            # После успешной обработки удаляем usage-объект из S3

            #await self.s3_client.delete_ostor_usage_obj(obj=obj)

            return data

    async def delete_s3_stat_object(self, obj):

        try:
            await self.s3_client.delete_ostor_usage_obj(obj=obj)
            logger.info(f"{self.__module__} | Success deleted s3 stat object: {obj}")

        except Exception as e:
            logger.error(f"{self.__module__} | Error in  deleting s3 stat object: {obj} | {e}")

    def _parse_timestamp_from_object_name(self, obj_name: str) -> Optional[datetime]:
        pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z)'
        match = re.search(pattern, obj_name)
        
        if not match:
            logger.warning(f"Could not parse timestamp from object name: {obj_name}")
            return None
        
        timestamp_str = match.group(1)
        try:
            if '.' in timestamp_str:
                dt = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ')
            else:
                dt = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%SZ')

            return dt
        except ValueError as e:
            logger.warning(f"Failed to parse timestamp '{timestamp_str}' from object '{obj_name}': {e}")
            return None

    def _filter_ready_objects(self, items: list[str]) -> list[str]:
        if not items:
            return []

        parsed: list[tuple[str, datetime]] = []
        skipped_no_ts = 0

        for obj_name in items:
            ts = self._parse_timestamp_from_object_name(obj_name)
            if ts is None:
                logger.debug(f"[{self.__module__}] | Skip '{obj_name}' (no parsable timestamp)")
                skipped_no_ts += 1
                continue
            parsed.append((obj_name, ts))

        if not parsed:
            logger.warning(f"[{self.__module__}] | No parsable timestamps, skip all objects")
            return []

        latest_ts = max(ts for _, ts in parsed)

        cutoff_ts = latest_ts - timedelta(
            seconds=self.s3_usage_period_seconds
        )

        logger.info(
            f"[{self.__module__}] | Cutoff timestamp (UTC): {cutoff_ts.isoformat()} "
            f"(latest={latest_ts.isoformat()}, "
            f"period={self.s3_usage_period_seconds}, "
            f"cache={self.s3_cache_timeout_seconds})"
        )

        parsed.sort(key=lambda x: x[1], reverse=True)

        ready_items: list[str] = []
        skipped_fresh = 0

        for obj_name, ts in parsed:
            if ts > cutoff_ts:
                skipped_fresh += 1
                logger.debug(
                    f"[{self.__module__}] | Skip '{obj_name}' "
                    f"(ts={ts} >= cutoff_ts={cutoff_ts})"
                )
                continue

            ready_items.append(obj_name)
            logger.debug(
                f"[{self.__module__}] | Include '{obj_name}' "
                f"(ts={ts} < cutoff_ts={cutoff_ts})"
            )

        logger.info(
            f"[{self.__module__}] | Ready objects: {len(ready_items)} "
            f"of {len(items)} total (skipped {skipped_no_ts} no-ts, {skipped_fresh} fresh)"
        )
        return ready_items

    async def ostor_usage(self):
        try:

            all_stats = await self.s3_client.get_ostor_usage()
            items = all_stats.get('items', [])
            logger.debug(f"[{self.__module__}] | S3_Stats | Got {len(items)} objects from statistics")

            filtered_items = self._filter_ready_objects(items)

            received_items = len(items)
            processed_requests = len(filtered_items)

            if not filtered_items:
                logger.warning(f"[{self.__module__}] | No objects to process (all in guard zone)")

                return self.cache.build_usage_summary(
                    received_items=received_items,
                    processed_requests=processed_requests,
                )

            self.cache.reset_usage_aggregate()
            tasks = [self.get_stats(obj) for obj in filtered_items]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            summary = self.cache.build_usage_summary(
                received_items=received_items,
                processed_requests=processed_requests,
            )

            if self.remove_items:
                tasks = [self.delete_s3_stat_object(obj) for obj in filtered_items]
                await asyncio.gather(*tasks, return_exceptions=True)

            return summary

        except Exception as e:
            summary = self.cache.build_usage_summary(
                received_items=0,
                processed_requests=0,
                error=True
            )
            logger.error(f"[{self.__module__}] | S3 Collector Flow | Something went wrong | {e} ")

            return summary

