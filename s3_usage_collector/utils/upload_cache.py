import json
import os
from datetime import datetime
from typing import Dict, Optional, Tuple
from loguru import logger

from s3_usage_collector.data.config import (
    RESULTS_DIR,
    STATS_CHUNKS_DIR,
    USAGE_SUMMARY_FILE,
    USAGE_BACKUP_DIR,
)

class UploadCache:
    def __init__(self):
        self.current_upload: Dict[str, Dict[str, float]] = {}
        self.current_stats: Dict[str, Dict] = {}
        self.current_buckets: Dict[str, dict] = {}
        self.usage_aggregate: Dict[Tuple[str, str], Dict] = {}

        self._ensure_directories()

    def _ensure_directories(self):
        os.makedirs(RESULTS_DIR, exist_ok=True)
        os.makedirs(STATS_CHUNKS_DIR, exist_ok=True)
        os.makedirs(USAGE_BACKUP_DIR, exist_ok=True)

    def save_usage_summary_to_file(self, summary: dict) -> tuple[Optional[str], Optional[str]]:

        if not summary:
            logger.warning("No usage summary to save")
            return None, None

        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        backup_path = os.path.join(USAGE_BACKUP_DIR, f"usage_summary_{ts}.json")
        results_usage_log = os.path.join(RESULTS_DIR, f"summarized_data_{ts}.json")

        main_path = USAGE_SUMMARY_FILE

        try:
            with open(main_path, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)

            with open(results_usage_log, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)

            with open(backup_path, "w", encoding="utf-8") as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved usage summary to '{main_path}', backup='{backup_path}'")
            return main_path, backup_path

        except Exception as e:
            logger.error(f"Failed to save usage summary (main='{main_path}', backup='{backup_path}'): {e}")
            return None, None


    def add_upload(self, bucket: str, storage_type: str, size_mb: float):
        if bucket not in self.current_upload:
            self.current_upload[bucket] = {}
        self.current_upload[bucket][storage_type] = (
            self.current_upload[bucket].get(storage_type, 0.0) + size_mb
        )
        logger.debug(f"Added upload: {bucket} ({storage_type}) - {size_mb} MiB")

    def save_current_upload(self) -> Optional[str]:
        if not self.current_upload:
            logger.warning("No upload data to save")
            return None

        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        upload_file = os.path.join(CHUNKS_UPLOAD_DIR, f"upload_{date_str}.json")

        try:
            with open(upload_file, "w", encoding="utf-8") as f:
                json.dump(self.current_upload, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved current upload to {upload_file}")

        except Exception as e:
            logger.error(f"Failed to save upload file {upload_file}: {e}")
            return None

        self.current_upload = {}
        return upload_file

    def get_current_upload(self) -> Dict[str, Dict[str, float]]:
        return {b: st.copy() for b, st in self.current_upload.items()}

    def reset_current_upload(self):
        self.current_upload = {}
        logger.debug("Current upload data reset")

    def add_raw_stats_for_object(self, object_name: str, raw_usage: dict):

        self.current_stats[object_name] = raw_usage
        logger.debug(f"Added raw stats for object '{object_name}'")

    def save_object_stats(self, object_name: str) -> Optional[str]:
        data = self.current_stats.get(object_name)
        if not data:
            logger.warning(f"No stats data for object '{object_name}' to save")
            return None

        stats_file = os.path.join(STATS_CHUNKS_DIR, f"{object_name}.json")

        try:
            with open(stats_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved stats for object '{object_name}' to {stats_file}")
        except Exception as e:
            logger.error(f"Failed to save stats file {stats_file}: {e}")
            return None

        del self.current_stats[object_name]
        return stats_file

    def get_object_stats(self, object_name: str) -> dict:
        return self.current_stats.get(object_name, {}).copy()

    @staticmethod
    def _merge_counters(dst: dict, src: dict):
        for key, value in src.items():
            if isinstance(value, dict):
                node = dst.setdefault(key, {})
                UploadCache._merge_counters(node, value)
            else:
                try:
                    dst[key] = dst.get(key, 0) + value
                except TypeError:
                    dst[key] = value

    def add_usage_item(self, bucket: str, user_id: str, counters: dict):
        key = (bucket, user_id)
        if key not in self.usage_aggregate:
            self.usage_aggregate[key] = json.loads(json.dumps(counters))
        else:
            self._merge_counters(self.usage_aggregate[key], counters)

        logger.debug(
            f"Aggregated usage for bucket='{bucket}', user_id='{user_id}' "
            f"(types: {list(counters.keys())})"
        )

    def build_usage_summary(
        self,
        received_items: int = 0,
        processed_requests: int = 0,
        error: bool = False,
    ) -> dict:

        summarized_data = []
        for (bucket, user_id), counters in self.usage_aggregate.items():
            summarized_data.append(
                {
                    "bucket": bucket,
                    "user_id": user_id,
                    "counters": {
                        "counters": counters,
                    },
                }
            )

        if processed_requests == 0 and not error:
            result = {
                "status": "skip",
                "received_items": received_items,
                "processed_requests": processed_requests,
                "summarized_data": [],
            }

        elif error:

            result = {
                "status": "error",
                "received_items": received_items,
                "processed_requests": processed_requests,
                "summarized_data": summarized_data,
            }

        else:
            result = {
                "status": "done",
                "received_items": received_items,
                "processed_requests": processed_requests,
                "summarized_data": summarized_data,
            }

        logger.info(
            f"Built usage summary: buckets={len(summarized_data)}, "
            f"received_items={received_items}, processed_requests={processed_requests}"
        )

        self.save_usage_summary_to_file(result)

        return result

    def reset_usage_aggregate(self):
        self.usage_aggregate = {}
        logger.debug("Usage aggregate reset")

    def add_bucket_stats(self, bucket_name: str, bucket_data: dict):
        self.current_buckets[bucket_name] = bucket_data.copy()
        logger.debug(f"Added bucket stats for '{bucket_name}'")

    def save_bucket_stats(self, bucket_name: str) -> Optional[str]:
        data = self.current_buckets.get(bucket_name)
        if not data:
            logger.warning(f"No stats data for bucket '{bucket_name}' to save")
            return None

        stats_file = os.path.join(BUCKETS_CHUNKS_DIR, f"{bucket_name}.json")

        try:
            with open(stats_file, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved stats for bucket '{bucket_name}' to {stats_file}")
        except Exception as e:
            logger.error(f"Failed to save bucket stats file {stats_file}: {e}")
            return None

        del self.current_buckets[bucket_name]
        return stats_file

    def get_bucket_stats(self, bucket_name: str) -> dict:
        return self.current_buckets.get(bucket_name, {}).copy()
