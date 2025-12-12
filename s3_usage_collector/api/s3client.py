import base64
import hmac
import hashlib
import email.utils

from curl_cffi.requests import AsyncSession

from s3_usage_collector.api.expections import HTTPException
from urllib.parse import urlencode, urlparse
from loguru import logger


class S3Client:
    def __init__(self, access_key: str, secret_key: str, endpoint: str):
        self.access_key = access_key
        self.secret_key = secret_key.encode()
        self.endpoint = endpoint.rstrip("/")

    def _make_headers(self, method: str, canonical_path: str):
        s3_date = email.utils.formatdate(usegmt=True)
        content_type = "application/json"

        string_to_sign = f"{method}\n\n{content_type}\n{s3_date}\n{canonical_path}"
        
        signature = base64.b64encode(
            hmac.new(self.secret_key, string_to_sign.encode(), hashlib.sha1).digest()
        ).decode()

        parsed = urlparse(self.endpoint)

        return {
            "Host": parsed.netloc,
            "Accept": "application/json",
            "Content-Type": content_type,
            "Date": s3_date,
            "Authorization": f"AWS {self.access_key}:{signature}"
        }
    
    async def _request(self, method: str, path: str, query: dict = None):
        if query:
            query_string = urlencode(query)
            url_path = f"{path}&{query_string}"

        else:
            url_path = path

        url = f"{self.endpoint}{url_path}"

        headers = self._make_headers(method, path)

        async with AsyncSession() as session:
            try:
                response = await session.request(method=method, url=url, headers=headers, timeout=20)
                status_code = response.status_code
                content_type = response.headers.get("content-type", "")
                if status_code <= 204:

                    if "application/json" in content_type:
                        return response.json()

                    return response.text

                else:
                    raise HTTPException(response=response)

            except Exception as e:
                logger.exception(e)

    async def get_ostor_usage(self, obj: str | None = None):
        path = '/?ostor-usage'

        query = {
            "limit": "1000"
        }
        
        if obj:
            query = {
                **query,
                'obj': obj
            }

        resp = await self._request(
            method='GET',
            path=path,
            query=query
        )

        return resp
    
    async def delete_ostor_usage_obj(self, obj: str):
        path = '/?ostor-usage'

        query = {
            'obj': obj
        }

        resp = await self._request(
            method='DELETE',
            path=path,
            query=query
        )

        return resp

    async def get_users(self, user: str = None):
        path = '/?ostor-users'

        query = {}

        if user:
            query = {
                'emailAddress': user
            }

        resp = await self._request(
            method='GET',
            path=path,
            query=query
        )

        return resp
    
    async def get_buckets(self, bucket_type: str = 'DEFAULT'):
        path = '/?ostor-buckets'

        query = {
            'storage_class': bucket_type
        }

        resp = await self._request(
            method='GET',
            path=path,
            query=query,
        )

        return resp.get('Buckets')
    
    async def get_limits(self, user = None, bucket = None):
        path = '/?ostor-limits'

        if user:
            query = {
                'emailAddress': user
            }
        elif bucket:
            query = {
                'bucket': bucket
            }

        else:
            logger.warning(f'No user or bucket provided...')
            return None
        
        resp = await self._request(
            method='GET',
            path=path,
            query=query
        )

        return resp

    async def get_quotas(self, user = None, bucket = None, default_user = False, default_bucket = False):
        path = '/?ostor-quotas'
        
        if user:
            query = {
                'emailAddress': user
            }
        elif bucket:
            query = {
                'bucket': bucket
            }
        elif default_user:
            query = {
                'default': 'user'
            }
        elif default_bucket:
            query = {
                'default': 'bucket'
            }
            
        else:
            logger.warning(f'No user or bucket provided...')
            return None
        
        resp = await self._request(
            method='GET',
            path=path,
            query=query
        )

        return resp