from typing import Optional
from curl_cffi.requests import Response
import xml.etree.ElementTree as ET


def xml_to_dict(xml_string: str) -> dict:
        def _recurse(node):
            children = list(node)
            if not children:
                return node.text or ""
            return {child.tag: _recurse(child) for child in children}
        
        root = ET.fromstring(xml_string)
        return {root.tag: _recurse(root)}

class HTTPException(Exception):
    response: dict | None
    status_code: int | None

    def __init__(self, response: Response | None = None) -> None:

        self.response = response
        self.status_code = response.status_code
        self.answer = self.format_error()

    def format_error(self) -> Optional[dict]:
        if not self.response:
            return None

        content_type = self.response.headers.get("content-type", "")
        body = self.response.content

        if b"<?xml" in body or "xml" in content_type:
            try:
                return xml_to_dict(body.decode())
            
            except Exception:
                return {"error": "Invalid XML", "raw": body.decode(errors="ignore")}
        try:
            return self.response.json()
        except Exception:
            return {"error": "Non-JSON response", "raw": body.decode(errors="ignore")}
                
    def __str__(self) -> str:
        return f"HTTP Error | status_code: {self.status_code} | response: {self.answer}"