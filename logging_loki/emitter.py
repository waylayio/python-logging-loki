# -*- coding: utf-8 -*-

import copy
import functools
import json
import logging
import threading
import time
from logging.config import ConvertingDict
from typing import Any, Dict, Optional, Tuple

import requests

from logging_loki import const

BasicAuth = Optional[Tuple[str, str]]


class LokiEmitter:
    """Base Loki emitter class."""

    success_response_code = const.success_response_code
    level_tag = const.level_tag
    logger_tag = const.logger_tag
    label_allowed_chars = const.label_allowed_chars
    label_replace_with = const.label_replace_with
    session_class = requests.Session

    def __init__(self, 
        url: str, 
        tags: Optional[dict] = None, 
        headers: Optional[dict] = None, 
        auth: BasicAuth = None, 
        as_json: bool = False,
        props_to_labels: Optional[list[str]] = None
    ):
        """
        Create new Loki emitter.

        Arguments:
            url: Endpoint used to send log entries to Loki (e.g. `https://my-loki-instance/loki/api/v1/push`).
            tags: Default tags added to every log record.
            auth: Optional tuple with username and password for basic HTTP authentication.

        """
        #: Tags that will be added to all records handled by this handler.
        self.tags = tags or {}
        #: Headers that will be added to all requests handled by this emitter.
        self.headers = headers or {}
        #: Loki JSON push endpoint (e.g `http://127.0.0.1/loki/api/v1/push`)
        self.url = url
        #: Optional tuple with username and password for basic authentication.
        self.auth = auth
        #: Optional bool, send record as json?
        self.as_json = as_json
        #: Optional list, send record as json?
        self.props_to_labels = props_to_labels or []

        self._session: Optional[requests.Session] = None
        self._lock = threading.Lock()

    def __call__(self, record: logging.LogRecord, line: str):
        """Send log record to Loki."""
        # Prevent "recursion" when e.g. urllib3 logs debug messages on POST
        if not self._lock.acquire(blocking=False):
            return
        try:
            payload = self.build_payload(record, line)
            self._post_to_loki(payload)
        finally:
            self._lock.release()

    def _post_to_loki(self, payload: dict):
        resp = self.session.post(self.url, json=payload, headers=self.headers)
        # TODO: Enqueue logs instead of raising an error and losing the logs
        if resp.status_code != self.success_response_code:
            raise ValueError("Unexpected Loki API response status code: {0}".format(resp.status_code))


    @property
    def session(self) -> requests.Session:
        """Create HTTP session."""
        if self._session is None:
            self._session = self.session_class()
            self._session.auth = self.auth or None
        return self._session

    def close(self):
        """Close HTTP session."""
        if self._session is not None:
            self._session.close()
            self._session = None

    @functools.lru_cache(const.format_label_lru_size)
    def format_label(self, label: str) -> str:
        """
        Build label to match prometheus format.

        `Label format <https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels>`_
        """
        for char_from, char_to in self.label_replace_with:
            label = label.replace(char_from, char_to)
        return "".join(char for char in label if char in self.label_allowed_chars)

    def build_tags(self, record: logging.LogRecord) -> Dict[str, Any]:
        """Return tags that must be send to Loki with a log record."""
        tags = dict(self.tags) if isinstance(self.tags, ConvertingDict) else self.tags
        tags = copy.deepcopy(tags)
        tags[self.level_tag] = record.levelname.lower()
        tags[self.logger_tag] = record.name

        extra_tags = {k: getattr(record, k) for k in self.props_to_labels if getattr(record, k, None)}
        if isinstance(passed_tags := getattr(record, "tags", {}), dict):
            extra_tags = extra_tags | passed_tags

        
        for tag_name, tag_value in extra_tags.items():
            cleared_name = self.format_label(tag_name)
            if cleared_name:
                tags[cleared_name] = tag_value

        return tags

    def build_payload(self, record: logging.LogRecord, line) -> dict:
        """Build JSON payload with a log entry."""
        labels = self.build_tags(record)
        ns = 1e9
        ts = str(int(time.time() * ns))

        line = json.dumps(record, default=lambda obj: obj.__dict__) if self.as_json else line

        stream = {
            "stream": labels,
            "values": [[ts, line]],
        }
        return {"streams": [stream]}
    
    def emit_batch(self, records: list[ Tuple[logging.LogRecord, str]]):
        """Send log record to Loki."""
        streams = [self.build_payload(record[0], record[1])["streams"][0] for record in records]
        self._post_to_loki({"streams": streams})
