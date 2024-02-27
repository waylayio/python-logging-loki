# -*- coding: utf-8 -*-

from contextlib import redirect_stderr, redirect_stdout
import functools
import logging
from logging.handlers import MemoryHandler, QueueHandler
from logging.handlers import QueueListener
import os
from queue import Queue
import sys
import time
from typing import Callable, Optional, Union
from logging_loki import const

from logging_loki.emitter import BasicAuth, LokiEmitter

LOKI_MAX_BATCH_BUFFER_SIZE = int(os.environ.get('LOKI_MAX_BATCH_BUFFER_SIZE', 10))

# capture original stdout and stderr
_sys_out = sys.stdout
_sys_err = sys.stderr

_error_logger = logging.getLogger(const.LOGLOG_LOGGER_NAME)

def with_original_stdout(method: Callable):
    @functools.wraps(method)
    def _impl(self, *method_args, **method_kwargs):
        with redirect_stdout(_sys_out):
            with redirect_stderr(_sys_err):
                return method(self, *method_args, **method_kwargs)
    return _impl

class LokiQueueHandler(QueueHandler):
    """This handler automatically creates listener and `LokiHandler` to handle logs queue."""

    handler: Union['LokiBatchHandler', 'LokiHandler']

    def __init__(self, queue: Queue, batch_interval: Optional[float] = None, **kwargs):
        """Create new logger handler with the specified queue and kwargs for the `LokiHandler`."""
        super().__init__(queue)

        loki_handler = LokiHandler(**kwargs)  # noqa: WPS110
        self.handler = LokiBatchHandler(batch_interval, target=loki_handler) if batch_interval else loki_handler

        self.listener = QueueListener(self.queue, self.handler)
        self.listener.start()

    def flush(self) -> None:
        super().flush()
        self.handler.flush()

    def __del__(self):
        self.listener.stop()

class LokiHandler(logging.Handler):
    """
    Log handler that sends log records to Loki.

    `Loki API <https://github.com/grafana/loki/blob/master/docs/api.md>`_
    """

    emitter: LokiEmitter

    def __init__(
        self,
        url: str,
        tags: Optional[dict] = None,
        headers: Optional[dict] = None,
        auth: Optional[BasicAuth] = None,
        as_json: Optional[bool] = False,
        props_to_labels: Optional[list[str]] = None,
        level_tag: Optional[str] = const.level_tag,
        logger_tag: Optional[str] = const.logger_tag,
        verify: Union[bool, str] = True
    ):
        """
        Create new Loki logging handler.

        Arguments:
            url: Endpoint used to send log entries to Loki (e.g. `https://my-loki-instance/loki/api/v1/push`).
            tags: Default tags added to every log record.
            auth: Optional tuple with username and password for basic HTTP authentication.
            headers: Optional record with headers that are send with each POST to loki.
            as_json: Flag to support sending entire JSON record instead of only the message.
            props_to_labels: List of properties that should be converted to loki labels.
            level_tag: Label name indicating logging level.
            logger_tag: Label name indicating logger name.
            verify: Either a boolean, in which case it controls whether we verify the server's TLS certificate, or a string, in which case it must be a path to a CA bundle to use.

        """
        super().__init__()
        self.emitter = LokiEmitter(url, tags, headers, auth, as_json, props_to_labels, level_tag, logger_tag, verify)

    def handleError(self, exc: Exception):  # noqa: N802
        """Close emitter and let default handler take actions on error."""
        _error_logger.error(exc, exc_info=True)
        self.emitter.close()

    @with_original_stdout
    def emit(self, record: logging.LogRecord):
        """Send log record to Loki."""
        # noinspection PyBroadException
        try:
            self.emitter(record, self.format(record))
        except Exception as exc:
            self.handleError(exc)

    @with_original_stdout
    def emit_batch(self, records: list[logging.LogRecord]):
        """Send a batch of log records to Loki."""
        # noinspection PyBroadException
        try:
            self.emitter.emit_batch([(record, self.format(record)) for record in records])
        except Exception as exc:
            self.handleError(exc)

class LokiBatchHandler(MemoryHandler):
    interval: float # The interval at which batched logs are sent in seconds
    _last_flush_time: float
    target: LokiHandler

    def __init__(self, interval: float, capacity: int = LOKI_MAX_BATCH_BUFFER_SIZE, **kwargs):
        super().__init__(capacity, **kwargs)
        self.interval = interval
        self._last_flush_time = time.time()

    def flush(self) -> None:
        self.acquire()
        try:
            if self.target and self.buffer:
                self.target.emit_batch(self.buffer)
                self.buffer.clear()
        finally:
            self._last_flush_time = time.time()
            self.release()

    def shouldFlush(self, record: logging.LogRecord) -> bool:
        return (
            super().shouldFlush(record) or 
            (time.time() - self._last_flush_time >= self.interval)
        )