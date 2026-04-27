"""Thin wrapper around :func:`wxyc_etl.logger.init_logger`.

The ``wxyc_etl.logger`` module landed in WXYC/wxyc-etl#50 and ships from
``main`` of the wxyc-etl repo. Older wxyc-etl refs (currently pinned in our
CI install step) do not include it. This shim lets every entrypoint call
:func:`init_logger` unconditionally; once the CI install ref is bumped to a
wxyc-etl revision that contains #50, real Sentry + JSON logging is wired up
without any further consumer-side change.

When the underlying module isn't installed we fall back to a minimal
``logging.basicConfig`` so the scripts still log something useful in the
interim.
"""

from __future__ import annotations

import logging
from typing import Any


def init_logger(
    repo: str,
    tool: str,
    sentry_dsn: str | None = None,
    run_id: str | None = None,
) -> Any:
    """Initialize Sentry + JSON logging via wxyc_etl.logger when available.

    Falls back to a basic stderr formatter when ``wxyc_etl.logger`` is not
    installed. Returns whatever ``wxyc_etl.logger.init_logger`` returns, or
    ``None`` if the fallback path is taken.
    """
    try:
        from wxyc_etl.logger import init_logger as _wxyc_init
    except ImportError:
        if not logging.getLogger().handlers:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(levelname)s - %(message)s",
            )
        return None

    return _wxyc_init(
        repo=repo,
        tool=tool,
        sentry_dsn=sentry_dsn,
        run_id=run_id,
    )


__all__ = ["init_logger"]
