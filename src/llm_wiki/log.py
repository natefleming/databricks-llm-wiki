"""Logging configuration using loguru.

All output goes to stderr. No log files are created.

Usage:
    from llm_wiki.log import logger

    logger.info("Processing page", page_id="kubernetes-scheduling")
    logger.error("Compilation failed", error=str(e))
"""

import sys

from loguru import logger

# Remove default handler and configure stderr-only output
logger.remove()
logger.add(
    sys.stderr,
    format=(
        "<level>{level: <8}</level> | "
        "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
        "<level>{message}</level>"
    ),
    level="INFO",
    colorize=True,
)

__all__ = ["logger"]
