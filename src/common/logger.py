"""Logger estruturado com loguru para toda a plataforma."""

import sys
from typing import Any

from loguru import logger

from src.common.config import settings


def configure_logger(
    level: str | None = None,
    fmt: str | None = None,
    context: dict[str, Any] | None = None,
) -> None:
    """Configura o logger global da aplicação.

    Args:
        level: Nível de log (DEBUG, INFO, WARNING, ERROR). Usa settings se None.
        fmt: Formato de saída ('json' ou 'text'). Usa settings se None.
        context: Campos extras para adicionar a todos os logs.
    """
    log_level = level or settings.log_level
    log_format = fmt or settings.log_format

    logger.remove()

    if log_format == "json":
        log_fmt = (
            '{{"time": "{time:YYYY-MM-DDTHH:mm:ss.SSSZ}", '
            '"level": "{level}", '
            '"name": "{name}", '
            '"message": "{message}", '
            '"extra": {extra}}}'
        )
    else:
        log_fmt = (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
            "<level>{message}</level>"
        )

    logger.add(
        sys.stdout,
        format=log_fmt,
        level=log_level,
        colorize=(log_format != "json"),
        serialize=(log_format == "json"),
        backtrace=True,
        diagnose=(settings.environment == "local"),
    )

    if context:
        logger.configure(extra=context)


def get_logger(name: str, **context: Any):
    """Retorna um logger contextualizado.

    Args:
        name: Nome do módulo/componente.
        **context: Campos extras para este logger.

    Returns:
        Logger configurado com contexto.
    """
    return logger.bind(component=name, **context)


configure_logger()

__all__ = ["logger", "get_logger", "configure_logger"]
