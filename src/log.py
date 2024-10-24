from pathlib import Path

from loguru import logger


def setup_logger():
    log_path = Path(__file__).parent.parent / "logs"

    logger.add(log_path.joinpath("logs.log"), rotation="50 MB")

    logger.add(
        log_path.joinpath("info.log"),
        filter=lambda record: record["level"].name == "INFO",
        rotation="50 MB",
        compression="zip",
    )

    logger.add(
        log_path.joinpath("errors.log"),
        filter=lambda record: record["level"].name == "ERROR",
        rotation="50 MB",
    )

    logger.add(
        log_path.joinpath("debug.log"),
        filter=lambda record: record["level"].name == "DEBUG",
        rotation="10 MB",
        compression="zip",
    )


setup_logger()
