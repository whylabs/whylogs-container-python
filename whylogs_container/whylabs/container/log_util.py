import logging


def add_message_filter(filter_prefix: str, logger_name: str, log_level: int) -> None:
    class CustomMessageFilter(logging.Filter):
        def filter(self, record):
            # Only filter for the given log level
            if record.levelno != log_level:
                return True
            return not record.getMessage().startswith(filter_prefix)

    logger = logging.getLogger(logger_name)
    custom_filter = CustomMessageFilter()
    logger.addFilter(custom_filter)
