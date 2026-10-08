import logging


def configure_logging():
    # HTTPX INFO содержит полный URL, включая query-токен webhook.
    for name in ["httpx", "httpcore"]:
        logging.getLogger(name).setLevel(logging.WARNING)
