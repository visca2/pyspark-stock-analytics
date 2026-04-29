"""Generic websocket callback functions."""


def on_error(ws, error):
    """Log error message coming from a websocket."""
    print(error)


def on_close(ws, close_status_code, close_msg):
    """Log information regarding the closure of a websocket."""
    print(f"closed: code={close_status_code}, message={close_msg}")
