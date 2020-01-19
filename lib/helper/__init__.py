import sys
def is_list_empty(items) -> bool:
    if items is None:
        return True
    if len(items) == 0:
        return True
    first = items[0]
    if len(items) == 1 and (first is None or first == '' or first == 0):
        return True
    return False


def print_std_err(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


LOGLEVEL_DEBUG = 0
LOGLEVEL_INFO = 1
LOGLEVEL_WARN = 2
LOGLEVEL = LOGLEVEL_INFO


def log_message(message: str, level: int) -> None:
    if LOGLEVEL > level:
        return
    if level == LOGLEVEL_DEBUG:
        print_std_err(message)
    else:
        print(message, flush=True)
