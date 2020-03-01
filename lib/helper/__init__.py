import subprocess
import json
import sys
import re
from datetime import datetime, timedelta

REGEX_GUID = r'[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}'
_seconds_per_unit = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400, 'M': 2629746, 'y': 31556952}


class Time:
    _time: datetime

    def __init__(self, s):
        self._time = datetime.strptime(s, '%a %b %d %H:%M:%S %Y')

    def is_older_than(self, age: str):
        return self._time < datetime.now() - timedelta(seconds=convert_to_seconds(age))


def convert_to_seconds(s: str):
    return int(s[:-1]) * _seconds_per_unit[s[-1]]


class ArgumentError(Exception):
    def __init__(self, *args, **kwargs):
        pass


def is_list_empty(items) -> bool:
    if items is None:
        return True
    if len(items) == 0:
        return True
    first = items[0]
    if len(items) == 1 and (first is None or first == '' or first == 0):
        return True
    return False


def is_guid(value: str):
    return re.match(r'^' + REGEX_GUID + r'$', value)


def unique_list(list: []):
    tmp_list = []
    for x in list:
        if x not in tmp_list:
            tmp_list.append(x)
    return tmp_list


LOGLEVEL_DEBUG = 0
LOGLEVEL_INFO = 1
LOGLEVEL_WARN = 2
LOGLEVEL_ERR = 3


def map_loglevel_str(level: int):
    if level == LOGLEVEL_DEBUG:
        return 'DEBUG'
    if level == LOGLEVEL_INFO:
        return ' INFO'
    if level == LOGLEVEL_WARN:
        return ' WARN'
    if level == LOGLEVEL_ERR:
        return 'ERROR'
    return 'UNKNOWN'


def map_loglevel(level: str):
    level = level.upper()
    if level == 'DEBUG':
        return LOGLEVEL_DEBUG
    if level == 'INFO':
        return LOGLEVEL_INFO
    if level == 'WARN':
        return LOGLEVEL_WARN
    if level == 'ERROR':
        return LOGLEVEL_ERR
    return 'UNKNOWN'


class Log:
    _LOGLEVEL = LOGLEVEL_INFO

    @staticmethod
    def set_loglevel(level: int):
        if level not in range(0, 3):
            raise NotImplementedError(f'log level is out of range')
        Log._LOGLEVEL = level

    @staticmethod
    def print_std_err(message: str) -> None:
        print(message, file=sys.stderr, flush=True)

    @staticmethod
    def message(message: str, level: int) -> None:
        if Log._LOGLEVEL > level:
            return
        message = f'[{datetime.now()}] {map_loglevel_str(level)}: {message}'
        if level == LOGLEVEL_DEBUG or level == LOGLEVEL_ERR:
            Log.print_std_err(message)
        else:
            print(message, flush=True)

    @staticmethod
    def debug(message: str):
        Log.message(message, LOGLEVEL_DEBUG)

    @staticmethod
    def info(message: str):
        Log.message(message, LOGLEVEL_INFO)

    @staticmethod
    def warn(message: str):
        Log.message(message, LOGLEVEL_WARN)

    @staticmethod
    def error(message: str):
        Log.message(message, LOGLEVEL_ERR)


def sizeof_fmt(num: float, suffix: str = 'B') -> str:
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)


def exec_raw(command: str) -> str:
    Log.debug(f'exec command \'{command}\'')
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    if process.returncode != 0:
        raise RuntimeError(f'command failed with code: {process.returncode}')
    return str(process.stdout.read().decode("utf-8")).strip("\n")


def exec_parse_json(command: str):
    return json.loads(exec_raw(command), encoding='UTF-8')


def rbd_image_from_proxmox_disk(disk):
    import lib.ceph as ceph
    return ceph.Image(disk.storage.pool, disk.name)


def proxmox_disk_from_rbd_image(disk):
    import lib.proxmox as proxmox
    return proxmox.Disk(disk.pool, disk.name)


class Cacheable:
    cached_since: datetime

    def __init__(self):
        self.reset_cached_since()

    def reset_cached_since(self):
        self.cached_since = datetime.now()
