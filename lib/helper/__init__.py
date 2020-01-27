import sys
import subprocess
import json
from datetime import datetime
import lib.ceph as ceph
import lib.proxmox as proxmox


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
    message = f'[{datetime.now()}] {message}'
    if level == LOGLEVEL_DEBUG:
        print_std_err(message)
    else:
        print(message, flush=True)


def sizeof_fmt(num: float, suffix: str = 'B') -> str:
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)


def exec_raw(command: str) -> str:
    log_message(f'exec command "{command}"', LOGLEVEL_DEBUG)
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    if process.returncode != 0:
        raise RuntimeError(f'command failed with code: {process.returncode}')
    return str(process.stdout.read().decode("utf-8")).strip("\n")


def exec_parse_json(command: str):
    return json.loads(exec_raw(command), encoding='UTF-8')


def rbd_image_from_proxmox_disk(disk: proxmox.Disk):
    return ceph.RbdImage(disk.ceph_pool, disk.name)


def proxmox_disk_from_rbd_image(disk: ceph.RbdImage):
    return proxmox.Disk(disk.pool, disk.name)
