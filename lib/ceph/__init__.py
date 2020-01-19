import lib.helper as helper
import re
import time
import random
import subprocess


def get_rbd_images(pool: str, command_inject: str = ''):
    return helper.exec_parse_json(command_inject + 'rbd -p ' + pool + ' ls --format json')


def is_rbd_image_existing(pool: str, image: str, command_inject: str = ''):
    return image in get_rbd_images(pool, command_inject)


def get_rbd_snapshots(pool: str, image: str, command_inject: str = ''):
    return helper.exec_parse_json(command_inject + 'rbd -p ' + pool + ' snap ls --format json ' + image)


def get_rbd_snapshots_by_prefix(pool: str, image: str, snapshot_prefix: str, command_inject: str = ''):
    helper.log_message('get ceph snapshot count for image ' + image, helper.LOGLEVEL_INFO)
    snapshots = []
    for current_snapshot in get_rbd_snapshots(pool, image, command_inject):
        if current_snapshot['name'].startswith(snapshot_prefix, 0, len(snapshot_prefix)):
            snapshots.append(current_snapshot)
    return snapshots


def create_rbd_snapshot(pool: str, image: str, snapshot_prefix: str = '', new_snapshot_name: str = '', command_inject: str = '') -> str:
    helper.log_message('creating ceph snapshot for image ' + command_inject + pool + '/' + image, helper.LOGLEVEL_INFO)
    if len(new_snapshot_name.strip()) == 0:
        name = snapshot_prefix + ''.join([random.choice('0123456789abcdef') for _ in range(16)])
    else:
        name = new_snapshot_name
    helper.log_message('exec command "' + command_inject + 'rbd -p ' + pool + ' snap create ' + image + '@' + name + '"', helper.LOGLEVEL_INFO)
    if command_inject != '':
        code = subprocess.call(command_inject.strip().split(' ') + ['rbd', '-p', pool, 'snap', 'create', image + '@' + name])
    else:
        code = subprocess.call(['rbd', '-p', pool, 'snap', 'create', image + '@' + name])
    if code != 0:
        raise RuntimeError('error creating ceph snapshot code: ' + str(code))
    helper.log_message('ceph snapshot created ' + name, helper.LOGLEVEL_INFO)
    return name


def create_rbd_image(pool: str, image: str, size: str = '1', command_inject: str = ''):
    """
    :param pool:
    :param image:
    :param size: size-in-M/G/T. Examples: 1, 100M, 20G, 4T
    :param command_inject:
    """
    helper.log_message('creating ceph rbd image ' + command_inject + pool + '/' + image, helper.LOGLEVEL_INFO)
    helper.exec_raw(command_inject + 'rbd create ' + pool + '/' + image + ' -s ' + size)


def remove_rbd_snapshot(pool: str, image: str, snapshot: str, command_inject: str = ''):
    helper.exec_raw(command_inject + 'rbd -p ' + pool + ' snap rm ' + image + '@' + snapshot)


def get_rbd_image_info(pool: str, image: str, command_inject: str = ''):
    return helper.exec_parse_json(command_inject + 'rbd -p ' + pool + ' --format json info ' + image)


def set_scrubbing(enable: bool, command_inject: str = ''):
    action_name = 'enable' if enable else 'disable'
    action = 'set' if enable else 'unset'
    helper.log_message(action_name + ' ceph scrubbing', helper.LOGLEVEL_INFO)
    helper.exec_raw(command_inject + 'ceph osd ' + action + ' nodeep-scrub')
    helper.exec_raw(command_inject + 'ceph osd ' + action + ' noscrub')


def wait_for_cluster_healthy(command_inject: str = ''):
    helper.log_message('waiting for ceph cluster to become healthy', helper.LOGLEVEL_INFO)
    while helper.exec_raw(command_inject + 'ceph health detail').startswith('HEALTH_ERR'):
        time.sleep(10)
        helper.log_message('waiting for ceph cluster to become healthy', helper.LOGLEVEL_INFO)


def wait_for_scrubbing_completion(command_inject: str = ''):
    helper.log_message('waiting for ceph cluster to complete scrubbing', helper.LOGLEVEL_INFO)
    pattern = re.compile("scrubbing")
    while pattern.search(helper.exec_raw(command_inject + 'ceph status')):
        time.sleep(10)
        helper.log_message('waiting for ceph cluster to complete scrubbing', helper.LOGLEVEL_INFO)


def map_rbd_image(pool: str, image: str, command_inject: str = ''):
    helper.log_message('mapping ceph image ' + pool + '/' + image, helper.LOGLEVEL_INFO)
    return helper.exec_raw(command_inject + 'rbd -p ' + pool + ' device map ' + image)


def unmap_rbd_image(pool: str, image: str, command_inject: str = ''):
    helper.log_message('unmapping ceph image ' + pool + '/' + image, helper.LOGLEVEL_INFO)
    return helper.exec_raw(command_inject + 'rbd -p ' + pool + ' device unmap ' + image)


def get_rbd_image_mapped_info(command_inject: str = ''):
    helper.log_message('get info about mapped rbd images' + (' locally' if command_inject == '' else ' on remote: ' + command_inject.split('@')[1]), helper.LOGLEVEL_INFO)
    return helper.exec_parse_json(command_inject + 'rbd device list --format json')
