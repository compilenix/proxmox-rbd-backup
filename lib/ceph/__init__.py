from ..helper import *
from ..helper import Log as log
import re
import time
import random
import subprocess


class Image:
    def __init__(self, pool_name: str, image: str):
        self.pool = pool_name
        self.name = image

    def __str__(self):
        return f'{self.pool}/{self.name}'


class Ceph:
    def __init__(self):
        return

    def get_rbd_images(self, pool: str, command_inject: str = ''):
        return exec_parse_json(f'{command_inject + " " if command_inject else "" }rbd -p {pool} ls --format json')

    def is_rbd_image_existing(self, pool: str, image: str, command_inject: str = ''):
        return image in self.get_rbd_images(pool, command_inject)

    def get_rbd_snapshots(self, pool: str, image: str, command_inject: str = ''):
        """
        :return: [{
            "id": 1234,
            "name": "snapshot_name",
            "size": 1234,  # bytes
            "protected": True or False,
            "timestamp": "Sat Feb 29 00:50:17 2020"
        }]
        """
        if not self.is_rbd_image_existing(pool, image, command_inject=command_inject):
            return []
        return exec_parse_json(f'{command_inject + " " if command_inject else "" }rbd -p {pool} snap ls --format json {image}')

    def get_rbd_snapshot(self, pool: str, image: str, name: str, command_inject: str = ''):
        """
        :return: {
            "id": 1234,
            "name": "snapshot_name",
            "size": 1234,  # bytes
            "protected": True or False,
            "timestamp": "Sat Feb 29 00:50:17 2020"
        }
        """
        snaps = self.get_rbd_snapshots(pool, image, command_inject=command_inject)
        for snap in snaps:
            if snap['name'] == name:
                return snap
        return None

    def get_rbd_snapshots_by_prefix(self, pool: str, image: str, snapshot_prefix: str, command_inject: str = ''):
        log.message('get ceph snapshot count for image ' + image, LOGLEVEL_DEBUG)
        snapshots = []
        for current_snapshot in self.get_rbd_snapshots(pool, image, command_inject=command_inject):
            if current_snapshot['name'].startswith(snapshot_prefix, 0, len(snapshot_prefix)):
                snapshots.append(current_snapshot)
        return snapshots

    def create_rbd_snapshot(self, pool: str, image: str, snapshot_prefix: str = '', new_snapshot_name: str = '', command_inject: str = '') -> str:
        log.message('creating ceph snapshot for image ' + command_inject + pool + '/' + image, LOGLEVEL_INFO)
        if len(new_snapshot_name.strip()) == 0:
            name = snapshot_prefix + ''.join([random.choice('0123456789abcdef') for _ in range(16)])
        else:
            name = new_snapshot_name
        log.message('exec command "' + command_inject + 'rbd -p ' + pool + ' snap create ' + image + '@' + name + '"', LOGLEVEL_DEBUG)
        if command_inject != '':
            code = subprocess.call(command_inject.strip().split(' ') + ['rbd', '-p', pool, 'snap', 'create', image + '@' + name])
        else:
            code = subprocess.call(['rbd', '-p', pool, 'snap', 'create', image + '@' + name])
        if code != 0:
            raise RuntimeError('error creating ceph snapshot code: ' + str(code))
        log.message('ceph snapshot created ' + name, LOGLEVEL_DEBUG)
        return name

    def create_rbd_image(self, pool: str, image: str, size: str = '1', command_inject: str = ''):
        """
        :param size: size-in-M/G/T. Examples: 1, 100M, 20G, 4T
        """
        log.message('creating ceph rbd image ' + command_inject + pool + '/' + image, LOGLEVEL_INFO)
        exec_raw(f'{command_inject + " " if command_inject else "" }' + 'rbd create ' + pool + '/' + image + ' -s ' + size)

    def remove_rbd_snapshot(self, pool: str, image: str, snapshot: str, command_inject: str = ''):
        exec_raw(f'{command_inject + " " if command_inject else "" }' + 'rbd -p ' + pool + ' snap rm ' + image + '@' + snapshot)

    def get_rbd_image_info(self, pool: str, image: str, command_inject: str = ''):
        return exec_parse_json(f'{command_inject + " " if command_inject else "" }' + 'rbd -p ' + pool + ' --format json info ' + image)

    def set_scrubbing(self, enable: bool, command_inject: str = ''):
        action_name = 'enable' if enable else 'disable'
        action = 'set' if enable else 'unset'
        log.message(action_name + ' ceph scrubbing', LOGLEVEL_INFO)
        exec_raw(f'{command_inject + " " if command_inject else "" }' + 'ceph osd ' + action + ' nodeep-scrub')
        exec_raw(f'{command_inject + " " if command_inject else "" }' + 'ceph osd ' + action + ' noscrub')

    def wait_for_cluster_healthy(self, command_inject: str = ''):
        log.message('waiting for ceph cluster to become healthy', LOGLEVEL_INFO)
        while exec_raw(f'{command_inject + " " if command_inject else "" }' + 'ceph health detail').startswith('HEALTH_ERR'):
            time.sleep(10)
            log.message('waiting for ceph cluster to become healthy', LOGLEVEL_DEBUG)

    def wait_for_scrubbing_completion(self, command_inject: str = ''):
        log.message('waiting for ceph cluster to complete scrubbing', LOGLEVEL_INFO)
        pattern = re.compile("scrubbing")
        while pattern.search(exec_raw(f'{command_inject + " " if command_inject else "" }' + 'ceph status')):
            time.sleep(10)
            log.message('waiting for ceph cluster to complete scrubbing', LOGLEVEL_DEBUG)

    def map_rbd_image(self, pool: str, image: str, command_inject: str = ''):
        log.message('mapping ceph image ' + pool + '/' + image, LOGLEVEL_DEBUG)
        exec_raw(f'{command_inject + " " if command_inject else "" }' + 'rbd -p ' + pool + ' device map ' + image)
        mapped_path = ''
        mapped_images_info = self.get_rbd_image_mapped_info()
        for mapped_image in mapped_images_info:
            if mapped_image['name'] == image:
                mapped_path = mapped_image['device']
                break
        if mapped_path == '':
            raise RuntimeError(f'could not find mapped block-device of image {image}')
        del mapped_images_info
        return mapped_path

    def unmap_rbd_image(self, pool: str, image: str, command_inject: str = ''):
        log.message('unmapping ceph image ' + pool + '/' + image, LOGLEVEL_DEBUG)
        return exec_raw(f'{command_inject + " " if command_inject else "" }' + 'rbd -p ' + pool + ' device unmap ' + image)

    def get_rbd_image_mapped_info(self, command_inject: str = ''):
        log.message('get info about mapped rbd images' + (' locally' if command_inject == '' else ' on remote: ' + command_inject.split('@')[1]), LOGLEVEL_DEBUG)
        return exec_parse_json(f'{command_inject + " " if command_inject else "" }' + 'rbd device list --format json')

    def list_rbd_image_meta(self, pool: str, image: str, command_inject: str = ''):
        result = exec_raw(f'{command_inject + " " if command_inject else "" }rbd image-meta list {pool}/{image} --format json')
        if result and len(result) > 2:
            return parse_json(result)
        return None

    def get_rbd_image_meta(self, pool: str, image: str, key: str, command_inject: str = ''):
        return exec_raw(f'{command_inject + " " if command_inject else "" }' + f'rbd image-meta get {pool}/{image} "{key}"')

    def set_rbd_image_meta(self, pool: str, image: str, key: str, value: str, command_inject: str = ''):
        return exec_raw(f'{command_inject + " " if command_inject else "" }' + f'rbd image-meta set {pool}/{image} "{key}" "{value}"')

    def remove_rbd_image_meta(self, pool: str, image: str, key: str, command_inject: str = ''):
        return exec_raw(f'{command_inject + " " if command_inject else "" }' + f'rbd image-meta remove {pool}/{image} "{key}"')

    def remove_rbd_image(self, pool: str, image: str, command_inject: str = ''):
        return exec_raw(f'{command_inject + " " if command_inject else "" }' + f'rbd rm {pool}/{image}')
