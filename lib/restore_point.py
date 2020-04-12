import configparser
import re

from lib.ceph import Ceph
from .helper import Log as log, Time
from lib.helper import is_list_empty, ArgumentError
from lib.proxmox import Proxmox
from datetime import datetime


class RestorePoint:
    _config: configparser.ConfigParser
    _ceph: Ceph
    _servers: [str]
    _remote_connection_command: str
    _proxmox: Proxmox
    _storages_to_ignore: [str]
    _vms_to_ignore: [str]

    def __init__(self, servers: [str], config: configparser.ConfigParser):
        if is_list_empty(servers):
            raise ArgumentError('servers must be a list with at least one non-empty element')
        if config is None:
            raise ArgumentError('config must not be None')
        self._servers = servers
        self._config = config
        self._ceph = Ceph()
        self._proxmox = None
        self._backup_rbd_pool = self._config['global']['ceph_backup_pool']
        self._remote_connection_command = f'ssh {config["global"]["proxmox_ssh_user"]}@{servers[0]} -T -o Compression=no -x'
        self._storages_to_ignore = []
        self._vms_to_ignore = []
        if 'ignore_storages' in config['global']:
            for item in config['global']['ignore_storages'].replace(' ', '').split(','):
                self._storages_to_ignore.append(item)
        for section in config:
            if 'ignore' in config[section] and config[section]['ignore']:
                self._vms_to_ignore.append(section)

    def init_proxmox(self):
        if self._proxmox:
            return
        self._proxmox = Proxmox(self._servers, username=self._config['global']['user'], password=self._config['global']['password'], verify_ssl=self._config['global'].getboolean('verify_ssl'))
        self._proxmox.update_nodes()
        self._proxmox.update_storages(self._storages_to_ignore)
        self._proxmox.update_vms(self._vms_to_ignore)

    def get_restore_points(self, vm_uuid: str):
        """
        :return: [
            {
                image: pool/image_name
                name: restore_point_name
                timestamp: datetime
            }
        ]
        """
        image = f'{vm_uuid}_vm_metadata'
        tmp_points = []

        snapshots = self._ceph.get_rbd_snapshots(self._backup_rbd_pool, image)
        for snapshot in snapshots:
            tmp_points.append({
                'image': f'{self._backup_rbd_pool}/{image}',
                "name": snapshot['name'],
                "timestamp": snapshot['timestamp']
            })
        tmp_points = sorted(tmp_points, key=lambda x: datetime.strptime(x['timestamp'], '%a %b %d %H:%M:%S %Y'))
        return tmp_points

    def get_restore_point_detail(self, vm_uuid: str, restore_point: str, backup=None):
        """
        :return: {
            'has_proxmox_snapshot': False or True
            'timestamp': '%a %b %d %H:%M:%S %Y',
            'images': [
                {
                    'image': 'rbd/image_name',
                    'name': 'snapshot_name'
                }
            ]
        }
        """
        images = self._ceph.get_rbd_images(self._backup_rbd_pool)
        tmp_images = []
        result = {
            'has_proxmox_snapshot': False,
            'images': tmp_images,
            'timestamp': self._ceph.get_rbd_snapshot(self._backup_rbd_pool, f'{vm_uuid}_vm_metadata', restore_point)['timestamp']
        }

        for image in images:
            if vm_uuid and vm_uuid not in image:
                continue
            points = self._ceph.get_rbd_snapshots(self._backup_rbd_pool, image)
            for point in points:
                if restore_point != point['name']:
                    continue
                tmp_images.append({
                    'image': f'{self._backup_rbd_pool}/{image}',
                    "name": point['name']
                })
        if backup:
            result['has_proxmox_snapshot'] = backup.is_vm_snapshot_existing(backup.get_vm(vm_uuid), restore_point)
        return result

    def remove_restore_point(self, vm_uuid: str = None, restore_point: str = None, age: str = None, match: str = None, backup=None):
        if not vm_uuid and not restore_point and not age and not match:
            raise ArgumentError('at least one parameter must be set; vm_uuid, restore_point, age or match')
        if vm_uuid and not (restore_point or age or match):
            raise ArgumentError('if vm_uuid is set, restore_point, age or match must be set')

        points_to_remove = []
        images = self._ceph.get_rbd_images(self._backup_rbd_pool)

        for image in images:
            if vm_uuid and vm_uuid not in image:
                continue
            points = self._ceph.get_rbd_snapshots(self._backup_rbd_pool, image)
            for point in points:
                if restore_point and restore_point != point['name']:
                    continue
                if age and not Time(point['timestamp']).is_older_than(age):
                    continue
                if match and not re.match(match, point['name']):
                    continue
                points_to_remove.append({
                    'image': image,
                    'restore_point': point['name']
                })

        for point in points_to_remove:
            log.info(f'remove {point["restore_point"]} from image {self._backup_rbd_pool}/{point["image"]}')
            self._ceph.remove_rbd_snapshot(self._backup_rbd_pool, point["image"], point["restore_point"])
            if backup and vm_uuid:
                backup.remove_vm_snapshot(backup.get_vm(vm_uuid), point['restore_point'])

    def remove_backup(self, vm_uuid: str):
        images = self._ceph.get_rbd_images(self._backup_rbd_pool)
        for image in images:
            if vm_uuid and vm_uuid not in image:
                continue
            self._ceph.remove_rbd_image(self._backup_rbd_pool, image)
