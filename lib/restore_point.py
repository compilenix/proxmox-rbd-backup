import configparser

from lib.ceph import Ceph
from .helper import Log as log
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

    def list_restore_points(self, vm_uuid: str):
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

    def remove_restore_point(self, vm_uuid: str, restore_point: str):
        images = self._ceph.get_rbd_images(self._backup_rbd_pool)
        for image in images:
            if vm_uuid not in image:
                continue
            points = self._ceph.get_rbd_snapshots(self._backup_rbd_pool, image)
            for point in points:
                if point['name'] == restore_point:
                    log.info(f'remove {restore_point} from image {self._backup_rbd_pool}/{image}')
                    self._ceph.remove_rbd_snapshot(self._backup_rbd_pool, image, restore_point)
                    break

