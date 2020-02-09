import configparser
import random
import re
import time
from .ceph import Ceph, Image
from .helper import *
from .helper import Log as log
from .proxmox import Proxmox, Disk, VM
from .filesystem import mount_rbd_metadata_image, unmount_rbd_metadata_image


class Backup:
    _config: configparser.ConfigParser
    _ceph: Ceph
    _servers: [str]
    _remote_connection_command: str
    _proxmox: Proxmox
    _storages_to_ignore: [str]
    _vms_to_ignore: [str]

    def __init__(self, servers, config):
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

    def update_metadata(self, vm: VM, snapshot_name: str):
        rbd_image_vm_metadata_name = vm.uuid + '_vm_metadata'
        log.info(f'save current config into vm metadata image of vm {vm.uuid} (id={vm.id}, name={vm.name})')
        is_vm_metadata_existing = self._ceph.is_rbd_image_existing(self._backup_rbd_pool, rbd_image_vm_metadata_name)
        if is_vm_metadata_existing:
            # map vm metadata image
            mapped_image_path = self._ceph.map_rbd_image(self._backup_rbd_pool, rbd_image_vm_metadata_name)
            # mount vm metadata image
            mount_rbd_metadata_image(rbd_image_vm_metadata_name, mapped_image_path)
        else:
            # create vm metadata image
            log.info('metadata image for vm not existing; creating...')
            self._ceph.create_rbd_image(self._backup_rbd_pool, rbd_image_vm_metadata_name, self._config['global']['vm_metadata_image_size'])
            is_vm_metadata_existing = self._ceph.is_rbd_image_existing(self._backup_rbd_pool, rbd_image_vm_metadata_name)
            if not is_vm_metadata_existing:
                raise RuntimeError(f'ceph metadata image for vm is not existing right after creation, this may be a transient error: {rbd_image_vm_metadata_name}')
            if 'ceph_backup_disable_rbd_image_features_for_metadata' in self._config['global'] and len(self._config['global']['ceph_backup_disable_rbd_image_features_for_metadata']) > 0:
                # disable metadata image features (if needed)
                exec_raw(f'rbd feature disable {rbd_image_vm_metadata_name} {" ".join(self._config["global"]["ceph_backup_disable_rbd_image_features_for_metadata"].replace(" ", "").split(","))}')
            # map metadata image
            mapped_image_path = self._ceph.map_rbd_image(self._backup_rbd_pool, rbd_image_vm_metadata_name)
            # format metadata image
            exec_raw(f'mkfs.ext4 -L {rbd_image_vm_metadata_name[0:16]} {mapped_image_path}')
            # mount metadata image
            mount_rbd_metadata_image(rbd_image_vm_metadata_name, mapped_image_path)
        del is_vm_metadata_existing

        # save current config into metadata image
        log.debug(f'save current config into metadata image -> /tmp/{rbd_image_vm_metadata_name}/{vm.id}.conf')
        with open(f'/tmp/{rbd_image_vm_metadata_name}/{vm.id}.conf', 'w') as config_file:
            print(vm.get_config(), file=config_file)
            # TODO: add hooking-system to call external scripts which may include more metadata export variables using ENV
        del config_file

        unmount_rbd_metadata_image(rbd_image_vm_metadata_name)
        self._ceph.unmap_rbd_image(self._backup_rbd_pool, rbd_image_vm_metadata_name)
        self._ceph.set_rbd_image_meta(self._backup_rbd_pool, rbd_image_vm_metadata_name, 'vm.id', str(vm.id))
        self._ceph.set_rbd_image_meta(self._backup_rbd_pool, rbd_image_vm_metadata_name, 'vm.uuid', str(vm.uuid))
        self._ceph.set_rbd_image_meta(self._backup_rbd_pool, rbd_image_vm_metadata_name, 'vm.name', str(vm.name))
        self._ceph.set_rbd_image_meta(self._backup_rbd_pool, rbd_image_vm_metadata_name, 'vm.running', str(vm.running))
        self._ceph.set_rbd_image_meta(self._backup_rbd_pool, rbd_image_vm_metadata_name, 'last_updated', str(datetime.utcnow()))
        self._ceph.create_rbd_snapshot(self._backup_rbd_pool, rbd_image_vm_metadata_name, new_snapshot_name=snapshot_name)

    def update_vm_ignore_disks(self, vm):
        disks_to_ignore = []
        for section in self._config:
            if section == vm.uuid and 'ignore_disks' in self._config[section] and self._config[section]['ignore_disks']:
                for disk in self._config[section]['ignore_disks'].replace(' ', '').split(','):
                    disk = disk.split('/')
                    disks_to_ignore.append(Disk(disk[0], disk[1]))
        vm.update_rbd_disks(self._proxmox.get_storages(), disks_to_ignore)

    def get_vm_backup_snapshot(self, vm):
        existing_backup_snapshot_count = 0
        existing_backup_snapshot = None
        snapshots = self._proxmox.get_snapshots(vm)
        for vm_state in snapshots:
            if 'name' in vm_state and re.match(self._config['global']['snapshot_name_prefix'] + r'.+', vm_state['name']):
                existing_backup_snapshot = vm_state['name']
                existing_backup_snapshot_count += 1

        return existing_backup_snapshot_count, existing_backup_snapshot

    def wait_for_rbd_image_snapshot_completion(self, vm: VM, image: Image, snapshot_name: str):
        tries = 500
        tries_attempted = tries
        succeed = False
        while not succeed and tries > 0:
            log.debug(f'wait for snapshot creation completion of {vm} -> {image}@{snapshot_name}. {tries} tries left oft {tries_attempted}')
            time.sleep(1)
            tries -= 1
            results = self._ceph.get_rbd_snapshots_by_prefix(image.pool, image.name, self._config['global']['snapshot_name_prefix'], self._remote_connection_command)
            for snap in results:
                if 'name' in snap and snap['name'] == snapshot_name:
                    log.debug(f'snapshot of {vm} -> {image}@{snapshot_name} found')
                    succeed = True
                    break
        if not succeed:
            raise RuntimeError(f'waiting for ceph rbd snapshot creation completion of {vm} -> {image} tined out after {tries_attempted} tries')
        return succeed

    def is_image_snapshot_existing(self, vm: VM, image: Image, snapshot_name: str):
        log.debug(f'check if image and snapshot does exist on backup cluster for {vm} -> {vm.uuid}-{image.pool}-{image.name}@{snapshot_name}')
        results = self._ceph.get_rbd_snapshots_by_prefix(self._backup_rbd_pool, f'{vm.uuid}-{image.pool}-{image.name}', self._config['global']['snapshot_name_prefix'])
        succeed = False
        for snap in results:
            if 'name' in snap and snap['name'] == snapshot_name:
                log.debug(f'snapshot {self._backup_rbd_pool}/{vm.uuid}-{image.pool}-{image.name}@{snapshot_name} found')
                succeed = True
        if not succeed:
            raise RuntimeError('image and snapshot does exist on backup cluster')
        return succeed

    def backup_vm_disk(self, vm: VM,  disk: Disk, snapshot_name: str, is_backup_mode_incremental: bool, existing_backup_snapshot: str = None):
        image = rbd_image_from_proxmox_disk(disk)
        self.wait_for_rbd_image_snapshot_completion(vm, image, snapshot_name)

        if is_backup_mode_incremental:
            log.info(f'incremental backup, starting for {vm} -> {image}')
            whole_object_command = ''
            if 'enable_intra_object_delta_transfer' in self._config['global'] and not self._config['global'].getboolean('enable_intra_object_delta_transfer'):
                whole_object_command = '--whole-object'
            exec_raw(f'/bin/bash -c set -o pipefail; {self._remote_connection_command} "rbd export-diff --no-progress {whole_object_command} --from-snap {existing_backup_snapshot} {image}@{snapshot_name} -" | pv --rate --bytes --timer | rbd import-diff --no-progress - {self._backup_rbd_pool}/{vm.uuid}-{image.pool}-{image.name}')
            log.info(f'incremental backup of {vm} -> {image} complete')
        else:
            log.info(f'initial backup, starting full copy of {vm} -> {image}')
            image_size = exec_parse_json(f'{self._remote_connection_command} rbd info {image} --format json')['size']
            exec_raw(f'/bin/bash -c set -o pipefail; {self._remote_connection_command} "rbd export --no-progress {image}@{snapshot_name} -" | pv --rate --bytes --progress --timer --eta --size {image_size} | rbd import --no-progress - {self._backup_rbd_pool}/{vm.uuid}-{image.pool}-{image.name}')
            self._ceph.create_rbd_snapshot(self._backup_rbd_pool, f'{vm.uuid}-{image.pool}-{image.name}', new_snapshot_name=snapshot_name)
            log.info(f'initial backup of {vm} -> {image} complete')

        return self.is_image_snapshot_existing(vm, image, snapshot_name)

    def run_backup(self, vms: [VM] = None):
        tmp_vms = vms if not is_list_empty(vms) else self._proxmox.get_vms()

        for vm in tmp_vms:
            snapshot_name = self._config['global']['snapshot_name_prefix'] + ''.join([random.choice('0123456789abcdef') for _ in range(16)])

            self.update_metadata(vm, snapshot_name)
            self.update_vm_ignore_disks(vm)

            existing_backup_snapshot_count, existing_backup_snapshot = self.get_vm_backup_snapshot(vm)
            if existing_backup_snapshot_count > 1:
                # TODO: implement
                log.warn(f'latest backup is incomplete, re-processing')
                raise RuntimeError(f'last backup is incomplete, re-processing. automatic repair not implemented, yet. Manual fixing required')
            is_backup_mode_incremental = None
            if existing_backup_snapshot_count == 0:
                is_backup_mode_incremental = False
            if existing_backup_snapshot_count == 1:
                is_backup_mode_incremental = True

            self._proxmox.create_vm_snapshot(vm, snapshot_name)
            for disk in vm.get_rbd_disks():
                self.backup_vm_disk(vm, disk, snapshot_name, is_backup_mode_incremental, existing_backup_snapshot)
            if is_backup_mode_incremental:
                self._proxmox.delete_vm_snapshot(vm, existing_backup_snapshot)

    def list_vms(self):
        tmp_vms = []
        images = self._ceph.get_rbd_images(self._backup_rbd_pool)
        for image in images:
            if not re.match(r'^[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}_vm_metadata$', image): # https://www.regextester.com/94410
                continue
            image_metas = self._ceph.list_rbd_image_meta(self._backup_rbd_pool, image)
            tmp_vms.append(image_metas)
        tmp_vms = sorted(tmp_vms, key=lambda x: x['vm.id'])
        return tmp_vms
