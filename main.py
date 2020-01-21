# TODO: backup rbd disk's of vm
# TODO: remove old vm snapshots
# TODO: list vm's in backup
# TODO: list backups of given vm
# TODO: perform restore of given backup
# TODO: write all logged messages into buffer to be able to provide detailed context on exceptions
# TODO: how to handle proxmox snapshots (especially those including RAM)
# TODO: migrate from str.format() to f'' (f-strings)

from proxmoxer import ProxmoxAPI, logging
import configparser
import os.path
from lib.helper import *
import lib.helper
import re
import lib.ceph as ceph
from typing import List
import random
import time

if not os.path.isfile('config/global.ini'):
    raise FileNotFoundError('config/global.ini')

config = configparser.ConfigParser()
config.read('config/global.ini')
servers = config['global']['proxmox_servers'].replace(' ', '').split(',')
remote_connection_command = 'ssh ' + config['global']['ceph_backup_cluster_ssh_host'] + ' -T -o Compression=no -x '

if is_list_empty(servers):
    raise RuntimeError('no servers found in config')

LOGLEVEL = LOGLEVEL_DEBUG
lib.helper.LOGLEVEL = LOGLEVEL
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
proxmox = ProxmoxAPI(servers[0], user=config['global']['user'], password=config['global']['password'], verify_ssl=config['global'].getboolean('verify_ssl'))

nodes = proxmox.nodes.get()
nodes = sorted(nodes, key=lambda x: x['node'])
vms = []
storages_rbd = []
storages_to_ignore = []
tmp_storages = proxmox.storage.get(type='rbd')

if 'ignore_storages' in config['global']:
    for item in config['global']['ignore_storages'].replace(' ', '').split(','):
        storages_to_ignore.append(item)

for item in tmp_storages:
    if item['storage'] in storages_to_ignore:
        log_message('ignore proxmox storage {0}'.format(item['storage']), LOGLEVEL_DEBUG)
        continue
    if 'images' in item['content']:
        storages_rbd.append({
            'name': item['storage'],
            'pool': item['pool']
        })
del tmp_storages


def is_rbd_disk(name: str) -> object or None:
    for storage_rbd in storages_rbd:
        if name.startswith(storage_rbd['name'] + ':vm-'):
            return storage_rbd
    return None


class CephRbdImage:
    def __init__(self, pool_name: str, image: str):
        self.pool = pool_name
        self.name = image

    def __str__(self):
        return f'{self.pool}:{self.name}'


class VM:
    def __init__(self):
        self.id = 0
        self.uuid = ''
        self.name = ''
        self.node = ''
        self.config = ''
        self.rbd_disks = []  # type: List[CephRbdImage]

    def __str__(self):
        return f'{self.name} (id={self.id}, uuid={self.uuid})'


log_message('get vm\'s...', LOGLEVEL_INFO)
tmp_vms = []
for node in nodes:
    log_message('get vm\'s from node {0}'.format(node['node']), LOGLEVEL_INFO)
    tmp_vms = proxmox.nodes(node['node']).qemu.get()
    tmp_vms = sorted(tmp_vms, key=lambda x: x['vmid'])
    for vm in tmp_vms:
        vm_item = VM()
        vm_item.id = vm['vmid']
        vm_item.node = node['node']
        vm_item.name = vm['name']
        vm_item.status = vm['status']
        vm_item.rbd_disks = []
        vm_item.config = ''
        vm_item.uuid = ''

        log_message('found vm {0} with name {1}'.format(vm_item.id, vm_item.name), LOGLEVEL_DEBUG)

        # check if this vm should be excluded according to config
        if vm_item.id in config and 'ignore' in config[vm_item.id] and config[vm_item.id].getboolean('ignore'):
            log_message('ignore vm as requested by config [{0} (name={1})] -> \"ignore\": {2}'.format(vm_item.id, vm_item.name, config[vm_item.id].getboolean('ignore')), LOGLEVEL_DEBUG)
            continue

        # format and add vm config
        cfg = proxmox.nodes(vm_item.node).qemu(vm_item.id).get('pending')
        cfg = sorted(cfg, key=lambda x: x['key'])
        description = ''
        for item in cfg:
            if item['key'] == 'digest':
                continue
            if item['key'] == 'smbios1':
                # extract vm uuid from smbios1
                tmp_smbios1 = item['value'].split(',')
                found_uuid = False
                for smbios_part in tmp_smbios1:
                    if smbios_part.startswith('uuid='):
                        tmp_smbios1 = smbios_part[5::]
                        found_uuid = True
                        break
                if not found_uuid:
                    raise RuntimeError('could not find uuid of vm {0} in config property \"smbios1\"'.format(vm_item.id))
                vm_item.uuid = tmp_smbios1
                del tmp_smbios1
                del found_uuid
                del smbios_part
            # extract rbd disks from config (if not ignored via config)
            if re.match(r'scsi\d|sata\d|ide\d|virtio\d|efidisk\d', item['key']) is not None:
                storage = is_rbd_disk(item['value'])
                if storage is not None:
                    disk_name = item['value'].split(',')[0]
                    log_message('found proxmox vm disk: ' + disk_name, LOGLEVEL_DEBUG)
                    # can't check if disk should be ignored because vm uuid may not be preset, yet
                    pool = disk_name.replace(storage['name'], storage['pool']).split(':')[0]
                    image_name = disk_name.split(':')[1]
                    vm_item.rbd_disks.append(CephRbdImage(pool, image_name))
                    del disk_name
                    del pool
                    del image_name
                del storage
            if item['key'] == 'description':
                for description_line in item["value"].split('\n'):
                    description += f'#{description_line}\n'
                del description_line
            else:
                vm_item.config += f'{item["key"]}: {item["value"]}\n'
        del cfg
        vm_item.config = f'{description}{vm_item.config}'

        for disk in vm_item.rbd_disks:
            disks_to_ignore = []  # type: List[str]
            if vm_item.uuid in config and 'ignore_disks' in config[vm_item.uuid]:
                disks_to_ignore = config[vm_item.uuid]['ignore_disks'].replace(' ', '').split(',')
            if disk in disks_to_ignore:
                log_message('ignore rbd image: {0} as requested by config [{1} (name={2}, number={3})] -> \"ignore_disks\": {4}'.format(disk, vm_item.uuid, vm_item.name, vm_item.id, ', '.join(disks_to_ignore)), LOGLEVEL_DEBUG)
                vm_item.rbd_disks.remove(disk)
            del disks_to_ignore
        del disk

        vms.append(vm_item)
        del vm_item
    log_message('found {0} vm\'s on {1}'.format(len(tmp_vms), node['node']), LOGLEVEL_INFO)
log_message('found a total of {0} vm\'s on {1} nodes'.format(len(vms), len(nodes)), LOGLEVEL_INFO)
del vm
del item
del node
del tmp_vms


def map_rbd_image(pool_name: str, image: str):
    ceph.map_rbd_image(pool_name, image)
    mapped_path = ''
    mapped_images_info = ceph.get_rbd_image_mapped_info()
    for mapped_image in mapped_images_info:
        if mapped_image['name'] == rbd_image_vm_metadata_name:
            mapped_path = mapped_image['device']
            break
    if mapped_path == '':
        raise RuntimeError(f'could not find mapped block-device of image {rbd_image_vm_metadata_name}')
    del mapped_images_info
    return mapped_path


def mount_rbd_metadata_image(image: str, mapped_device_path: str):
    log_message('mount vm metadata filesystem: {0}'.format(image), LOGLEVEL_DEBUG)
    exec_raw('mkdir -p /tmp/{0}'.format(image))
    exec_raw('mount {0} /tmp/{1}'.format(mapped_device_path, image))


def create_vm_snapshot(vm_object: VM):
    log_message(f'create vm snapshot via proxmox api for {vm_object}', LOGLEVEL_INFO)
    results = proxmox.nodes(vm_object.node).qemu(vm_object.id).post('snapshot', snapname=snapshot_name, vmstate=0, description='!!!DO NOT REMOVE!!!automated snapshot by proxmox-rbd-backup. !!!DO NOT REMOVE!!!')
    if 'UPID' not in results:
        raise RuntimeError(f'unexpected result while creating proxmox vm snapshot of {vm_object} result: {results}')
    del results

    tries = 60
    tries_attempted = tries
    succeed = False
    while not succeed and tries > 0:
        tries -= 1
        results = proxmox.nodes(vm_object.node).qemu(vm_object.id).get('snapshot')
        for vm_state in results:
            if 'name' in vm_state and vm_state['name'] == snapshot_name:
                succeed = True
                break
    del tries, results, vm_state

    if not succeed:
        raise RuntimeError(f'proxmox vm snapshot creation of {vm_object} tined out after {tries_attempted} seconds')
    log_message(f'snapshot creation for {vm_object} was successful', LOGLEVEL_DEBUG)


for vm in vms:
    if is_list_empty(vm.rbd_disks):
        log_message('ignore vm {0} (name={1}, number={2}), because it has no rbd disk to backup'.format(vm.uuid, vm.name, vm.id), LOGLEVEL_DEBUG)
        continue

    snapshot_name = config['global']['snapshot_name_prefix'] + ''.join([random.choice('0123456789abcdef') for _ in range(16)])
    rbd_image_vm_metadata_name = vm.uuid + '_vm_metadata'
    log_message(f'save current config into vm metadata image of vm {vm.uuid} (id={vm.id}, name={vm.name})', LOGLEVEL_INFO)
    backup_rbd_pool = config['global']['ceph_backup_pool']
    is_vm_metadata_existing = ceph.is_rbd_image_existing(backup_rbd_pool, rbd_image_vm_metadata_name)
    mapped_image_path = ''
    if is_vm_metadata_existing:
        # map vm metadata image
        mapped_image_path = map_rbd_image(backup_rbd_pool, rbd_image_vm_metadata_name)
        # mount vm metadata image
        mount_rbd_metadata_image(rbd_image_vm_metadata_name, mapped_image_path)
    else:
        # create vm metadata image
        log_message('metadata image for vm not existing; creating...', LOGLEVEL_INFO)
        ceph.create_rbd_image(backup_rbd_pool, rbd_image_vm_metadata_name, config['global']['vm_metadata_image_size'])
        is_vm_metadata_existing = ceph.is_rbd_image_existing(backup_rbd_pool, rbd_image_vm_metadata_name)
        if not is_vm_metadata_existing:
            raise RuntimeError('ceph metadata image for vm is not existing right after creation, this may be a transient error: {0}'.format(rbd_image_vm_metadata_name))
        if 'ceph_backup_disable_rbd_image_features_for_metadata' in config['global'] and len(config['global']['ceph_backup_disable_rbd_image_features_for_metadata']) > 0:
            # disable metadata image features (if needed)
            exec_raw('rbd feature disable {0} {1}'.format(rbd_image_vm_metadata_name, ' '.join(config['global']['ceph_backup_disable_rbd_image_features_for_metadata'].replace(' ', '').split(','))))
        # map metadata image
        mapped_image_path = map_rbd_image(backup_rbd_pool, rbd_image_vm_metadata_name)
        # format metadata image
        exec_raw(f'mkfs.ext4 -L {rbd_image_vm_metadata_name[0:16]} {mapped_image_path}')
        # mount metadata image
        mount_rbd_metadata_image(rbd_image_vm_metadata_name, mapped_image_path)
    del is_vm_metadata_existing

    # save current config into metadata image
    log_message(f'save current config into metadata image -> /tmp/{rbd_image_vm_metadata_name}/{vm.id}.conf', LOGLEVEL_DEBUG)
    with open(f'/tmp/{rbd_image_vm_metadata_name}/{vm.id}.conf', 'w') as config_file:
        print(vm.config, file=config_file)
        # TODO: add hocking-system to call external scripts which may include more metadata
        #       export variables using ENV
    del config_file

    # unmount vm metadata image
    exec_raw(f'umount /tmp/{rbd_image_vm_metadata_name}')
    exec_raw(f'rmdir /tmp/{rbd_image_vm_metadata_name}')

    # unmap rbd image
    ceph.unmap_rbd_image(backup_rbd_pool, rbd_image_vm_metadata_name)

    # create snapshot of vm metadata image
    ceph.create_rbd_snapshot(backup_rbd_pool, rbd_image_vm_metadata_name, new_snapshot_name=snapshot_name)
    del rbd_image_vm_metadata_name

    existing_backup_snapshots = 0
    existing_backup_snapshot = ''
    result = proxmox.nodes(vm.node).qemu(vm.id).get('snapshot')
    for state in result:
        if 'name' in state and re.match(config['global']['snapshot_name_prefix'] + r'.+', state['name']):
            existing_backup_snapshot = state['name']
            existing_backup_snapshots += 1
    del result, state
    if existing_backup_snapshots > 1:
        # TODO: implement
        log_message(f'last backup is incomplete, re-processing', LOGLEVEL_WARN)
        raise RuntimeError(f'last backup is incomplete, re-processing. automatic repair not implemented, yet. Manual fixing required')

    # create vm snapshot via proxmox api
    create_vm_snapshot(vm)

    break
