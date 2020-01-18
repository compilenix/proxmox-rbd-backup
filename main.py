# TODO: create and mount metadata of vm in backup cluster
# TODO: copy current vm config to metadata
# TODO: unmount metadata
# TODO: create snapshot of metadata
# TODO: create vm snapshot
# TODO: backup rbd disk's of vm
# TODO: remove old vm snapshot
# TODO: how to handle proxmox snapshots (especially those including RAM)

from proxmoxer import ProxmoxAPI, logging
import configparser
import os.path
import lib.helper
import re

if not os.path.isfile('config/global.ini'):
    raise FileNotFoundError('config/global.ini')

config = configparser.ConfigParser()
config.read('config/global.ini')

servers = config['global']['proxmox_servers'].replace(' ', '').split(',')

if lib.helper.is_list_empty(servers):
    raise RuntimeError('no servers found in config')

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
proxmox = ProxmoxAPI(servers[0], user=config['global']['user'], password=config['global']['password'], verify_ssl=config['global'].getboolean('verify_ssl'))

nodes = proxmox.nodes.get()
vms = []
storages_rbd = []
storages_to_ignore = []
tmp_storages = proxmox.storage.get(type='rbd')

for item in config['global']['ignore_storages'].replace(' ', '').split(','):
    storages_to_ignore.append(item)

for item in tmp_storages:
    if item['storage'] in storages_to_ignore:
        continue
    if 'images' in item['content']:
        storages_rbd.append({
            'name': item['storage'],
            'pool': item['pool']
        })
del tmp_storages


def is_rbd_disk(disk: str) -> object or None:
    for storage_rbd in storages_rbd:
        if disk.startswith(storage_rbd['name'] + ':vm-'):
            return storage_rbd
    return None


class VM:
    def __init__(self):
        self.number = 0
        self.id = ''
        self.name = ''
        self.node = ''
        self.config = ''
        self.rbd_disks = []


tmp_vms = []
for node in nodes:
    tmp_vms = proxmox.nodes(node['node']).qemu.get()
    for vm in tmp_vms:
        vm_item = VM()
        vm_item.number = vm['vmid']
        vm_item.node = node['node']
        vm_item.name = vm['name']
        vm_item.status = vm['status']

        # check if this vm should be excluded according to config
        if vm_item.number in config and 'ignore' in config[vm_item.number] and config[vm_item.number].getboolean('ignore'):
            continue

        # format and add vm config
        tmp_id = ''
        cfg = proxmox.nodes(vm_item.node).qemu(vm_item.number).get('pending')
        cfg = sorted(cfg, key=lambda x: x['key'])
        for item in cfg:
            if item['key'] == 'digest':
                continue
            if item['key'] == 'smbios1':
                tmp_id = item['value']
            # extract rbd disks from config (if not ignored via config)
            if re.match(r'scsi\d|sata\d|ide\d|virtio\d|efidisk\d', item['key']) is not None:
                storage = is_rbd_disk(item['value'])
                if storage is not None:
                    disk_name = item['value'].split(',')[0]
                    disks_to_ignore = []
                    if vm_item.number in config and 'ignore_disks' in config[vm_item.number]:
                        disks_to_ignore = config[vm_item.number]['ignore_disks'].replace(' ', '').split(',')
                    if disk_name not in disks_to_ignore:
                        vm_item.rbd_disks.append(disk_name.replace(storage['name'], storage['pool']))
                    del disk_name
                    del disks_to_ignore
                del storage
            vm_item.config += '{0}: {1}\n'.format(item['key'], item['value'])
        del cfg

        # extract vm uuid from smbios1
        tmp_id = tmp_id.split(',')
        for item in tmp_id:
            if item.startswith('uuid='):
                tmp_id = item[5::]
                break
        vm_item.id = tmp_id
        vms.append(vm_item)
        del tmp_id
        del vm_item
del vm
del item
del node
del tmp_vms
