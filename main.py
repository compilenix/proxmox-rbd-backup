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
import lib.helper as helper
import re

if not os.path.isfile('config/global.ini'):
    raise FileNotFoundError('config/global.ini')

config = configparser.ConfigParser()
config.read('config/global.ini')
servers = config['global']['proxmox_servers'].replace(' ', '').split(',')

if helper.is_list_empty(servers):
    raise RuntimeError('no servers found in config')

helper.LOGLEVEL = helper.LOGLEVEL_DEBUG
#logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
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
        helper.log_message('ignore proxmox storage {0}'.format(item['storage']), helper.LOGLEVEL_DEBUG)
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


helper.log_message('get vm\'s...', helper.LOGLEVEL_INFO)
tmp_vms = []
for node in nodes:
    helper.log_message('get vm\'s from node {0}'.format(node['node']), helper.LOGLEVEL_INFO)
    tmp_vms = proxmox.nodes(node['node']).qemu.get()
    for vm in tmp_vms:
        vm_item = VM()
        vm_item.number = vm['vmid']
        vm_item.node = node['node']
        vm_item.name = vm['name']
        vm_item.status = vm['status']
        vm_item.rbd_disks = []
        vm_item.config = ''
        vm_item.id = ''

        helper.log_message('found vm {0} with name {1}'.format(vm_item.number, vm_item.name), helper.LOGLEVEL_DEBUG)

        # check if this vm should be excluded according to config
        if vm_item.number in config and 'ignore' in config[vm_item.number] and config[vm_item.number].getboolean('ignore'):
            helper.log_message('ignore vm as requested by config [{0} (name={1})] -> \"ignore\": {2}'.format(vm_item.number, vm_item.name, config[vm_item.number].getboolean('ignore')), helper.LOGLEVEL_DEBUG)
            continue

        # format and add vm config
        cfg = proxmox.nodes(vm_item.node).qemu(vm_item.number).get('pending')
        cfg = sorted(cfg, key=lambda x: x['key'])
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
                    raise RuntimeError('could not find uuid of vm {0} in config property \"smbios1\"'.format(vm_item.number))
                vm_item.id = tmp_smbios1
                del tmp_smbios1
                del found_uuid
                del smbios_part
            # extract rbd disks from config (if not ignored via config)
            if re.match(r'scsi\d|sata\d|ide\d|virtio\d|efidisk\d', item['key']) is not None:
                storage = is_rbd_disk(item['value'])
                if storage is not None:
                    disk_name = item['value'].split(',')[0]
                    helper.log_message('found proxmox vm disk: ' + disk_name, helper.LOGLEVEL_DEBUG)
                    # can't check if disk should be ignored because vm uuid may not be preset, yet
                    vm_item.rbd_disks.append(disk_name.replace(storage['name'], storage['pool']))
                    del disk_name
                del storage
            vm_item.config += '{0}: {1}\n'.format(item['key'], item['value'])
        del cfg

        for disk in vm_item.rbd_disks:
            disks_to_ignore = [str]
            if vm_item.id in config and 'ignore_disks' in config[vm_item.id]:
                disks_to_ignore = config[vm_item.id]['ignore_disks'].replace(' ', '').split(',')
            if disk in disks_to_ignore:
                helper.log_message('ignore rbd image: {0} as requested by config [{1} (name={2}, number={3})] -> \"ignore_disks\": {4}'.format(disk, vm_item.id, vm_item.name, vm_item.number, ', '.join(disks_to_ignore)), helper.LOGLEVEL_DEBUG)
                vm_item.rbd_disks.remove(disk)
            del disks_to_ignore
        del disk

        vms.append(vm_item)
        del vm_item
    helper.log_message('found {0} vm\'s on {1}'.format(len(tmp_vms), node['node']), helper.LOGLEVEL_INFO)
helper.log_message('found a total of {0} vm\'s on {1} nodes'.format(len(vms), len(nodes)), helper.LOGLEVEL_INFO)
del vm
del item
del node
del tmp_vms


for vm in vms:
    if helper.is_list_empty(vm.rbd_disks):
        helper.log_message('ignore vm {0} (name={1}, number={2}), because it has no rbd disk to backup'.format(vm.id, vm.name, vm.number), helper.LOGLEVEL_DEBUG)
        continue
