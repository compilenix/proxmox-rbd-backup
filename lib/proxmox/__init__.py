import re
from ..helper import Cacheable
from ..helper import Log as log
from .core import ProxmoxAPI
import time


class Node(Cacheable):
    id: str

    def __init__(self, node_id):
        super().__init__()
        self.id = node_id

    def __str__(self):
        return self.id

    def __eq__(self, other):
        return self.id == other.id


class Storage(Cacheable):
    pool: str
    content: str
    type: str
    shared: bool
    name: str
    krbd: bool
    digest: str

    def __init__(self, name, storage_type='', shared=0, content='', pool='', krbd=0, digest=''):
        super().__init__()
        self.name = name
        self.shared = True if shared == 1 else 0
        self.type = storage_type
        self.content = content
        self.pool = pool
        self.krbd = True if krbd == 1 else 0
        self.digest = digest

    def __str__(self):
        return self.name

    def __eq__(self, other):
        return self.digest == other.digest


class Disk(Cacheable):
    name: str
    storage: Storage

    def __init__(self, image: str, storage: Storage):
        super().__init__()
        self.storage = storage
        self.name = image

    def __str__(self):
        return f'{self.storage}:{self.name}'

    def __eq__(self, other):
        return self.storage == other.storage and self.name == other.name


class VM(Cacheable):
    status: bool
    __rbd_disks: [Disk]
    __config: str
    node: Node
    name: str
    uuid: str
    id: int

    def __init__(self, vm_id=0, uuid='', name='', node=None, rbd_disks=None, status=''):
        super().__init__()
        self.id = vm_id
        self.uuid = uuid
        self.name = name
        self.node = node
        self.__rbd_disks = rbd_disks if rbd_disks is not None else []
        self.running = True if status == 'running' else False

    def __str__(self):
        return f'{self.name} (id={self.id}, uuid={self.uuid})'

    def __eq__(self, other):
        return self.id == other.id and (True if not self.uuid or not other.uuid else self.uuid == other.uuid)

    def set_config(self, config: [dict]):
        self.__config = ''
        cfg = sorted(config, key=lambda x: x['key'])
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
                    raise RuntimeError(f'could not find uuid of vm {self.id} in config property \"smbios1\"')
                self.uuid = tmp_smbios1

            if item['key'] == 'description':
                for description_line in item["value"].split('\n'):
                    description += f'#{description_line}\n'
            else:
                self.__config += f'{item["key"]}: {item["value"]}\n'
        self.__config = f'{description}{self.__config}'

    def get_config(self):
        return self.__config

    def update_rbd_disks(self, storages: [Storage], disks_to_ignore=None, config: str = None):
        """vm.uuid has to be defined"""
        if not self.uuid:
            raise RuntimeError('self.uuid is empty, this is required to filter excluded disks for this vm (specified in config)')
        if disks_to_ignore is None:
            disks_to_ignore = []
        if not self.__config and not config:
            raise RuntimeError('config is None')

        disks = []
        config = config if config else self.__config
        for line in config.split('\n'):
            if re.match(r'^(scsi|sata|ide|virtio|efidisk|unused)\d', line) is None:
                continue
            for storage in storages:
                if f': {storage}:' not in line:
                    continue
                disk = line.replace(' ', '').split(':')[2]  # remove spaces, remove config key and split disk storage name from disk name
                disk = disk.split(',')[0]  # remove optional disk parameters
                disk = Disk(disk, storage)
                if disk in disks_to_ignore:
                    log.debug(f'ignore proxmox vm disk: {disk} as requested by config [{self.uuid} (name={self.name}, id={self.id})] -> \"ignore_disks\": {", ".join(disks_to_ignore)}')
                    continue
                log.debug(f'found proxmox vm disk: {disk}')
                disks.append(disk)
        self.__rbd_disks = disks

    def get_rbd_disks(self):
        return self.__rbd_disks


class Proxmox:
    _vms: [VM]
    _storages: [Storage]
    _nodes: [Node]
    verify_ssl: bool
    password: str
    user: str
    servers: [str]
    session: ProxmoxAPI

    def __init__(self, servers, username, password, verify_ssl=True):
        self.servers = servers if servers else []
        self.user = username
        self.password = password
        self.verify_ssl = verify_ssl
        self.session = ProxmoxAPI(self.servers[0], 'https', user=self.user, password=self.password, verify_ssl=self.verify_ssl)
        self._nodes = []
        self._storages = []
        self._vms = []

    def update_nodes(self):
        tmp_nodes = self.session.nodes.get()
        tmp_nodes = sorted(tmp_nodes, key=lambda x: x['node'])
        self._nodes = []
        for node in tmp_nodes:
            self._nodes.append(Node(node['node']))

    def get_nodes(self):
        return self._nodes

    def update_storages(self, storages_to_ignore=None):
        if storages_to_ignore is None:
            storages_to_ignore = []
        tmp_storages = self.session.storage.get(type='rbd')
        tmp_storages = sorted(tmp_storages, key=lambda x: x['storage'])
        self._storages = []
        for storage in tmp_storages:
            if storage['storage'] in storages_to_ignore:
                log.debug(f'ignore proxmox storage {storage["storage"]}')
                continue
            if 'images' in storage['content']:
                self._storages.append(Storage(name=storage['storage'], storage_type=storage['type'], shared=storage['shared'], content=storage['content'], pool=storage['pool'], krbd=int(storage['krbd']), digest=storage['digest']))

    def get_storages(self):
        return self._storages

    def update_vms(self, vms_to_ignore=None):
        if vms_to_ignore is None:
            vms_to_ignore = []
        self._vms = []
        log.info('get vm\'s...')
        for node in self._nodes:
            log.info(f'get vm\'s from node {node.id}')
            tmp_vms = self.session.nodes(node.id).qemu.get()
            tmp_vms = sorted(tmp_vms, key=lambda x: x['vmid'])
            for vm in tmp_vms:
                tmp_vm = VM(vm['vmid'], name=vm['name'], node=node, status=vm['status'])
                log.debug(f'found vm {tmp_vm.id} with name {tmp_vm.name}')
                tmp_vm.set_config(self.session.nodes(node.id).qemu(tmp_vm.id).get('pending'))

                # check if this vm should be excluded according to config
                if tmp_vm.uuid in vms_to_ignore:
                    log.debug(f'ignore vm as requested ({tmp_vm})')
                    continue

                self._vms.append(tmp_vm)

    def get_vms(self):
        return self._vms

    def create_vm_snapshot(self, vm: VM, name: str):
        log.info(f'create vm snapshot via proxmox api for {vm}')
        results = self.session.nodes(vm.node).qemu(vm.id).post('snapshot', snapname=name, vmstate=0, description='!!!DO NOT REMOVE!!!automated snapshot by proxmox-rbd-backup. !!!DO NOT REMOVE!!!')
        if 'UPID' not in results:
            raise RuntimeError(f'unexpected result while creating proxmox vm snapshot of {vm} result: {results}')
        del results

        tries = 500
        tries_attempted = tries
        succeed = False
        while not succeed and tries > 0:
            time.sleep(1)
            tries -= 1
            results = self.session.nodes(vm.node).qemu(vm.id).get('snapshot')
            for vm_state in results:
                if 'name' in vm_state and vm_state['name'] == name:
                    succeed = True
                    break
        del tries, results, vm_state

        if not succeed:
            raise RuntimeError(f'proxmox vm snapshot creation of {vm} tined out after {tries_attempted} tries')
        log.debug(f'snapshot creation for {vm} was successful')

    def delete_vm_snapshot(self, vm: VM, name: str):
        self.session.nodes(vm.node).qemu(vm.id).snapshot(name).delete()

    def get_snapshots(self, vm: VM):
        return self.session.nodes(vm.node).qemu(vm.id).get('snapshot')