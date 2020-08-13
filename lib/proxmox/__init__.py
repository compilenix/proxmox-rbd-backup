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
    _rbd_disks: [Disk]
    _config: str
    node: Node
    name: str
    uuid: str
    id: int
    _guest_agent_info: object

    def __init__(self, vm_id=0, uuid='unknown', name='unknown', node=None, rbd_disks=None, status='unknown'):
        super().__init__()
        self.id = vm_id
        self.uuid = uuid
        self.name = name
        self.node = node
        self._rbd_disks = rbd_disks if rbd_disks is not None else []
        self.running = True if status == 'running' else False
        self._guest_agent_info = None
        self._config = ''
        self.status = False

    def __str__(self):
        return f'{self.name} (id={self.id}, uuid={self.uuid})'

    def __eq__(self, other):
        if not hasattr(other, 'id'):
            return False
        return self.id == other.id and (True if not self.uuid or not other.uuid else self.uuid == other.uuid)

    def set_config(self, config: [dict]):
        self._config = ''
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
                self._config += f'{item["key"]}: {item["value"]}\n'
        self._config = f'{description}{self._config}'

    def get_config(self):
        return self._config

    def update_rbd_disks(self, storages: [Storage], disks_to_ignore=None, config: str = None):
        """vm.uuid has to be defined"""
        if not self.uuid:
            raise RuntimeError('self.uuid is empty, this is required to filter excluded disks for this vm (specified in config)')
        if disks_to_ignore is None:
            disks_to_ignore = []
        if not self._config and not config:
            raise RuntimeError('config is None')

        disks = []
        config = config if config else self._config
        for line in config.split('\n'):
            if re.match(r'^(scsi|sata|ide|virtio|efidisk)\d', line) is None:
                continue
            for storage in storages:
                if f': {storage}:' not in line:
                    continue
                disk = line.replace(' ', '').split(':')[2]  # remove spaces, remove config key and split disk storage name from disk name
                disk = disk.split(',')[0]  # remove optional disk parameters
                disk = Disk(disk, storage)
                if str(disk) in disks_to_ignore:
                    log.debug(f'ignore proxmox vm disk: {disk} as requested by config [{self.uuid} (name={self.name}, id={self.id})] -> \"ignore_disks\": {", ".join(disks_to_ignore)}')
                    continue
                log.debug(f'found proxmox vm disk: {disk}')
                disks.append(disk)
        self._rbd_disks = disks

    def get_rbd_disks(self):
        return self._rbd_disks

    def set_guest_agent_info(self, agent_info: object):
        self._guest_agent_info = agent_info

    def get_guest_agent_info(self):
        return self._guest_agent_info


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
            for vm in tmp_vms:
                tmp_vm = VM(vm['vmid'], name=vm['name'], node=node, status=vm['status'])
                self.init_vm_config(tmp_vm)
                log.debug(f'found vm: {tmp_vm}')

                # check if this vm should be excluded according to config
                if tmp_vm.uuid in vms_to_ignore:
                    log.debug(f'ignore vm as requested by config ({tmp_vm})')
                    continue

                self._vms.append(tmp_vm)
        self._vms = sorted(self._vms, key=lambda x: x['vmid'])

    def init_vm_config(self, vm: VM, from_cache: bool = True):
        if not vm.get_config() or not from_cache:
            vm.set_config(self.session.nodes(vm.node.id).qemu(vm.id).get('pending'))

    def get_vms(self):
        return self._vms

    def create_vm_snapshot(self, vm: VM, name: str, tries: int):
        self.init_vm_config(vm)
        log.info(f'create vm snapshot via proxmox api for {vm}')
        results = self.session.nodes(vm.node).qemu(vm.id).post('snapshot', snapname=name, vmstate=0, description='!!!DO NOT REMOVE!!! automated snapshot by proxmox-rbd-backup. !!!DO NOT REMOVE!!!')
        if 'UPID' not in results:
            raise RuntimeError(f'unexpected result while creating proxmox vm snapshot of {vm} result: {results}')
        del results

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

    def remove_vm_snapshot(self, vm: VM, name: str):
        if self.is_snapshot_existing(vm, name):
            self.session.nodes(vm.node).qemu(vm.id).snapshot(name).delete()

    def get_snapshots(self, vm: VM):
        snapshots = self.session.nodes(vm.node).qemu(vm.id).get('snapshot')
        for snapshot in snapshots:
            if snapshot['name'] == 'current':
                # this is the proxmox dummy snapshot representing the current state, not an actual one
                snapshots.remove(snapshot)

        return snapshots

    def is_snapshot_existing(self, vm: VM, snapshot_name: str):
        snaps = self.get_snapshots(vm)
        for snap in snaps:
            if snap['name'] == snapshot_name:
                return True
        return False

    def get_snapshot_current(self, vm: VM):
        snapshots = self.session.nodes(vm.node).qemu(vm.id).get('snapshot')
        current = None
        # find dummy snapshot representing the current state
        for snapshot in snapshots:
            if snapshot['name'] == 'current':
                current = snapshot
                break
        # return if the dummy snapshot could not be found or there is no parent snapshot
        if not current or not current['parent']:
            return None
        # find the actual snapshot
        for snapshot in snapshots:
            if snapshot['name'] == current['parent']:
                current = snapshot
                break
        return current

    def update_agent_info(self, vm: VM):
        agent_info = self.session.nodes(vm.node).qemu(vm.id).agent('info').get()
        agent_info = agent_info['result'] if agent_info and agent_info['result'] else None
        vm.set_guest_agent_info(agent_info)
        return agent_info

    def get_or_update_guest_agent_info(self, vm: VM):
        agent_info = vm.get_guest_agent_info()
        return agent_info if agent_info else self.update_agent_info(vm)

    def is_guest_agent_running(self, vm: VM):
        agent_info = self.get_or_update_guest_agent_info(vm)
        return agent_info and agent_info['version'] and agent_info['supported_commands'] and len(agent_info['supported_commands']) > 0

    def is_guest_agent_command_supported(self, vm: VM, command_name: str):
        agent_info = self.get_or_update_guest_agent_info(vm)
        if not self.is_guest_agent_running(vm):
            return False
        for command in agent_info['supported_commands']:
            if command['name'] == command_name and command['enabled']:
                return True
        return False

    def is_feature_available(self, feature: str, vm: VM):
        return self.session.nodes(vm.node).qemu(vm.id).get('feature', feature=feature)['hasFeature']

    def invoke_guest_agent_exec(self, vm: VM, command_name: str):
        if not self.is_guest_agent_command_supported(vm, 'guest-exec'):
            return False
        return self.session.nodes(vm.node).qemu(vm.id).agent('exec').post(command=command_name)

    def invoke_guest_agent_fstrim(self, vm: VM):
        if not self.is_guest_agent_command_supported(vm, 'guest-fstrim'):
            return False
        return self.session.nodes(vm.node).qemu(vm.id).agent('exec').post('fstrim')

    def invoke_guest_agent_fs_freeze(self, vm: VM):
        if not self.is_guest_agent_command_supported(vm, 'guest-fsfreeze-freeze'):
            return False
        return self.session.nodes(vm.node).qemu(vm.id).agent('exec').post('fsfreeze-freeze')

    def invoke_guest_agent_fs_status(self, vm: VM):
        if not self.is_guest_agent_command_supported(vm, 'guest-fsfreeze-status'):
            return False
        return self.session.nodes(vm.node).qemu(vm.id).agent('exec').post('fsfreeze-status')

    def invoke_guest_agent_fs_unfreeze(self, vm: VM):
        if not self.is_guest_agent_command_supported(vm, 'guest-fsfreeze-thaw'):
            return False
        return self.session.nodes(vm.node).qemu(vm.id).agent('exec').post('fsfreeze-thaw')
