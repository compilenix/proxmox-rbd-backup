import re
from lib.helper import log_message, LOGLEVEL_DEBUG, LOGLEVEL_INFO
from proxmoxer import ProxmoxAPI, logging


class Node:
    name: str
    id: int

    def __init__(self):
        self.id = 0
        self.name = ''

    def __str__(self):
        return self.name


class Storage:
    pool: str
    content: str
    type: str
    shared: bool
    name: str
    krbd: bool
    digest: str

    def __init__(self, name, storage_type='', shared=0, content='', pool='', krbd=0, digest=''):
        self.name = name
        self.shared = True if shared == 1 else 0
        self.type = storage_type
        self.content = content
        self.pool = pool
        self.krbd = True if krbd == 1 else 0
        self.digest = digest

    def __str__(self):
        return self.name


class Disk:
    name: str
    storage: Storage

    def __init__(self, image: str, storage):
        self.storage: Storage = storage
        self.name = image

    def __str__(self):
        return f'{self.storage}:{self.name}'


class VM:
    status: bool
    __rbd_disks: [Disk]
    __config: str
    node: Node
    name: str
    uuid: str
    id: int

    def __init__(self, vm_id=0, uuid='', name='', node=None, rbd_disks=None, status=''):
        self.id = vm_id
        self.uuid = uuid
        self.name = name
        self.node = node
        self.__rbd_disks = rbd_disks if rbd_disks is not None else []
        self.running = True if status == 'running' else False

    def __str__(self):
        return f'{self.name} (id={self.id}, uuid={self.uuid})'

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

    def update_rbd_disks(self, storages: [Storage], config: str = None):
        if not self.__config and not config:
            raise RuntimeError('config is None')

        disks = []
        config = config if config else self.__config
        for line in config.split('\n'):
            if re.match(r'^(scsi|sata|ide|virtio|efidisk|unused)\d', line) is None:
                continue
            for storage in storages:
                if not line.startswith(f'{storage}'):
                    continue
                disk_identifier = line.split(',')[0]
                image = disk_identifier.replace(storage.name, storage.pool).split(':')[1]
                disk = Disk(image, storage)
                log_message(f'found proxmox vm disk: {disk}', LOGLEVEL_DEBUG)
                disks.append(disk)
        self.__rbd_disks = disks

    def update_uuid(self, config: str = None):
        if not self.__config and not config:
            raise RuntimeError('config is None')

        config = config if config else self.__config
        for line in config.split('\n'):
            if not line.startswith('smbios1'):
                continue
            smbios1 = line.split(',')
            found_uuid = False
            for smbios_part in smbios1:
                if smbios_part.startswith('uuid='):
                    smbios1 = smbios_part[5::]
                    found_uuid = True
                    break
            if not found_uuid:
                raise RuntimeError(f'could not find uuid of vm {self.id} in config property \"smbios1\"')
            self.uuid = smbios1


class ProxmoxApiSession:
    host: str
    verify_ssl: bool
    password: str
    user: str
    servers: [str]
    __session: ProxmoxAPI

    def __init__(self, servers, user, password, host, verify_ssl=True):
        self.servers = servers if servers else []
        self.user = user
        self.password = password
        self.host = host
        self.verify_ssl = verify_ssl
        self.__session = self.__init_session()

    def __init_session(self):
        return ProxmoxAPI(self.host, 'https', user=self.user, password=self.password, verify_ssl=self.verify_ssl)

    # TODO: continue here... create abstraction for ProxmoxAPI requests to intercept api session timeout


class Proxmox:
    nodes: [Node]
    verify_ssl: bool
    password: str
    user: str
    servers: [str]
    session: ProxmoxAPI

    def __init__(self):
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s:%(name)s: %(message)s')
        self.servers = []
        self.user = ''
        self.password = ''
        self.verify_ssl = True
        self.nodes = []

    def create_vm_snapshot(self, vm: VM, name: str):
        log_message(f'create vm snapshot via proxmox api for {vm}', LOGLEVEL_INFO)
        results = proxmox.nodes(vm.node).qemu(vm.id).post('snapshot', snapname=name, vmstate=0, description='!!!DO NOT REMOVE!!!automated snapshot by proxmox-rbd-backup. !!!DO NOT REMOVE!!!')
        if 'UPID' not in results:
            raise RuntimeError(f'unexpected result while creating proxmox vm snapshot of {vm} result: {results}')
        del results

        tries = 60
        tries_attempted = tries
        succeed = False
        while not succeed and tries > 0:
            time.sleep(1)
            tries -= 1
            results = proxmox.nodes(vm.node).qemu(vm.id).get('snapshot')
            for vm_state in results:
                if 'name' in vm_state and vm_state['name'] == name:
                    succeed = True
                    break
        del tries, results, vm_state

        if not succeed:
            raise RuntimeError(f'proxmox vm snapshot creation of {vm} tined out after {tries_attempted} tries')
        log_message(f'snapshot creation for {vm} was successful', LOGLEVEL_DEBUG)
