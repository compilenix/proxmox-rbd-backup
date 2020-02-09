# TODO: list vm's in backup
# TODO: list backups of given vm
# TODO: perform restore of given backup
# TODO: write all logged messages into buffer to be able to provide detailed context on exceptions
# TODO: how to handle proxmox snapshots (especially those including RAM)?

import configparser
import os.path
from lib.helper import *
from lib.helper import Log as log
from lib.proxmox import Proxmox, Disk
import re
from lib.ceph import Ceph
import random
import time
import argparse

parser = argparse.ArgumentParser(description='Manage and perform backup / restore of ceph rbd enabled proxmox vms')
subparsers = parser.add_subparsers(dest='action')

# backup
parser_backup = subparsers.add_parser('backup', help='perform backups & get basic infos about backups')
subparsers_backup = parser_backup.add_subparsers(dest='action_backup')

# backup list
parser_backup_list = subparsers_backup.add_parser('list', help='list vms with backups')

# backup run
parser_backup_run = subparsers_backup.add_parser('run', help='perform backup')


# restore-point
parser_restore_point = subparsers.add_parser('restore-point', help='manage restore points & get details about restore points')
subparsers_restore_point = parser_restore_point.add_subparsers(dest='action_restore_point')

# restore-point list
parser_restore_point_list = subparsers_restore_point.add_parser('list', help='list backups of a vm')
parser_restore_point_list.add_argument('vm-uuid', action="store")

# restore-point delete
parser_restore_point_delete = subparsers_restore_point.add_parser('delete', help='remove a restore point from a vm and all associated disks')
parser_restore_point_delete.add_argument('vm-uuid', action="store")
parser_restore_point_delete.add_argument('restore-point', action="store")

args = parser.parse_args()

print(vars(args))

exit(0)

if not os.path.isfile('config/global.ini'):
    raise FileNotFoundError('config/global.ini')

ceph = Ceph()
config = configparser.ConfigParser()
config.read('config/global.ini')
servers = config['global']['proxmox_servers'].replace(' ', '').split(',')

if is_list_empty(servers):
    raise RuntimeError('no servers found in config')

remote_connection_command = f'ssh {config["global"]["proxmox_ssh_user"]}@{servers[0]} -T -o Compression=no -x'

log.set_loglevel(LOGLEVEL_DEBUG)
proxmox = Proxmox(servers, username=config['global']['user'], password=config['global']['password'], verify_ssl=config['global'].getboolean('verify_ssl'))
proxmox.update_nodes()

storages_to_ignore = []
vms_to_ignore = []
if 'ignore_storages' in config['global']:
    for item in config['global']['ignore_storages'].replace(' ', '').split(','):
        storages_to_ignore.append(item)
for section in config:
    if 'ignore' in config[section] and config[section]['ignore']:
        vms_to_ignore.append(section)

proxmox.update_storages(storages_to_ignore)
proxmox.update_vms(vms_to_ignore)


def mount_rbd_metadata_image(image: str, mapped_device_path: str):
    log.debug(f'mount vm metadata filesystem: {image}')
    exec_raw(f'mkdir -p /tmp/{image}')
    exec_raw(f'mount {mapped_device_path} /tmp/{image}')


def unmount_rbd_metadata_image(image_name: str):
    exec_raw(f'umount /tmp/{image_name}')
    exec_raw(f'rmdir /tmp/{image_name}')


for vm in proxmox.get_vms():
    snapshot_name = config['global']['snapshot_name_prefix'] + ''.join([random.choice('0123456789abcdef') for _ in range(16)])
    rbd_image_vm_metadata_name = vm.uuid + '_vm_metadata'
    log.info(f'save current config into vm metadata image of vm {vm.uuid} (id={vm.id}, name={vm.name})')
    backup_rbd_pool = config['global']['ceph_backup_pool']
    is_vm_metadata_existing = ceph.is_rbd_image_existing(backup_rbd_pool, rbd_image_vm_metadata_name)
    mapped_image_path = ''
    if is_vm_metadata_existing:
        # map vm metadata image
        mapped_image_path = ceph.map_rbd_image(backup_rbd_pool, rbd_image_vm_metadata_name)
        # mount vm metadata image
        mount_rbd_metadata_image(rbd_image_vm_metadata_name, mapped_image_path)
    else:
        # create vm metadata image
        log.info('metadata image for vm not existing; creating...')
        ceph.create_rbd_image(backup_rbd_pool, rbd_image_vm_metadata_name, config['global']['vm_metadata_image_size'])
        is_vm_metadata_existing = ceph.is_rbd_image_existing(backup_rbd_pool, rbd_image_vm_metadata_name)
        if not is_vm_metadata_existing:
            raise RuntimeError(f'ceph metadata image for vm is not existing right after creation, this may be a transient error: {rbd_image_vm_metadata_name}')
        if 'ceph_backup_disable_rbd_image_features_for_metadata' in config['global'] and len(config['global']['ceph_backup_disable_rbd_image_features_for_metadata']) > 0:
            # disable metadata image features (if needed)
            exec_raw(f'rbd feature disable {rbd_image_vm_metadata_name} {" ".join(config["global"]["ceph_backup_disable_rbd_image_features_for_metadata"].replace(" ", "").split(","))}')
        # map metadata image
        mapped_image_path = ceph.map_rbd_image(backup_rbd_pool, rbd_image_vm_metadata_name)
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
    ceph.unmap_rbd_image(backup_rbd_pool, rbd_image_vm_metadata_name)
    ceph.create_rbd_snapshot(backup_rbd_pool, rbd_image_vm_metadata_name, new_snapshot_name=snapshot_name)

    # update vm disks to backup
    disks_to_ignore = []
    for section in config:
        if section == vm.uuid and 'ignore_disks' in config[section] and config[section]['ignore_disks']:
            for disk in config[section]['ignore_disks'].replace(' ', '').split(','):
                disk = disk.split('/')
                disks_to_ignore.append(Disk(disk[0], disk[1]))
    vm.update_rbd_disks(proxmox.get_storages(), disks_to_ignore)

    # check for existing vm snapshot
    existing_backup_snapshot_count = 0
    existing_backup_snapshot = ''
    snapshots = proxmox.get_snapshots(vm)
    for vm_state in snapshots:
        if 'name' in vm_state and re.match(config['global']['snapshot_name_prefix'] + r'.+', vm_state['name']):
            existing_backup_snapshot = vm_state['name']
            existing_backup_snapshot_count += 1
    del snapshots, vm_state
    if existing_backup_snapshot_count > 1:
        # TODO: implement
        log.warn(f'last backup is incomplete, re-processing')
        raise RuntimeError(f'last backup is incomplete, re-processing. automatic repair not implemented, yet. Manual fixing required')

    # create vm snapshot via proxmox api
    proxmox.create_vm_snapshot(vm, snapshot_name)

    # backup rbd disk's of vm
    for disk in vm.get_rbd_disks():
        image = rbd_image_from_proxmox_disk(disk)
        # wait for snapshot creation completion
        tries = 500
        tries_attempted = tries
        succeed = False
        while not succeed and tries > 0:
            log.debug(f'wait for snapshot creation completion of {vm} -> {image}@{snapshot_name}. {tries} tries left oft {tries_attempted}')
            time.sleep(1)
            tries -= 1
            results = ceph.get_rbd_snapshots_by_prefix(image.pool, image.name, config['global']['snapshot_name_prefix'], remote_connection_command)
            for snap in results:
                if 'name' in snap and snap['name'] == snapshot_name:
                    log.debug(f'snapshot of {vm} -> {image}@{snapshot_name} found')
                    succeed = True
                    break
        if not succeed:
            raise RuntimeError(f'waiting for ceph rbd snapshot creation completion of {vm} -> {image} tined out after {tries_attempted} tries')
        del tries, results, snap, succeed, tries_attempted

        # perform initial backup
        if existing_backup_snapshot_count == 0:
            log.info(f'initial backup, starting full copy of {vm} -> {image}')
            image_size = exec_parse_json(f'{remote_connection_command} rbd info {image} --format json')['size']
            exec_raw(f'/bin/bash -c set -o pipefail; {remote_connection_command} "rbd export --no-progress {image}@{snapshot_name} -" | pv --rate --bytes --progress --timer --eta --size {image_size} | rbd import --no-progress - {backup_rbd_pool}/{vm.uuid}-{image.pool}-{image.name}')
            ceph.create_rbd_snapshot(backup_rbd_pool, f'{vm.uuid}-{image.pool}-{image.name}', new_snapshot_name=snapshot_name)
            log.info(f'initial backup of {vm} -> {image} complete')
            del image_size

        # perform incremental backup
        if existing_backup_snapshot_count == 1:
            log.info(f'incremental backup, starting for {vm} -> {image}')
            whole_object_command = ''
            if 'enable_intra_object_delta_transfer' in config['global'] and not config['global'].getboolean('enable_intra_object_delta_transfer'):
                whole_object_command = '--whole-object'
            exec_raw(f'/bin/bash -c set -o pipefail; {remote_connection_command} "rbd export-diff --no-progress {whole_object_command} --from-snap {existing_backup_snapshot} {image}@{snapshot_name} -" | pv --rate --bytes --timer | rbd import-diff --no-progress - {backup_rbd_pool}/{vm.uuid}-{image.pool}-{image.name}')
            log.info(f'incremental backup of {vm} -> {image} complete')
            del whole_object_command

        # check if image and snapshot does exist on backup cluster
        log.debug(f'check if image and snapshot does exist on backup cluster for {vm} -> {vm.uuid}-{image.pool}-{image.name}@{snapshot_name}')
        results = ceph.get_rbd_snapshots_by_prefix(backup_rbd_pool, f'{vm.uuid}-{image.pool}-{image.name}', config['global']['snapshot_name_prefix'])
        succeed = False
        for snap in results:
            if 'name' in snap and snap['name'] == snapshot_name:
                log.debug(f'snapshot {backup_rbd_pool}/{vm.uuid}-{image.pool}-{image.name}@{snapshot_name} found')
                succeed = True
        if not succeed:
            raise RuntimeError('image and snapshot does exist on backup cluster')
        del results, succeed, snap, disk, image

    # remove old vm snapshot
    if existing_backup_snapshot_count == 1:
        proxmox.delete_vm_snapshot(vm, existing_backup_snapshot)
