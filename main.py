#!/usr/bin/env python3
# TODO: perform restore of given backup
# TODO: write all logged messages into buffer to be able to provide detailed context on exceptions
# TODO: how to handle proxmox snapshots (especially those including RAM)?

import configparser
import os.path
import argparse

from lib.backup import Backup
from lib.helper import *
from lib.helper import Log as log
from tabulate import tabulate
from lib.restore_point import RestorePoint

parser = argparse.ArgumentParser(description='Manage and perform backup / restore of ceph rbd enabled proxmox vms')
subparsers = parser.add_subparsers(dest='action', required=True)

# backup
parser_backup = subparsers.add_parser('backup', help='perform backups & get basic infos about backups')
subparsers_backup = parser_backup.add_subparsers(dest='action_backup')

# backup list
parser_backup_list = subparsers_backup.add_parser('list', help='list vms with backups')

# backup run
parser_backup_run = subparsers_backup.add_parser('run', help='perform backup')
parser_backup_run.add_argument('--vm_uuid', action='store', nargs='*', help='perform backup of this vm(s)')
parser_backup_run.add_argument('--match', action='store', help='perform backup of vm(s) which match the given regex')
parser_backup_run.add_argument('--snapshot_name_prefix', action='store', help='override "snapshot_name_prefix" from config')
parser_backup_run.add_argument('--allow_using_any_existing_snapshot', action='store_true', help='use the latest existing snapshot, instead of one that matches the snapshot_name_prefix. This implies that the existing found snapshot will not be removed after backup completion, if it does not match snapshot_name_prefix')

# restore-point
parser_restore_point = subparsers.add_parser('restore-point', help='manage restore points & get details about restore points')
subparsers_restore_point = parser_restore_point.add_subparsers(dest='action_restore_point', required=True)

# restore-point list
parser_restore_point_list = subparsers_restore_point.add_parser('list', help='list backups of a vm')
parser_restore_point_list.add_argument('vm-uuid', action='store')

# restore-point remove
parser_restore_point_remove = subparsers_restore_point.add_parser('remove', help='remove a restore point from a vm and all associated disks')
parser_restore_point_remove.add_argument('--vm-uuid', action='store')
parser_restore_point_remove.add_argument('--restore-point', action='store', nargs='*')
parser_restore_point_remove.add_argument('--age', action='store', help='timespan, i.e.: 15m, 3h, 7d, 3M, 1y')
parser_restore_point_remove.add_argument('--match', action='store', help='restore point name matches regex')

args = parser.parse_args()

if not os.path.isfile('config/global.ini'):
    raise FileNotFoundError('config/global.ini')

config = configparser.ConfigParser()
config.read('config/global.ini')
servers = config['global']['proxmox_servers'].replace(' ', '').split(',')

if is_list_empty(servers):
    raise RuntimeError('no servers found in config')

log.set_loglevel(map_loglevel(config['global']['log_level']))
log.debug(f'CLI args: {vars(args)}')

if args.action == 'backup':
    backup = Backup(servers, config)
    if args.action_backup == 'run':
        backup.init_proxmox()
        vms_uuid = args.vm_uuid
        vm_name_match = args.match
        snapshot_name_prefix = args.snapshot_name_prefix
        allow_using_any_existing_snapshot = args.allow_using_any_existing_snapshot

        if snapshot_name_prefix:
            backup.set_snapshot_name_prefix(snapshot_name_prefix)
        else:
            backup.set_snapshot_name_prefix(config['global']['snapshot_name_prefix'])

        if not vms_uuid and not vm_name_match:
            backup.run_backup(allow_using_any_existing_snapshot=allow_using_any_existing_snapshot)
            exit(0)

        existing_vms = backup.get_vms_proxmox()
        tmp_vms = []

        if vms_uuid and len(vms_uuid) > 0:
            for i, vm_uuid in enumerate(vms_uuid):
                if vm_uuid in map(lambda x: x.uuid, existing_vms):
                    tmp_vms.append(existing_vms[i])

        if vm_name_match:
            for i, vm_name in enumerate(map(lambda x: x.name, existing_vms)):
                if re.match(vm_name_match, vm_name):
                    tmp_vms.append(existing_vms[i])
        backup.run_backup(tmp_vms, allow_using_any_existing_snapshot=allow_using_any_existing_snapshot)
    if args.action_backup == 'list':
        tmp_vms = []
        for vm in backup.get_vms():
            tmp_vms.append({
                'VMID': vm['vm.id'],
                'Name': vm['vm.name'],
                'UUID': vm['vm.uuid'],
                'Last updated': vm['last_updated']
            })
        print(tabulate(tmp_vms, headers='keys'))
if args.action == 'restore-point':
    restore_point = RestorePoint(servers, config)
    if args.action_restore_point == 'list':
        vm_uuid = getattr(args, 'vm-uuid')
        tmp_points = []
        for point in restore_point.get_restore_points(vm_uuid):
            tmp_points.append({
                'Name': point['name'],
                'Timestamp': point['timestamp']
            })
        print(tabulate(tmp_points, headers='keys'))
    if args.action_restore_point == 'remove':
        vm_uuid = args.vm_uuid
        restore_point_names = args.restore_point
        age = args.age
        match = args.match
        if restore_point_names and len(restore_point_names) > 0:
            for restore_point_name in restore_point_names:
                log.info(f'remove snapshots named {restore_point_name} from {vm_uuid}')
                restore_point.remove_restore_point(vm_uuid, restore_point_name, age, match)
        else:
            restore_point.remove_restore_point(vm_uuid, age=age, match=match)
