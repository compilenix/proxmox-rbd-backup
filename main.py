#!/usr/bin/env python3

import configparser
import os.path
import argparse
import argcomplete
import traceback

from lib.backup import Backup
from lib.helper import *
from lib.helper import Log as log
from tabulate import tabulate
from lib.proxmox import VM
from lib.restore_point import RestorePoint

parser = argparse.ArgumentParser(description='Manage and perform backup / restore of ceph rbd enabled proxmox vms')
subparsers = parser.add_subparsers(dest='action', required=True)

# backup
parser_backup = subparsers.add_parser('backup', help='perform backups & get basic infos about backups')
subparsers_backup = parser_backup.add_subparsers(dest='action_backup')

# backup list
parser_backup_list = subparsers_backup.add_parser('list', aliases=['ls'], help='list vms with backups')

# backup run
parser_backup_run = subparsers_backup.add_parser('run', help='perform backup')
parser_backup_run.add_argument('--vm_uuid', action='store', nargs='*', help='perform backup of this vm(s)')
parser_backup_run.add_argument('--match', action='store', help='perform backup of vm(s) which match the given regex')
parser_backup_run.add_argument('--snapshot_name_prefix', action='store', help='override "snapshot_name_prefix" from config')
parser_backup_run.add_argument('--allow_using_any_existing_snapshot', action='store_true', help='use the latest existing snapshot, instead of one that matches the snapshot_name_prefix. This implies that the existing found snapshot will not be removed after backup completion, if it does not match snapshot_name_prefix.This option is mostly used for adding a new backup interval to an existing backup (only the first backup of that interval needs this option) or for manual / temporary / development backups.')

# backup remove
parser_backup_remove = subparsers_backup.add_parser('remove', aliases=['rm'], help='remove a backup')
parser_backup_remove.add_argument('--vm_uuid', action='store', nargs='*', help='remove backup of this vm(s)')
parser_backup_remove.add_argument('--match', action='store', help='remove backup of vm(s) which match the given regex')
parser_backup_remove.add_argument('--force', action='store_true', help='remove restore points, too')

# restore-point
parser_restore_point = subparsers.add_parser('restore-point', help='manage restore points & get details about restore points')
subparsers_restore_point = parser_restore_point.add_subparsers(dest='action_restore_point', required=True)

# restore-point list
parser_restore_point_list = subparsers_restore_point.add_parser('list', aliases=['ls'], help='list backups of a vm')
parser_restore_point_list.add_argument('vm-uuid', action='store')

# restore-point info
parser_restore_point_info = subparsers_restore_point.add_parser('info', help='get details of a restore point')
parser_restore_point_info.add_argument('vm-uuid', action='store')
parser_restore_point_info.add_argument('restore-point', action='store')

# restore-point remove
parser_restore_point_remove = subparsers_restore_point.add_parser('remove', aliases=['rm'], help='remove a restore point from a vm and all associated disks')
parser_restore_point_remove.add_argument('--vm-uuid', action='store')
parser_restore_point_remove.add_argument('--restore-point', action='store', nargs='*')
parser_restore_point_remove.add_argument('--age', action='store', help='timespan, i.e.: 15m, 3h, 7d, 3M, 1y')
parser_restore_point_remove.add_argument('--match', action='store', help='restore point name matches regex')

argcomplete.autocomplete(parser)
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

try:
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
            tmp_vms = unique_list(tmp_vms)
            backup.run_backup(tmp_vms, allow_using_any_existing_snapshot=allow_using_any_existing_snapshot)
        if re.match(r'^(list|ls)$', args.action_backup):
            tmp_vms = []
            for vm in backup.get_vms():
                tmp_vms.append({
                    'VMID': vm['vm.id'],
                    'Name': vm['vm.name'],
                    'UUID': vm['vm.uuid'],
                    'Last updated': vm['last_updated']
                })
            tmp_vms = unique_list(tmp_vms)
            print(tabulate(tmp_vms, headers='keys'))
        if re.match(r'^(remove|rm)$', args.action_backup):
            vms_uuid = args.vm_uuid
            vm_name_match = args.match
            force = args.force

            if is_list_empty(vms_uuid) and not vm_name_match:
                log.error('require any of: vms_uuid, vm_name_match')
                exit(1)

            restore_point = RestorePoint(servers, config)
            backup.init_proxmox()
            vms_proxmox = backup.get_vms_proxmox()
            vm_backups = backup.get_vms()
            tmp_vms = []  # type: [VM]
            if vms_uuid and len(vms_uuid) > 0:
                done = False
                for vm_uuid in vms_uuid:
                    for vm in vms_proxmox:
                        if vm.uuid == vm_uuid:
                            tmp_vms.append(vm)
                            done = True
                            break
                    if done:
                        break

            if vm_name_match:
                for vm_name in map(lambda x: x['vm.name'], vm_backups):
                    if re.match(vm_name_match, vm_name):
                        for vm in vms_proxmox:
                            if vm.name == vm_name:
                                tmp_vms.append(vm)

            if len(tmp_vms) == 0:
                exit(0)

            for vm in tmp_vms:
                if force:
                    restore_points = restore_point.get_restore_points(vm.uuid)
                    for point in restore_points:
                        restore_point.remove_restore_point(vm.uuid, point['name'], backup=backup)
                try:
                    restore_point.remove_backup(vm.uuid)
                except Exception as error:
                    log.error(f'could not remove backup of {vm}: {error}')

    if args.action == 'restore-point':
        restore_point = RestorePoint(servers, config)
        if re.match(r'^(list|ls)$', args.action_restore_point):
            vm_uuid = getattr(args, 'vm-uuid')
            tmp_points = []

            for point in restore_point.get_restore_points(vm_uuid):
                tmp_points.append({
                    'Name': point['name'],
                    'Timestamp': point['timestamp']
                })
            print(tabulate(tmp_points, headers='keys'))
        if args.action_restore_point == 'info':
            arg_uuid = getattr(args, 'vm-uuid')
            arg_restore_point = getattr(args, 'restore-point')
            backup = Backup(servers, config)
            backup.init_proxmox()

            point_details = restore_point.get_restore_point_detail(arg_uuid, arg_restore_point, backup=backup)
            print(f'Summary:\n'
                  f'  VM: {backup.get_vm(arg_uuid)}\n'
                  f'  Restore point name: {arg_restore_point}\n'
                  f'  Timestamp: {point_details["timestamp"]}\n'
                  f'  Has Proxmox Snapshot: {point_details["has_proxmox_snapshot"]}\n'
                  f'  RBD images: {len(point_details["images"])}\n')
            tmp_points = []
            for point in point_details['images']:
                tmp_points.append({
                    'Name': point['name'],
                    'Image': point['image']
                })
            print('Images:')
            print(tabulate(tmp_points, headers='keys'))
        if re.match(r'^(remove|rm)$', args.action_restore_point):
            vm_uuid = args.vm_uuid
            restore_point_names = args.restore_point
            age = args.age
            match = args.match
            backup = Backup(servers, config)
            backup.init_proxmox()

            if restore_point_names and len(restore_point_names) > 0:
                for restore_point_name in restore_point_names:
                    log.info(f'remove snapshots named {restore_point_name} from {vm_uuid}')
                    restore_point.remove_restore_point(vm_uuid, restore_point_name, age, match, backup=backup)
            else:
                restore_point.remove_restore_point(vm_uuid, age=age, match=match, backup=backup)

except KeyboardInterrupt:
    log.warn('Interrupt, terminating...')

except Exception as e:
    log.error(f'unexpected exception (probably a bug): {e}')
    print('=========================  DEBUG LOG  =========================')
    print(log.get_log_buffer())
    print('=========================  TRACEBACK  =========================')
    traceback.print_exc()
