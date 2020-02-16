# TODO: list backups of given vm
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
parser_backup_run.add_argument('--vm', action='store', nargs='*', help='perform backup of this vm(s)')
parser_backup_run.add_argument('--snapshot_name_prefix', action='store', help='override "snapshot_name_prefix" from config')

# restore-point
parser_restore_point = subparsers.add_parser('restore-point', help='manage restore points & get details about restore points')
subparsers_restore_point = parser_restore_point.add_subparsers(dest='action_restore_point', required=True)

# restore-point list
parser_restore_point_list = subparsers_restore_point.add_parser('list', help='list backups of a vm')
parser_restore_point_list.add_argument('vm-uuid', action='store')

# restore-point delete
parser_restore_point_delete = subparsers_restore_point.add_parser('delete', help='remove a restore point from a vm and all associated disks')
parser_restore_point_delete.add_argument('vm-uuid', action='store')
parser_restore_point_delete.add_argument('restore-point', action='store', nargs='*')

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
        # TODO: add from args: vms, snapshot_name_prefix
        backup.run_backup()
    if args.action_backup == 'list':
        tmp_vms = []
        for vm in backup.list_vms():
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
        image = getattr(args, 'vm-uuid')
        tmp_points = []
        for point in restore_point.list_restore_points(image):
            tmp_points.append({
                'Name': point['name'],
                'Timestamp': point['timestamp']
            })
        print(tabulate(tmp_points, headers='keys'))
