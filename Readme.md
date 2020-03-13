# Requirements
- python3.7+
- pip3
- bash
- pv
- lz4
- ssh
- ceph nautilus (or newer)
- ceph tools
    - ceph
    - rbd
- ssh access from backup system to production proxmox / ceph cluster
- admin access to proxmox web api

# Help
## main.py
```
usage: main.py [-h] {backup,restore-point} ...

Manage and perform backup / restore of ceph rbd enabled proxmox vms

positional arguments:
  {backup,restore-point}
    backup              perform backups & get basic infos about backups
    restore-point       manage restore points & get details about restore
                        points
```
## main.py backup
```
usage: main.py backup [-h] {list,ls,run,remove,rm} ...

positional arguments:
  {list,ls,run,remove,rm}
    list (ls)           list vms with backups
    run                 perform backup
    remove (rm)         remove a backup
```
## main.py backup list
```
usage: main.py backup list [-h]
```

### Example
```
$ main.py backup list
  VMID  Name                UUID                                  Last updated
------  ------------------  ------------------------------------  --------------------------
   100  srv-01              f67efb32-c284-40c1-8d54-daf17a5d1ce2  2020-03-13 21:00:07.401266
   101  srv-02              da7a9f27-1641-4bb8-a975-ef2828a422be  2020-03-13 21:10:15.207048
   107  dc01                ceca542c-d01a-4c1c-9290-007d39632f3b  2020-03-13 21:07:33.665873
   110  testvm              5ecca473-6969-4d55-b1a7-47503980fe52  2020-03-13 21:10:09.681048
```

## main.py backup run
```
usage: main.py backup run [-h] [--vm_uuid [VM_UUID [VM_UUID ...]]]
                          [--match MATCH]
                          [--snapshot_name_prefix SNAPSHOT_NAME_PREFIX]
                          [--allow_using_any_existing_snapshot]

optional arguments:
  -h, --help            show this help message and exit
  --vm_uuid [VM_UUID [VM_UUID ...]]
                        perform backup of this vm(s)
  --match MATCH         perform backup of vm(s) which match the given regex
  --snapshot_name_prefix SNAPSHOT_NAME_PREFIX
                        override "snapshot_name_prefix" from config
  --allow_using_any_existing_snapshot
                        use the latest existing snapshot, instead of one that
                        matches the snapshot_name_prefix. This implies that
                        the existing found snapshot will not be removed after
                        backup completion, if it does not match
                        snapshot_name_prefix.This option is mostly used for
                        adding a new backup interval to an existing backup
                        (only the first backup of that interval needs this
                        option) or for manual / temporary / development
                        backups.
```

## main.py remove
```
usage: main.py backup remove [-h] [--vm_uuid [VM_UUID [VM_UUID ...]]]
                             [--match MATCH] [--force]

optional arguments:
  -h, --help            show this help message and exit
  --vm_uuid [VM_UUID [VM_UUID ...]]
                        remove backup of this vm(s)
  --match MATCH         remove backup of vm(s) which match the given regex
  --force               remove restore points, too
```

## main.py restore-point
```
usage: main.py restore-point [-h] {list,ls,info,remove,rm} ...

positional arguments:
  {list,ls,info,remove,rm}
    list (ls)           list backups of a vm
    info                get details of a restore point
    remove (rm)         remove a restore point from a vm and all associated
                        disks
```

## main.py restore-point list
```
usage: main.py restore-point list [-h] vm-uuid

positional arguments:
  vm-uuid
```
### Example
```
$ main.py restore-point list f67efb32-c284-40c1-8d54-daf17a5d1ce2
Name                           Timestamp
-----------------------------  ------------------------
backup_daily_7ad726ab12670638  Sun Mar  8 21:00:04 2020
backup_daily_f371aee79f52a83c  Mon Mar  9 21:00:07 2020
backup_daily_2d98c56c54e35429  Tue Mar 10 21:00:04 2020
backup_daily_b51cadf45b4208ed  Wed Mar 11 21:00:04 2020
backup_daily_c7bb3f42d7911d6e  Thu Mar 12 21:00:08 2020
backup_daily_be19c417474edcbe  Fri Mar 13 21:00:08 2020
```

## main.py restore-point info
```
usage: main.py restore-point info [-h] vm-uuid restore-point

positional arguments:
  vm-uuid
  restore-point
```

### Example
```
$ main.py restore-point info f67efb32-c284-40c1-8d54-daf17a5d1ce2 backup_daily_be19c417474edcbe
Summary:
  VM: srv-proxy01 (id=100, uuid=f67efb32-c284-40c1-8d54-daf17a5d1ce2)
  Restore point name: backup_daily_be19c417474edcbe
  Timestamp: Fri Mar 13 21:00:08 2020
  Has Proxmox Snapshot: True
  RBD images: 3

Images:
Name                           Image
-----------------------------  -----------------------------------------------------------------
backup_daily_be19c417474edcbe  rbd/f67efb32-c284-40c1-8d54-daf17a5d1ce2-rbd-vm-100-disk-0
backup_daily_be19c417474edcbe  rbd/f67efb32-c284-40c1-8d54-daf17a5d1ce2-uefi_disks-vm-100-disk-0
backup_daily_be19c417474edcbe  rbd/f67efb32-c284-40c1-8d54-daf17a5d1ce2_vm_metadata
```

## main.py restore-point remove
```
usage: main.py restore-point remove [-h] [--vm-uuid VM_UUID]
                                    [--restore-point [RESTORE_POINT [RESTORE_POINT ...]]]
                                    [--age AGE] [--match MATCH]

optional arguments:
  -h, --help            show this help message and exit
  --vm-uuid VM_UUID
  --restore-point [RESTORE_POINT [RESTORE_POINT ...]]
  --age AGE             timespan, i.e.: 15m, 3h, 7d, 3M, 1y
  --match MATCH         restore point name matches regex
```

# Manual restore
> **WARNING**: Read the complete procedure and understand the implications of each step before starting a manual restore!

## VM Config
- power off running vm
- get vm config from backup
    ```shell script
    # get vm id and uuid
    proxmox-rbd-backup backup list
    # get restore point name
    proxmox-rbd-backup restore-point list 38f8188f-7051-44e0-98d8-25fabaa3c459
    # get metadata image name
    proxmox-rbd-backup restore-point info 38f8188f-7051-44e0-98d8-25fabaa3c459 dev_00bb3dd7aafd85ca
    # map rbd image on local system
    rbd device map --read-only rbd/38f8188f-7051-44e0-98d8-25fabaa3c459_vm_metadata@dev_00bb3dd7aafd85ca
    # get block device path of mapped image
    rbd device list
    # mount filesystem
    mkdir -pv /tmp/38f8188f-7051-44e0-98d8-25fabaa3c459_vm_metadata-dev_00bb3dd7aafd85ca && mount /dev/rbd0 /tmp/38f8188f-7051-44e0-98d8-25fabaa3c459_vm_metadata-dev_00bb3dd7aafd85ca
    # get config
    cat /tmp/38f8188f-7051-44e0-98d8-25fabaa3c459_vm_metadata-dev_00bb3dd7aafd85ca/100.conf
    # umount filesystem
    umount /tmp/38f8188f-7051-44e0-98d8-25fabaa3c459_vm_metadata-dev_00bb3dd7aafd85ca && rmdir -v /tmp/38f8188f-7051-44e0-98d8-25fabaa3c459_vm_metadata-dev_00bb3dd7aafd85ca
    # unmap rbd image
    rbd device unmap rbd/38f8188f-7051-44e0-98d8-25fabaa3c459_vm_metadata@dev_00bb3dd7aafd85ca
    ```
- Replace vm config with result from `cat`, without removing config states from currently existing snapshots.
- start vm

## VM Disk
- power off running vm
- remove all proxmox snapshots of the vm via web gui
- transfer desired disk(s) from backup cluster into production
    ```shell script
    # get vm id and uuid
    proxmox-rbd-backup backup list
    # get restore point name
    proxmox-rbd-backup restore-point list 38f8188f-7051-44e0-98d8-25fabaa3c459
    # get disks of restore point
    rbd -p rbd ls | grep 38f8188f-7051-44e0-98d8-25fabaa3c459
    # remove rbd image of the vm in production
    rbd snap purge rbd/vm-110-disk-0 && rbd rm rbd/vm-110-disk-0
    # transfer image from backup into production
    set -o pipefail
    rbd export --no-progress rbd/38f8188f-7051-44e0-98d8-25fabaa3c459-rbd-vm-110-disk-0@dev_00bb3dd7aafd85ca - | pv --rate --bytes --timer -c -N export | lz4 -z --fast=12 --sparse | pv --rate --bytes --timer -c -N compressed-network | ssh root@10.1.1.201 -o Compression=no -x "lz4 -d | rbd import --no-progress - rbd/vm-110-disk-0"
    ```
- rename ALL existing rbd images of the vm in backup cluster, to be able to create new restore points
    ```shell script
    rbd -p rbd rename rbd/38f8188f-7051-44e0-98d8-25fabaa3c459-rbd-vm-110-disk-0 38f8188f-7051-44e0-98d8-25fabaa3c459-rbd-vm-110-disk-0_old
    rbd -p rbd rename rbd/38f8188f-7051-44e0-98d8-25fabaa3c459-uefi_disks-vm-110-disk-0 38f8188f-7051-44e0-98d8-25fabaa3c459-uefi_disks-vm-110-disk-0_old
    rbd -p rbd rename rbd/38f8188f-7051-44e0-98d8-25fabaa3c459_vm_metadata 38f8188f-7051-44e0-98d8-25fabaa3c459_vm_metadata_old
    ```
- it's recommended to create a new backup, right away
    ```shell script
    proxmox-rbd-backup backup run --match vm_name
    ```
- if the restored disk was a uefi boot disk you may need to do the following steps, after starting the vm:
  - `Enter BIOS configuration` > `Boot Maintenance Manager` > `Boot Options` > `Add Boot Option` > select the disk, browse to `efi` > `boot` > select bootx64.efi
  - Change Boot order
  - Save all changes
- start vm (is possible while vm backup is still running)
