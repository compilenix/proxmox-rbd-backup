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
