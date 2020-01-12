from proxmoxer import ProxmoxAPI

proxmox = ProxmoxAPI('x.x.x.x', user='root@pam', password='****************', verify_ssl=False)

for node in proxmox.nodes.get():
    for vm in proxmox.nodes(node['node']).qemu.get():
        config = proxmox.nodes(node['node']).qemu(vm['vmid']).config.get()
        print("vm {0} on {1} named {2} is {3} with confsig {4}" .format(vm['vmid'], node['node'], vm['name'], vm['status'], config))

# TODO: read backup jobs
# TODO: get nodes
# TODO: get rbd storage's
# TODO: get vm config's
# TODO: get vm rbd disk's
# TODO: create and mount metadata of vm in backup cluster
# TODO: copy current vm config to metadata
# TODO: unmount metadata
# TODO: create snapshot of metadata
# TODO: create vm snapshot
# TODO: backup rbd disk's of vm
# TODO: remove old vm snapshot
