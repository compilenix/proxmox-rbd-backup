from proxmoxer import ProxmoxAPI

proxmox = ProxmoxAPI('x.x.x.x', user='root@pam', password='****************', verify_ssl=False)

for node in proxmox.nodes.get():
    for vm in proxmox.nodes(node['node']).qemu.get():
        print("vm {0} on {1} named {2} is {3}" .format(vm['vmid'], node['node'], vm['name'], vm['status']))
