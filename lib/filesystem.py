from .helper import Log as log
from .helper import exec_raw


def mount_rbd_metadata_image(image: str, mapped_device_path: str):
    log.debug(f'mount vm metadata filesystem: {image}')
    exec_raw(f'mkdir -p /tmp/{image}')
    exec_raw(f'mount {mapped_device_path} /tmp/{image}')


def unmount_rbd_metadata_image(image_name: str):
    exec_raw(f'umount /tmp/{image_name}')
    exec_raw(f'rmdir /tmp/{image_name}')
