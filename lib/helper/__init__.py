def is_list_empty(items) -> bool:
    if items is None:
        return True
    if len(items) == 0:
        return True
    first = items[0]
    if len(items) == 1 and (first is None or first == '' or first == 0):
        return True
    return False
