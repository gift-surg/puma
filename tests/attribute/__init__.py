def get_error_message(attribute_name: str, attempted_source: str, allowed_source: str) -> str:
    return rf"Attempting to access attribute '{attribute_name}' from invalid scope." \
           rf" Access only allowed from {allowed_source} but access from {attempted_source} \([0-9\-]*\) attempted"


def get_access_child_from_parent_message(attribute_name: str) -> str:
    return get_error_message(attribute_name, "Parent", "Child")


def get_access_parent_from_child_message(attribute_name: str) -> str:
    return get_error_message(attribute_name, "Child", "Parent")
