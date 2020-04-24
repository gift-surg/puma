from puma.attribute import ATTRIBUTE_NAME_PREFIX, ATTRIBUTE_NAME_SEPARATOR


def format_scope_id_for_attribute_id(scope_id: str) -> str:
    return f"##{scope_id}##"


def create_attribute_id(state_manager_identifier: str, attribute_name: str) -> str:
    return f"{ATTRIBUTE_NAME_PREFIX}_{ATTRIBUTE_NAME_SEPARATOR}{state_manager_identifier}{ATTRIBUTE_NAME_SEPARATOR}_{attribute_name}"
