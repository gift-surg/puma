import logging


class ExtraFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # This enables us to have a field called "module_function_line" in a logging config file, and to set its length as a combined string
        record.module_function_line = "%s.%s:%d" % (record.module, record.funcName, record.lineno)  # type: ignore
        return True
