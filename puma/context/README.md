## Context management for multi-tasking

PUMA makes use of Python context management facilities to ensure clean execution of multiple tasks in separate processes and/or threads.
It facilitates:

* simple switching of the [multi-tasked execution context][multi-tasking];
* transparent management of [execution context scopes of attributes][attribute-scope]

[multi-tasking]: ./multitask
[attribute-scope]: ./scope
