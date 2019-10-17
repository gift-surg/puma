## Resource Context Management

The word "context" has a broad meaning spectrum which can be confusing.
We use the term "context management" to refer to what the [`with` statement in Python does][pep343].
Arguably, the `with` statement manages the context for resources, so it can be qualified as "resource context management".

PUMA makes use of Python resource context management facilities to ensure clean execution of multiple tasks in separate processes and/or threads.
It provides utility classes and decorators to enforce the use of resource contexts wherever applicable.

[pep343]: https://www.python.org/dev/peps/pep-0343/
