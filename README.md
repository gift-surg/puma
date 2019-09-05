## Python Unified Multi-tasking API (PUMA)

PUMA allows for seamless multi-tasking using processes or threads.
Different application tasks are implemented as "runnables" each of which can be run in a separate process or thread.
PUMA provides buffers for runnables to exchange data, plus mechanisms for sending commands to runnables and receiving error status and logging information from them.
Below is an example pipeline with three runnables that produce, process and consume data.

![PUMA example using multiple processes][example]

[example]: resources/example.png

### Why another library for multi-tasking?

Python does provide built-in multi-threading and multi-processing facilities.
Arguably, using these is more straightforward than many other programming languages.
Because the [Global Interpreter Lock][gil] effectively limits concurrency when using multiple threads, the user who wants to harness the available computing power for a high-performance application needs to use multiple processes.
However, commonly encountered tasks of controlling launched processes, capturing errors from them, and allowing them to write to a single log file require writing boilerplate code.
In addition, effort needs to be invested to avoid writing programs that stop (deadlock), which end without explanation, or whose performance is extremely poor.

[gil]: https://wiki.python.org/moin/GlobalInterpreterLock

PUMA aims at freeing the user to concentrate on writing their application code, by providing a framework that takes care of:
* launching and controlling concurrent tasks,
* capturing errors from them, and
* maintaining a single log output for all concurrent tasks.

Thus abstracting and separating multi-tasking concerns naturally yield a loosely-coupled architecture with high cohesion.
