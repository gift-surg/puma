## Python Unified Multi-tasking API (PUMA)

PUMA provides a simultaneous multi-tasking framework that takes care of managing the complexities of executing and controlling multiple threads and/or processes.
PUMA encapsulates each task of application code as a "runnable" which can be run in a separate process or thread.
PUMA provides buffers for runnables to exchange data, plus mechanisms for sending commands to runnables and receiving error status and logging information from them.

The diagram below illustrates a multi-processing example application with three tasks that respectively produce, process, and consume data.

![PUMA multi-tasking example using multiple processes][example]

[example]: ./resources/example-multi-tasking.png

### Why another library for multi-tasking?

Python does provide built-in multi-threading and multi-processing facilities.
Arguably, writing programs with these facilities in Python is more straightforward than many other programming languages.
But because the [Global Interpreter Lock][gil] effectively limits concurrency when using multiple threads, a user who wants to harness the available computing power for a high-performance application needs to use multiple processes.
However, the common tasks of controlling launched processes, capturing errors from them, and allowing them to write to a single log output require writing boilerplate code.
This is error-prone and makes it easy to write programs that stop (deadlock), which end without explanation, or whose performance is extremely poor.

[gil]: https://wiki.python.org/moin/GlobalInterpreterLock

PUMA aims at freeing the user to concentrate on writing their application code, by providing a framework that takes care of:

* launching and controlling concurrent tasks;
* capturing errors from them; and
* maintaining a single log output for all concurrent tasks.

This separation of multi-tasking concerns naturally results in a loosely-coupled architecture with high cohesion.

### Installation

Install directly from GitHub using `pip install git+https://github.com/gift-surg/puma.git#egg=puma`

**or**

Clone this repository and run `pip install .` from within your local copy.

### Getting Started

For example source code demonstrating a data producer-processor-consumer pipeline as illustrated above, please see this [simple demo application][example-code].

For more detailed documentation as well as an in-depth discussion, please see the README file within each PUMA package.

[example-code]: demos/simple/main.py

### Acknowledgements

This project is grateful for the support from
the [Wellcome Trust][wt],
the [Engineering and Physical Sciences Research Council (EPSRC)][epsrc],
and
the [School of Biomedical Engineering and Imaging Sciences, King's College London][bmeis] at [King's College London (KCL)][kcl].

[wt]: https://wellcome.ac.uk/
[epsrc]: https://www.epsrc.ac.uk/
[kcl]: http://www.kcl.ac.uk
[bmeis]: https://www.kcl.ac.uk/lsm/research/divisions/imaging/index.aspx
