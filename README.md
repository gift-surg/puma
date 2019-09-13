## Python Unified Multi-tasking API (PUMA)

PUMA allows for seamless multi-tasking using processes or threads.
PUMA abstracts each independent task of an application as a separate execution stream, called a "runnable", which can be run in a separate process or thread.
PUMA provides buffers for runnables to exchange data, plus mechanisms for sending commands to runnables and receiving error status and logging information from them.
An example pipeline with three runnables that respectively produce, process and consume data is illustrated below.

![PUMA example using multiple processes][example]

[example]: ./resources/example-multi-tasking.png

### Why another library for multi-tasking?

Python does provide built-in multi-threading and multi-processing facilities.
Arguably, writing programs with these facilities in Python is more straightforward than many other programming languages.
But because the [Global Interpreter Lock][gil] effectively limits concurrency when using multiple threads, a user who wants to harness the available computing power for a high-performance application needs to use multiple processes.
However, the common tasks of controlling launched processes, capturing errors from them, and allowing them to write to a single log output require writing boilerplate code.
This is error-prone and makes it easy to write programs that stop (deadlock), which end without explanation, or whose performance is extremely poor.

[gil]: https://wiki.python.org/moin/GlobalInterpreterLock

PUMA aims at freeing the user to concentrate on writing their application code, by providing a framework that takes care of:
* launching and controlling concurrent tasks,
* capturing errors from them, and
* maintaining a single log output for all concurrent tasks.

This separation of multi-tasking concerns naturally results in a loosely-coupled architecture with high cohesion.

### Installation

Install directly from GitHub using `pip install git+https://github.com/gift-surg/puma.git#egg=puma`

**or**

Clone this repository and run `pip install .` from within your local copy.

### Quick start

An example demonstrating a data producer-processor-consumer pipeline as shown above is available [here][prod-proc-cons].

[prod-proc-cons]: ./demos/producer_consumer_pipeline/main.py

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
