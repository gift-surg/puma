## Execution Environments

In order to readily switch between multi-threaded and multi-processing versions of a program, the creation of runnables and other items can be delegated to an execution environment.
This can be either a `ThreadExecutionContext` or a `ProcessExecutionContext`, for multi-threaded and multi-processing respectively.
This means that changing a program from multi-threaded to multi-processing operation typically involves changing a single line – the one that instantiates that program's execution context.
However, there is no semantic difference between specifying [runners][runner] and [buffers][buffer] explicitly and using an execution environment instance.

[runner]: ../runner
[buffer]: ../buffer
