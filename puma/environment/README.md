## Execution Environments

In order to readily switch between multi-threaded and multi-processing versions of a program, the creation of runnables and other items can be delegated to an execution environment.
This can be either a `ThreadExecutionEnvironment` or a `ProcessExecutionEnvironment`, for multi-threaded and multi-processing respectively.
This means that changing a program from multi-threaded to multi-processing operation typically involves changing a single line – the one that instantiates that program's execution context.
However, there is no semantic difference between specifying [runners][runner] and [buffers][buffer] explicitly and using an execution environment instance.

[runner]: ../runner
[buffer]: ../buffer

### Rationale for the Choice of the Name

We use the term "execution environment" to refer to the environment where a "runnable" is executed; a separate thread or a separate process.
Arguably, the both terms are fairly broad and mean different things in different contexts.
An equivalent term could be "execution context"; however this would have led to even further confusion, as the word "context" is used primarily for the "resource context management" (which is why we favoured it for the PUMA [`context`][context] package).

The word "environment" is the most descriptive concise term we were able to come up with to denote this concept.
If you have other ideas about this terminology, please discuss these by opening a new issue.

[context]: ../context
