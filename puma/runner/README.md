## Runners

Application code encapsulated in a ["runnable"][runnable] is executed by passing it as a constructor parameter to a "runner".
As illustrated below, PUMA provides two runner implementations: `ProcessRunner` and `ThreadRunner`.

![Relation of runners to runnables][runners-runnables]

[runners-runnables]: ../../resources/runners-and-runnables.png

These "runners" construct the command and status buffers needed for controlling runners and checking their (error) status as illustrated in the [introduction][puma], and they take on most of the burden of error handling and logging support.
However, the creator of the runnable is still responsible for polling it for errors while it is running.

[runnable]: ../runnable
[puma]: ../

### Error handling in runners and runnables 

The following design principles were adopted when implementing the default error handling in PUMA code, and user code should try to adhere to these same principles:

* User code is responsible for polling every runner it creates for errors, at regular intervals.
Although errors will be reported when a runnable goes out of context management, it is bad practice to rely on this mechanism, because there will be no reason for the program to shut down and the runnable to go out of context if the error is not detected.
* Errors that occur in a runner or a runnable kill the runner and runnable, in an orderly fashion.
* Errors are passed to every subscription that has not already received `on_complete`, where they should be sent out on all output buffers whenever possible.
In this way, errors propagate through the system, ending downstream runners, and are finally handled at the ultimate data destination (which might, for example, be a user interface).
A runner will only re-raise an error if it cannot pass the error to any of its subscriptions (either because they have all already received `on_complete`, or because of another error).
* An error arriving on a runnable's input buffer is treated as fatal error and passed out to all subscribers as described above.

### `Multicaster`

`Multicaster` is a special `ThreadRunner` that takes data from one input buffer and copies it to multiple output buffers, as illustrated below.

![`Multicaster` for copying data to multiple output buffers][multicaster]

[multicaster]: ../../resources/multicaster.png

This is a reusable system component for when data needs to go to several destinations, for example if it is both processed and visualised.
