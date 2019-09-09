## Runnables

`Runnable` is an abstract base class with a single method, `_execute()`, that must be implemented, although user-defined classes will not typically implement this directly.
Instead, user-defined runnables will typically derive from one of the standard runnables as shown below, which are described in the following section.

![Standard runners and runnables provided by PUMA][standard-runnables]

[standard-runnables]: ../../../resources/runners-and-runnables.png

It is the runnable's responsibility to respond to commands as well as to input values.
As a minimum, it should check for the `Stop` command.
The standard runnables described below contain a default implementation for this command.
The [`runner` package][runner] describes how each runnable is executed in the proper context using a `Runner`.

[runner]: ../runner

### Standard runnables

This section describes useful general-purpose runnables provided in PUMA.
The handling of errors that occur in runnable is described in the [`runner` package][runner].

#### `MultiBufferServicingRunnable`

A user runnable derived from `MultiBufferServicingRunnable` is illustrated below.

![`MultiBufferServicingRunnable` example][example-multi-buffer-servicing-runnable]

[example-multi-buffer-servicing-runnable]: ../../../resources/MultiBufferServicingRunnable.png

The `MultiBufferServicingRunnable` class handles responding to commands and to input data on buffers, routing this input data to user-defined "subscriptions", each of which processes the relevant data and, in the example shown, outputs the results on an output buffer.

An instance of `MultiBufferServicingRunnable` is constructed to take several input buffers.
For each input buffer a "subscription" is registered, which handles data arriving on that buffer.
As illustrated in the example above, it is legal to configure the runnable such that multiple input buffers are handled by a single subscription.
The user code can also respond to bespoke commands.

The `MultiBufferServicingRunnable` class contains default handling of the `Stop` command and of error handling.
Its `_execute()` method runs until one of the following conditions is met: 

* a `Stop` command is received; or
* one of its input buffers receives the "complete" message; or
* an error has occurred in the runnable itself (for instance, in the user's subscription code handling a message)

This runnable, and hence the other useful runnables described below, also has a "regular ticking" functionality, see below.

#### `SingleBufferServicingRunnable`

`SingleBufferServicingRunnable` is simply a special case of `MultiBufferServicingRunnable` which has only one input buffer, as illustrated below.

![`SingleBufferServicingRunnable` example][example-single-buffer-servicing-runnable]

[example-single-buffer-servicing-runnable]: ../../../resources/SingleBufferServicingRunnable.png

#### `CommandDrivenRunnable`

`CommandDrivenRunnable` is simply a special case of `MultiBufferServicingRunnable` which has no input buffers (and hence no output subscriptions), and therefore only responds to commands, as illustrated below.

![`CommandDrivenRunnable` example][example-command-driven-runnable]

[example-command-driven-runnable]: ../../../resources/CommandDrivenRunnable.png

In this example, commands are illustrated as producing some output to an output buffer.
An example might be a data source which is reading data from file, and responds to commands such as `Pause` and `Resume`.

Despite the different naming, there is nothing different about the handling of commands in `CommandDrivenRunnable` as compared to `MultiBufferServicingRunnable` and `SingleBufferServicingRunnable`.
All three runnables handle the `Stop` command and expect user code to handle other commands. 

### Command handling in runnables

In order to send commands to a Runnable, the command needs to be wrapped in a `Command` object, serialised, sent on the command buffer, de-serialised, and routed to either the default code (for the `Stop` command) or to user code.
The command buffer abstracts the switching of the execution context.
Below is an example using multiple threads.

![Runnable command handling in a multi-threaded example][example-runnable-command-handling]

[example-runnable-command-handling]: ../../../resources/run_in_child_context.png

In this example, a user of the runnable (for example, the program's main thread) calls `runnable.pause()`.
This gets converted to a command which eventually calls the code `self._paused = True` in the runnable, on the runnable's thread (or process if multiple processes were being used instead of threads).

To simplify the execution context switching as much as possible for the user, a decorator, `@run_in_child_context`, has been created.
The use of this decorator is illustrated in the below code snippet, where it is applied to a method of a class derived from `Runnable`:

```python
@run_in_child_context 
def pause(self) -> None: 
    self._paused = True
```

### Regular ticking in runnables

The runnables described above include a facility to call user code at regular intervals.
This might, for example, be used to play back recorded data at a certain rate.

The API for this facility consists of the following: 

* The runnable's constructor includes an optional parameter, `tick_interval`, which specifies the time between "ticks", in seconds.
Ticking starts after `resume_ticks()` has been called. 
* The method `set_tick_interval()` allows the interval to be modified, or set if it was not set in the constructor.
* `resume_ticks()` must be called in order to begin ticking.
The first tick occurs one interval after this is called.
* `pause_ticks()` pauses ticking.
* `_on_tick()` is called at every interval; this should be overridden in derived classes that use the ticking facility.
This method provides a timestamp which can be used if required.

#### Timestamps

The timestamp supplied to `_on_tick()` is obtained by a call to `precision_timestamp()`.
This provides a timestamp that:

* has a precision of at least one millisecond;
* is unaffected by changes to the "wall time";
* is system-wide, i.e. it is the same in all threads and processes;
* is monotonically increasing, unless the machine is rebooted.

The value is in seconds.
The "epoch" (zero time) is undefined and might be reset if the computer is rebooted.
That is, it does not define a point in time; but rather is useful when comparing two subsequent timestamps with a precision of at least one millisecond.
