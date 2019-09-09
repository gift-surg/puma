## Multi-tasking with PUMA

PUMA enables application code to be executed in either a multi-threaded or multi-processing manner.
The application code can be conceptualised as different components which will typically produce, process or consume data packets.
These data packets are communicated between the application components through buffers, as illustrated in the example below.

![PUMA multi-tasking example using multiple processes][example]

[example]: ../../resources/example-multi-tasking.png

In PUMA we refer to each such component as a ["runnable"][runnable].
Each runnable is executed by a ["runner"][runner], which provides the execution context; that is, whether the runnable should be run in a separate thread or a separate process.
PUMA provides the [buffers][buffer] for the runnables to communicate, plus mechanisms to send commands to the runnables and to receive error status and logging from them, freeing the user to concentrate on writing their application code.
The application code can be very simply switched between multi-threaded and multi-processed operation, typically by changing a single line of code that sets the execution [context][context].

[runnable]: ./runnable
[runner]: ./runner
[buffer]: ./buffer
[context]: ./context
