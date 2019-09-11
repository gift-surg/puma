## Multi-tasking with PUMA

PUMA encapsulates each task of application code as a ["runnable"][runnable].
Each runnable is executed by a ["runner"][runner], which provides the execution context; that is, whether the runnable should be run in a separate thread or a separate process.
PUMA provides ["buffers"][buffer] that serve:
* as a means of data exchange between; and
* as a control mechanism for sending commands to, and receiving error status and logging from runnables.

This architecture frees the user to concentrate on writing their application code instead of boilerplate code needed for managing the complexities of multi-tasking.

In the example below, the main program communicates with each runnable using the command and status buffers.
The consumer runnable consumes data produced and processed respectively by the producer and processing runnables.
Arrows indicate the direction of the flow of data on the respective buffers.

![PUMA example using multiple processes][example]

[example]: ../resources/example-multi-tasking.png

The application code can be very simply switched between multi-threaded and multi-processed operation, typically by changing a single line of code that sets the [multi-tasking operation context][context].

For how PUMA works in source code, please see this [simple demo application][demo].

[runnable]: ./runnable
[runner]: ./runner
[buffer]: ./buffer
[context]: ./operation
[demo]: ../demos/producer_consumer_pipeline/main.py
