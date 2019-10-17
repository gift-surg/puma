## Motivation

Python provides queues, which communicate data between threads or processes.
The API for popping an item off a Python queue is a blocking call with a timeout.
Unfortunately, this is not ideal if the program needs to service several queues (such as data arriving from several sources, or a data queue and a command queue): the user code must either poll each queue in rapid succession with short timeouts, or must accept long latency while the code gets round to processing each queue in turn.

There is also a problem of how to stop a thread or process when no more data is coming: a special "sentinel" value could be pushed to the queue, but this is difficult to reconcile with strong type checking.

To solve these issues, we have wrapped Python's queues in a different API, called `Buffer`.
As illustrated in the figure below, this allows processing to be suspended until there is data to be processed or a command to be executed, at which point the thread or process is woken.
By passing the same "event" object to all the input buffers, polling is avoided, and latency is eliminated. 

![Servicing multiple buffers without polling][buffer-servicing]

[buffer-servicing]: ../../resources/buffer-without-polling.png

## Buffers

A PUMA `Buffer` is a FIFO class that implements two interfaces, `Publishable` and `Observable`.
`Publishable` is used to publish data to the buffer, while `Observable` is used to receive that data. 

![Buffer API for data exchange][buffer-api]

[buffer-api]: ../../resources/buffer-api.png

`Publishable` has a single method, `publish`, which returns a `Publisher`, which is used to push data to the buffer.

`Observable` has a single method, `subscribe`, which takes an event variable. This method returns a `Subscription` object.
When data is pushed to the buffer by the publisher's `publish_value` method, the event will be set and data will be available by calling the subscription's `call_events` method.

When no more data is going to be published, `publish_complete` can be called, optionally taking an error (exception) which will be transported to the subscription.

Buffers can have multiple publishers but only one subscription.

The two-stage `Publishable` / `Publisher` and `Observable` / `Subscription` interface was adopted to support a context-managed approach to buffer usage, allowing them to be cleanly shut down when no longer required.

When the buffer has no publishers and no subscribers, it launches an internal thread called the "discard thread" which will delete any data in the buffer after a few seconds
(The rationale for this is explained later in this document).

There are two types of buffer which implement the `Buffer` interface, as illustrated in the figure below.

![Buffer class hierarchy][buffers]

[buffers]: ../../resources/buffer-inheritance.png

### Discard thread

The multi-processing queue in python (`multiprocessing.Queue`) contains a hidden thread which transports items from the source end of the queue to the "pipe" that transports the data across processes.
If this pipe is full, the thread blocks until space is available.
In this situation, a process that has pushed data onto the queue cannot end; the process explicitly [joins on the hidden thread][queue-join] (even though it's a daemon thread), and that blocks forever.

[queue-join]: https://github.com/python/cpython/blob/0461704060474cb358d3495322950c4fd00616a0/Lib/concurrent/futures/process.py#L662gets 

This situation only happens in practice if the items pushed to the buffer are quite large, or very numerous.
For a small number of small items, if the intended buffer consumer dies and publisher then moves on, the queue eventually gets collected.
So the thread ends cleanly, only the data is lost in the pipe this way.

Of course, the publisher cannot know if the data is ever going to be popped from the queue; the subscriber may simply be slow, or it may have exited (or died) and the data will never be popped.

Top solve this issue, a "discard thread" was implemented.
When a publisher or subscriber disconnects, if there are no remaining publishers or subscribers and the buffer is not empty, then a "discard thread" is launched.
This might happen at either end of the buffer, depending which end was unpublished / unsubscribed last, but it will only happen at one end. 

The discard thread sleeps for a few seconds and then deletes any items from the buffer.

If the buffer is published to or subscribed to during the sleep, the discard thread is woken and cancelled.

The reason for waiting a few seconds is to prevent loss of data in some legitimate use cases, such as pushing and then popping.
By default the timeout if 5 seconds, except for the multi-processing buffer on Windows where it is 15 seconds.
This is because python processes take several seconds to start on Windows.
