package pgqueue

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.SelectBuilder
import kotlinx.coroutines.selects.select
import java.util.concurrent.atomic.AtomicBoolean

enum class Ack {
    OK,
    Requeue
}

class PGSubscriber<Subscription, Message>(
    internal val driver: SubscriberDriver<Subscription, Message>
) {
    @ExperimentalCoroutinesApi
    suspend fun subscribe(subscription: Subscription): Consumer<Message> {
        driver.insertSubscription(subscription)

        return object : Consumer<Message> {
            override suspend fun start(scope: CoroutineScope, handle: suspend (Message) -> Ack): Stoppable =
                handleExceptions().start(
                    scope,
                    object : Handler<Message> {
                        override suspend fun handleMessage(m: Message): Ack = handle(m)
                        override suspend fun handleException(t: Throwable) {}
                    }
                )

            override fun handleExceptions(): StartStoppable<Handler<Message>> = StartStoppable { scope, handler ->
                scope.launchStoppable { stop ->
                    suspend fun consume(deliveries: DeliveryListener<Message>) =
                        selectWhileNotStopped(stop) { doneSelecting ->
                            deliveries.onReceiveOrNull { delivery ->
                                if (delivery == null) {
                                    throw doneSelecting
                                }

                                var ack: Ack? = null
                                try {
                                    val message = delivery.unwrap()
                                    ack = handler.handleMessage(message)
                                } catch (t: Throwable) {
                                    handler.handleException(t)
                                } finally {
                                    delivery.ack(ack ?: Ack.Requeue)
                                }
                            }
                        }

                    val listener = driver.listenForMessages(subscription)
                    driver.fetchPendingMessages(subscription).use { consume(it) }
                    listener.use { consume(it) }
                }
            }
        }
    }
}

internal object doneSelecting : Throwable()

@ExperimentalCoroutinesApi
suspend fun CoroutineScope.selectWhileNotStopped(
    stop: ReceiveChannel<SendChannel<Unit>>,
    block: SelectBuilder<Unit>.(Throwable) -> Unit
) {
    try {
        while (true) {
            select<Unit> {
                stop.onReceiveOrNull { resp ->
                    resp?.send(Unit)
                    throw doneSelecting
                }
                block(doneSelecting)
            }
        }
    } catch (t: doneSelecting) {
    }
}

suspend fun CoroutineScope.launchStoppable(block: suspend CoroutineScope.(ReceiveChannel<SendChannel<Unit>>) -> Unit): Stoppable {
    val done = AtomicBoolean(false)
    val stop = Channel<Channel<Unit>>()

    launch {
        try {
            block(stop)
        } finally {
            done.set(true)
        }
    }

    return asStop {
        if (!done.get()) {
            stop.roundTrip()
            stop.close()
        }
    }
}

suspend fun <T, Req : ReceiveChannel<T>, Ch : SendChannel<Req>> Ch.roundTrip(request: Req): T {
    send(request)
    return request.receive()
}

suspend fun <T, Ch : SendChannel<Channel<T>>> Ch.roundTrip(): T =
    roundTrip(Channel())

class PGPublisher<Message>(
    internal val driver: PublisherDriver<Message>
) {
    suspend fun publish(m: Message) {
        driver.publish(m)
    }
}

interface Stoppable {
    suspend fun stop()
}

suspend operator fun Stoppable.invoke() = stop()

suspend fun asStop(f: (suspend () -> Unit)): Stoppable = object : Stoppable {
    override suspend fun stop() = f()
}

interface StartStoppable<in T> {
    suspend fun start(scope: CoroutineScope, t: T): Stoppable

    companion object {
        operator fun <T> invoke(block: suspend (CoroutineScope, T) -> Stoppable): StartStoppable<T> =
            object : StartStoppable<T> {
                override suspend fun start(scope: CoroutineScope, t: T): Stoppable = block(scope, t)
            }
    }
}

suspend fun <T> CoroutineScope.start(s: StartStoppable<T>, t: T): Stoppable =
    s.start(this, t)

suspend fun CoroutineScope.start(s: StartStoppable<Unit>): Stoppable =
    s.start(this, Unit)

interface Handler<Message> {
    suspend fun handleMessage(m: Message): Ack
    suspend fun handleException(t: Throwable)
}

interface Consumer<Message> : StartStoppable<suspend (Message) -> Ack> {
    fun handleExceptions(): StartStoppable<Handler<Message>>
}

interface SubscriberDriver<Subscription, Message> {
    suspend fun insertSubscription(s: Subscription)
    suspend fun listenForMessages(s: Subscription): DeliveryListener<Message>
    suspend fun fetchPendingMessages(s: Subscription): DeliveryListener<Message>
}

interface PublisherDriver<Message> {
    suspend fun publish(m: Message)
}

interface DeliveryListener<out Message> : SuspendCloseable, ReceiveChannel<Delivery<Message>>

interface Delivery<out Message> {
    suspend fun unwrap(): Message
    suspend fun ack(ack: Ack)
}

interface SuspendCloseable {
    suspend fun close()

    companion object {
        inline operator fun invoke(crossinline op: suspend () -> Unit) =
            object : SuspendCloseable {
                override suspend fun close() = op()
            }
    }
}

suspend inline fun <T : SuspendCloseable?, R> T.use(block: (T) -> R): R {
    var exception: Throwable? = null
    try {
        return block(this)
    } catch (e: Throwable) {
        exception = e
        throw e
    } finally {
        when {
            this == null -> Unit
            exception == null -> close()
            else ->
                try {
                    close()
                } catch (closeException: Throwable) {
                    exception.addSuppressed(closeException)
                }
        }
    }
}
