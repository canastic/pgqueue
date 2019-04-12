package pgqueue

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*

enum class Ack {
    OK,
    Requeue
}

class PGQueue<Subscription, Message>(
    internal val driver: Driver<Subscription, Message>
) {
    suspend fun subscribe(subscription: Subscription): SubscriptionHandle<Message> {
        driver.insertSubscription(subscription)

        return object : SubscriptionHandle<Message> {
            override suspend fun handle(handler: suspend (Message) -> Ack) {
                val consume: suspend (DeliveryListener<Message>) -> Unit = { deliveries ->
                    for (delivery in deliveries) {
                        val message = delivery.unwrap()
                        delivery.ack(handler(message))
                    }
                }

                val listener = driver.listenForMessages(subscription)
                driver.fetchPendingMessages(subscription).use { consume(it) }
                listener.use { consume(it) }
            }
        }
    }

    suspend fun publish(m: Message) {
        driver.publish(m)
    }
}

interface SubscriptionHandle<Message> {
    suspend fun handle(handler: suspend (Message) -> Ack)
}

interface Driver<Subscription, Message> {
    suspend fun insertSubscription(s: Subscription)
    suspend fun listenForMessages(s: Subscription): DeliveryListener<Message>
    suspend fun fetchPendingMessages(s: Subscription): DeliveryListener<Message>
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
            this == null -> {}
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
