package pgqueue

import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.map
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals

class PGQueueTest {
    @Test
    fun testSubscribe() = runBlocking {
        val driver = mockk<SubscriberDriver<String, Message>>()
        val queue = PGSubscriber(driver)

        // Subscribing should call subscribe on the driver.

        coEvery { driver.insertSubscription(any()) } answers {}

        val consumers = listOf("subA", "subB").map { sub ->
            sub to queue.subscribe(sub).also {
                coVerify { driver.insertSubscription(sub) }
            }
        }.toMap()

        // Create channels that we'll send fixture messages through. pending
        // represent messages that are already in the queue when you subscribe,
        // and incoming, messages that come in once you're subscribed.

        val pending = mapOf("subA" to Channel<Pair<String, Ack>>(UNLIMITED), "subB" to Channel(UNLIMITED))
        val incoming = mapOf("subA" to Channel<Pair<String, Ack>>(UNLIMITED), "subB" to Channel(UNLIMITED))

        // Map those channels to the DeliveryListeners that our mock driver will
        // return to the PGQueue subscription handler. Get the ACKs and closes
        // that the handler performs through channels.

        val acksBySub = HashMap<String, Channel<Pair<String, Ack>>>()
        val closesBySub = HashMap<String, Channel<Unit>>()

        for (sub in listOf("subA", "subB")) {
            val acks = Channel<Pair<String, Ack>>().also { acksBySub[sub] = it }
            val closes = Channel<Unit>().also { closesBySub[sub] = it }
            coEvery { driver.listenForMessages(sub) } returns incoming[sub]!!.toDeliveryListener(acks, closes)
            coEvery { driver.fetchPendingMessages(sub) } returns pending[sub]!!.toDeliveryListener(acks, closes)
        }

        // "Publish" a bunch of messages, in random order, to both listeners.
        // We'll check that they're processed in the right order (first pending,
        // then incoming), with the payload and ACk we expect.

        val ops = listOf(
            Op("subA", OpSource.Incoming, Ack.OK),
            Op("subA", OpSource.Pending, Ack.Requeue),
            Op("subB", OpSource.Pending, Ack.Requeue),
            Op("subB", OpSource.Incoming, Ack.OK),
            Op("subB", OpSource.Incoming, Ack.OK),
            Op("subA", OpSource.Pending, Ack.Requeue),
            Op("subB", OpSource.Incoming, Ack.Requeue),
            Op("subB", OpSource.Pending, Ack.OK)
        )

        ops.withIndex().forEach { (i, op) ->
            val (sub, source, ack, _) = op
            op.msg = "msg $i -> $sub.$source ($ack)"
            when (source) {
                OpSource.Incoming -> incoming
                OpSource.Pending -> pending
            }[sub]!!.send(op.msg to ack)
        }

        for (sub in listOf("subA", "subB")) {
            val consumer = consumers[sub]!!
            val handled = Channel<Pair<String, SendChannel<Ack>>>()

            val stopConsumer = start(consumer) { msg ->
                // Our handler will send us the received payload and a channel
                // to send the ACK we want.
                val ack = Channel<Ack>()
                handled.send(msg.payload to (ack as SendChannel<Ack>))
                ack.receive()
            }

            val consume: suspend (Iterable<Op>).() -> Unit = {
                for (op in this.filter { it.sub == sub }) {
                    val (payload, ack) = soon { handled.receive() }
                    assertEquals(op.msg, payload)
                    soon { ack.send(op.ack) }

                    val (ackedPayload, acked) = soon { acksBySub[sub]!!.receive() }
                    assertEquals(payload, ackedPayload)
                    assertEquals(op.ack, acked)
                }
            }

            ops.filter { it.source == OpSource.Pending }.consume()

            // Closing pending should stop its DeliveryListener.
            pending[sub]!!.close()
            soon { closesBySub[sub]!!.receive() }

            ops.filter { it.source == OpSource.Incoming }.consume()

            incoming[sub]!!.close()
            soon { closesBySub[sub]!!.receive() }

            stopConsumer()
        }
    }

    @Test
    fun testPublish() = runBlocking {
        val driver = mockk<PublisherDriver<Int>>()
        val queue = PGPublisher(driver)

        coEvery { driver.publish(any()) } answers {}

        queue.publish(123)

        coVerify { driver.publish(123) }
    }

    @Test
    fun testSubscribe_requeue_on_crash() = runBlocking {
        val driver = mockk<SubscriberDriver<String, Int>>()
        val queue = PGSubscriber(driver)

        coEvery { driver.insertSubscription("sub") } answers {}

        val deliveries = Channel<Delivery<Int>>()

        coEvery { driver.fetchPendingMessages("sub") } returns deliveries.toDeliveryListener()
        coEvery { driver.listenForMessages("sub") } returns deliveries.toDeliveryListener()

        val stopHandler = start(queue.subscribe("sub")) {
            throw Exception("oops")
        }

        val gotAck = Channel<Ack>()

        (0 until 2).forEach {
            soon {
                deliveries.send(object : Delivery<Int> {
                    override suspend fun unwrap(): Int = 123
                    override suspend fun ack(ack: Ack) {
                        gotAck.send(ack)
                    }
                })
            }

            assertEquals(Ack.Requeue, soon { gotAck.receive() })
        }

        soon { stopHandler() }
    }
}

private data class Message(val payload: String)
enum class OpSource { Incoming, Pending }
data class Op(val sub: String, val source: OpSource, val ack: Ack, var msg: String = "")

private fun Pair<String, Ack>.toDelivery(
    onAck: suspend (Ack) -> Unit
): Delivery<Message> =
    let { (payload, _) ->
        object : Delivery<Message> {
            override suspend fun unwrap(): Message = Message(payload)
            override suspend fun ack(ack: Ack) = onAck(ack)
        }
    }

private fun Channel<Pair<String, Ack>>.toDeliveryListener(
    onAck: SendChannel<Pair<String, Ack>>,
    onClose: SendChannel<Unit>
): DeliveryListener<Message> {
    val closer = SuspendCloseable {
        onClose.send(Unit)
    }
    val deliveries = this.map {
        val (payload, _) = it
        it.toDelivery { ack ->
            onAck.send(payload to ack)
        }
    }
    return object :
        DeliveryListener<Message>,
        SuspendCloseable by (closer),
        ReceiveChannel<Delivery<Message>> by (deliveries) {}
}

private fun <T> ReceiveChannel<Delivery<T>>.toDeliveryListener(closer: SuspendCloseable = SuspendCloseable {}): DeliveryListener<T> =
    object :
        DeliveryListener<T>,
        SuspendCloseable by (closer),
        ReceiveChannel<Delivery<T>> by (this) {}

@Throws(TimeoutCancellationException::class)
private suspend fun <T> soon(block: suspend () -> T): T =
    keepStack { withTimeout(50L) { block() } }

internal suspend fun <T> keepStack(block: suspend () -> T): T = stackChainer().let { chain ->
    try {
        block()
    } catch (t: Throwable) {
        throw chain(t)
    }
}

fun stackChainer(): (Throwable) -> Throwable {
    val original = Throwable()
    return { t ->
        RuntimeException("exception caught while handling async operation result: $t", t).apply {
            stackTrace = original.stackTrace
        }
    }
}
