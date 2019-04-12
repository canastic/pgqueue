package pgqueue

import io.mockk.*
import kotlin.test.Test
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlin.test.assertEquals

class PGQueueTest {
    @Test
    fun testSubscribe() = runBlocking {
        val driver = mockk<Driver<String, Message>>()
        val queue = PGQueue(driver)

        // Subscribing should call subscribe on the driver.

        coEvery { driver.insertSubscription(any()) } answers {}

        val handles = listOf("subA", "subB").map { sub ->
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
            val handle = handles[sub]!!
            val handled = Channel<Pair<String, SendChannel<Ack>>>()

            val handler = launch {
                // Our handler will send us the received payload and a channel
                // to send the ACK we want.
                handle.handle { msg ->
                    val ack = Channel<Ack>()
                    handled.send(msg.payload to (ack as SendChannel<Ack>))
                    ack.receive()
                }
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

            // If the incoming DeliveryListener is closed, handle should return.
            soon { handler.join() }
        }
    }

    @Test
    fun testPublish() = runBlocking {
        val driver = mockk<Driver<Unit, Int>>()
        val queue = PGQueue(driver)

        coEvery { driver.publish(any()) } answers {}

        queue.publish(123)

        coVerify { driver.publish(123) }
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

@Throws(TimeoutCancellationException::class)
private suspend fun <T> soon(block: suspend () -> T): T =
    withTimeout(50L) { block() }
