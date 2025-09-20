package com.eventslooped

import kotlinx.coroutines.suspendCancellableCoroutine
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import kotlin.coroutines.resumeWithException

suspend inline fun <reified K : Any, reified V : Any> KafkaProducer<K, V>.dispatch(
    record: ProducerRecord<K, V>
): RecordMetadata = suspendCancellableCoroutine { continuation ->
    val callback = Callback { metadata, exception ->
        when {
            exception != null -> continuation.resumeWithException(exception)
            metadata != null -> continuation.resume(metadata)
            else -> continuation.resumeWithException(RuntimeException("Unknown error occurred"))
        }
    }

    val future = this.send(record, callback)
    continuation.invokeOnCancellation {
        future.cancel(true)
    }
}