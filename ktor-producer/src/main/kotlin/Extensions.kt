package com.eventslooped

import kotlinx.coroutines.suspendCancellableCoroutine
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata

suspend fun <K : Any, V : Any> KafkaProducer<K, V>.dispatch(
    record: ProducerRecord<K, V>
): RecordMetadata = suspendCancellableCoroutine { continuation ->
    val callback = Callback { metadata, exception ->
        when {
            exception != null -> continuation.resumeWith(Result.failure(exception))
            metadata != null -> continuation.resumeWith(Result.success(metadata))
            else -> continuation.resumeWith(Result.failure(RuntimeException("Unknown error occurred")))
        }
    }

    val future = this.send(record, callback)
    continuation.invokeOnCancellation {
        future.cancel(true)
    }
}

