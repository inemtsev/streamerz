package com.eventslooped

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

class KafkaService {
    private val producer: KafkaProducer<String, String>

    init {
        val kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092"
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
            put(ProducerConfig.CLIENT_ID_CONFIG, "ktor-kafka-producer") // identifier for this producer, helps with logs/metrics
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.ACKS_CONFIG, "all") // how many brokers need to acknowledge a write before success
            put(ProducerConfig.RETRIES_CONFIG, 3)
            put(ProducerConfig.LINGER_MS_CONFIG, 500)  // how long to wait before sending a batch of messages even if batch isn't filled
            put(ProducerConfig.BATCH_SIZE_CONFIG, 8192)  // size of batch per send
        }

        producer = KafkaProducer(props)
    }

    suspend fun sendMessage(topic: String, key: String, message: String) : RecordMetadata {
        val record = ProducerRecord(topic, key, message)
        return producer.dispatch(record)
    }

    suspend fun sendMultipleMessages(topic: String, key: String, message: String) : List<RecordMetadata> = coroutineScope {
        val routines = List(5) {
            val record = ProducerRecord(topic, key, message)
            async { producer.dispatch(record) }
        }

        routines.awaitAll()
    }
    
    fun sendMessageFireAndForget(topic: String, key: String, message: String) {
        val record = ProducerRecord(topic, key, message)
        producer.send(record)
    }
    
    fun close() = producer.close()
}
