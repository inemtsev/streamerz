package com.eventslooped

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

class KafkaService {
    private val producer: KafkaProducer<String, String>

    init {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
            put(ProducerConfig.CLIENT_ID_CONFIG, "ktor-kafka-producer")
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.RETRIES_CONFIG, 3)
            put(ProducerConfig.LINGER_MS_CONFIG, 1)
            put(ProducerConfig.BATCH_SIZE_CONFIG, 16384)
        }

        producer = KafkaProducer(props)
    }

    suspend fun sendMessage(topic: String, key: String, message: String) : RecordMetadata {
        val record = ProducerRecord(topic, key, message)
        return producer.dispatch(record)
    }

    fun close() = producer.close()
}
