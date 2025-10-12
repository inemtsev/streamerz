package com.eventslooped

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.time.Duration
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.util.concurrent.atomic.AtomicLong

class KafkaService(private val scope: CoroutineScope) {
    private val consumer: KafkaConsumer<String, String>
    private val messagesProcessed = AtomicLong(0)
    private var lastProcessedTime = 0L
    private var isRunning = false

    private val outputFile = File("kafka_messages.log")
    private var fileWriter: BufferedWriter? = null

    init {
        val kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092"
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, "ktor-consumer-group")
            put(ConsumerConfig.CLIENT_ID_CONFIG, "ktor-consumer")
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100) // max number of messages/records in one poll
        }

        consumer = KafkaConsumer(props)
    }

    fun startConsuming(topic: String) {
        if (isRunning) return

        scope.launch {
            isRunning = true
            consumer.subscribe(listOf(topic))

            fileWriter = BufferedWriter(FileWriter(outputFile, true))

            try {
                while (isActive && isRunning) {
                    val records = consumer.poll(Duration.ofMillis(100))

                    if (records.count() > 0) {
                        processRecords(records)
                    }
                }
            } catch (e: Exception) {
                println("Error while consuming messages: ${e.message}")
            } finally {
                consumer.close()
                fileWriter?.close()
                isRunning = false
            }
        }
    }

    private suspend fun processRecords(records: ConsumerRecords<String, String>) {
        records.forEach { record ->
            try {
                processMessage(record)
                messagesProcessed.incrementAndGet()
                lastProcessedTime = System.currentTimeMillis()
            } catch (e: Exception) {
                println("Error while processing message: ${e.message}")
            }
        }

        consumer.commitAsync { offsets, exception ->
            if (exception != null) {
                println("Error while committing offsets: ${exception.message}")
            } else {
                println("Offsets committed successfully, no: ${offsets.count()}")
            }
        }
    }

    private fun processMessage(record: ConsumerRecord<String, String>) {
        val eventsMessage = Json.decodeFromString<EventMessage>(record.value())
        val currentTime = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)

        val logEntry = """
                $currentTime | Topic: ${record.topic()} | Partition: ${record.partition()} | Offset: ${record.offset()} | Key: ${record.key() ?: "null"} | Message: {
                  id=${eventsMessage.id}, 
                  timestamp=${eventsMessage.timestamp}, 
                  data="${eventsMessage.data}", 
                  source="${eventsMessage.source}"
                }
            """.trimIndent()

        fileWriter?.let { writer ->
            writer.write(logEntry)
            writer.newLine()
            writer.flush()
        }

        println("Message processed and logged: ${eventsMessage.id}")
    }

    fun getStats() = ConsumerStats(
        messagesProcessed = messagesProcessed.get(),
        isRunning = isRunning,
        lastProcessedTime = lastProcessedTime
    )

    fun stop() {
        isRunning = false
        fileWriter?.close()
        fileWriter = null
    }
}