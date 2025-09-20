package com.eventslooped

import kotlinx.serialization.Serializable

@Serializable
data class ProduceResponse(
    val success: Boolean,
    val messageId: String? = null,
    val error: String? = null
)

@Serializable
data class EventMessage(
    val id: String,
    val timeStamp: Long,
    val data: String,
    val source: String = "Kafka-producer"
)