package com.eventslooped

import kotlinx.serialization.Serializable

@Serializable
data class EventMessage(
    val id: String,
    val timestamp: Long,
    val data: String,
    val source: String
)

@Serializable
data class ConsumerStats(
    val messagesProcessed: Long,
    val isRunning: Boolean,
    val lastProcessedTime: Long?
)
