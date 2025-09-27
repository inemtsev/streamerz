package com.eventslooped

import io.ktor.http.ContentType
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationStopped
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel

fun Application.configureKafkaConsumer() {
    val consumerScope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    val kafkaService = KafkaService(consumerScope)

    // Start consuming immediately when the application starts
    kafkaService.startConsuming("test-events")

    environment.monitor.subscribe(ApplicationStopped) {
        kafkaService.stop()
        consumerScope.cancel()
    }

    routing {
        get("/stats") {
            call.respond(kafkaService.getStats())
        }

        get("/health") {
            call.respondText("Consumer is healthy", ContentType.Text.Plain)
        }
    }
}