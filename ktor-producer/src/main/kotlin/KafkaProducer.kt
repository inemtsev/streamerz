package com.eventslooped

import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationStopped
import io.ktor.server.application.log
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.application
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import kotlinx.serialization.json.Json

fun Application.configureKafkaProducer() {
    val kafkaService = KafkaService()

    environment.monitor.subscribe(ApplicationStopped) {
        kafkaService.close()
    }

    routing {
        post("/produce") {
            val eventMessage = call.receive<EventMessage>()
            val messageJson = Json.encodeToString(eventMessage)

            val metadata = kafkaService.sendMessage("test-events", eventMessage.id, messageJson)

            call.respond(
                HttpStatusCode.OK,
                ProduceResponse(
                    success = true,
                    messageId = eventMessage.id
                )
            )

            application.log.info("Sent message to partition ${metadata.partition()} with offset ${metadata.offset()}")
        }

        post("/produce-fire-and-forget") {
            val eventMessage = call.receive<EventMessage>()
            val messageJson = Json.encodeToString(eventMessage)

            kafkaService.sendMessageFireAndForget("test-events", eventMessage.id, messageJson)

            call.respond(
                HttpStatusCode.OK,
                ProduceResponse(
                    success = true,
                    messageId = eventMessage.id
                )
            )

            application.log.info("Sent message (fire-and-forget) with id ${eventMessage.id}")
        }

        post("/produce-multiple") {
            val eventMessage = call.receive<EventMessage>()
            val messageJson = Json.encodeToString(eventMessage)

            kafkaService.sendMultipleMessages("test-events", eventMessage.id, messageJson)

            call.respond(
                HttpStatusCode.OK,
                ProduceResponse(
                    success = true,
                    messageId = eventMessage.id
                )
            )

            application.log.info("Sent message (fire-and-forget) with id ${eventMessage.id}")
        }

        get("/health") {
            call.respond(HttpStatusCode.OK, "OK")
        }
    }
}
