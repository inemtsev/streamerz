FROM gradle:8.14.3-jdk21-noble AS builder
WORKDIR /app
COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY gradle.properties .
COPY src ./src
RUN gradle build --no-daemon

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /app/build/libs/*.jar ./app.jar
EXPOSE 8081
CMD ["java", "-jar", "app.jar"]
