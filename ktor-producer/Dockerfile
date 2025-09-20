FROM gradle:8.14.3-jdk21-noble AS builder
WORKDIR /app
COPY build.gradle.kts .
COPY src ./src
RUN gradle build --no-daemon

FROM ubuntu/jre:21-24.04_stable
WORKDIR /app
COPY --from=builder /app/build/libs/*.jar ./app.jar
EXPOSE 8080
CMD ["java", "-jar", "app.jar"]
