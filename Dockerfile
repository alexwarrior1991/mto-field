# syntax=docker/dockerfile:1.7

# La etapa de construccion NO es Alpine a proposito: protobuf-maven-plugin descarga y ejecuta
# protoc-gen-grpc-java, un binario Linux enlazado contra glibc, y en musl protoc responde
# "program not found or is not executable". La etapa de ejecucion si es Alpine: el jar no
# ejecuta ningun binario nativo.
FROM maven:3.9-eclipse-temurin-25 AS build
WORKDIR /workspace

COPY pom.xml .
RUN --mount=type=cache,target=/root/.m2 mvn -B -DskipTests dependency:go-offline

COPY src ./src
RUN --mount=type=cache,target=/root/.m2 mvn -B -DskipTests package

FROM eclipse-temurin:25-jre-alpine

RUN addgroup -S mtofield \
    && adduser -S mtofield -G mtofield \
    && apk add --no-cache curl

WORKDIR /app
COPY --from=build /workspace/target/mto-field-*.jar /app/app.jar

# 8080 es Actuator (Tomcat); 9090, el servidor gRPC (Netty), que es la API.
ENV SPRING_PROFILES_ACTIVE=prod \
    SERVER_PORT=8080 \
    SPRING_GRPC_SERVER_PORT=9090 \
    LOGGING_LEVEL_ROOT=INFO \
    JAVA_OPTS=""

EXPOSE 8080 9090

USER mtofield
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /app/app.jar"]
