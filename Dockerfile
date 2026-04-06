FROM maven:3.9-eclipse-temurin-25 AS builder

WORKDIR /build

COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn package -DskipTests -q


FROM eclipse-temurin:25-jre-noble

WORKDIR /app

COPY --from=builder /build/target/app.jar /app/app.jar

ENTRYPOINT ["java", \
    "-XX:+UseG1GC", \
    "-XX:MaxRAMPercentage=75", \
    "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED", \
    "--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED", \
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED", \
    "--add-opens=java.base/javax.security.auth=ALL-UNNAMED", \
    "--enable-native-access=ALL-UNNAMED", \
    "--sun-misc-unsafe-memory-access=allow", \
    "-jar", "/app/app.jar"]
