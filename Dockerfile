# Stage 1: Build the application
FROM maven:3.9.6-eclipse-temurin-21 AS builder
WORKDIR /app

# Cache dependencies first for faster rebuilds
COPY pom.xml .
RUN mvn dependency:go-offline

# Copy source code and compile
COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Run the application
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

# Copy the compiled .jar from the builder stage
COPY --from=builder /app/target/*.jar app.jar

# Expose the new 8090 port
EXPOSE 8090

# Command to run the application
ENTRYPOINT ["java", "-jar", "app.jar"]