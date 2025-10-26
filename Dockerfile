FROM ubuntu:24.04

# Set environment variables for non-interactive installations
ENV DEBIAN_FRONTEND=noninteractive

# Update package list and install OpenJDK 21
RUN apt-get update && apt-get install -y \
    openjdk-21-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
RUN export JAVA_HOME=$(readlink -f /usr/bin/java | sed 's/\/bin\/java//')
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app
COPY build/libs/*.jar app.jar
CMD ["java", "-jar", "app.jar"]