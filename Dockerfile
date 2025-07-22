FROM ubuntu:24.04

# Set environment variables for non-interactive installations
ENV DEBIAN_FRONTEND=noninteractive

# Update package list and install OpenJDK 17
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
RUN export JAVA_HOME=$(readlink -f /usr/bin/java | sed 's/\/bin\/java//')
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app
COPY build/libs/*.war app.war
CMD ["java", "-jar", "app.war"]