FROM --platform=linux/amd64 adoptopenjdk/openjdk11:alpine
WORKDIR /app
COPY build/libs/*.war app.war
CMD ["java", "-jar", "app.war"]