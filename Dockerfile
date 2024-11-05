FROM --platform=linux/amd64 adoptopenjdk/openjdk11:alpine
WORKDIR /app
RUN ls -al build/libs
COPY build/libs/*.war app.war
CMD ["java", "-jar", "app.war"]