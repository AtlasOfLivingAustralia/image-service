FROM --platform=linux/amd64 adoptopenjdk/openjdk11:alpine
RUN echo "Starting process..."
WORKDIR /app
RUN pwd
RUN ls
COPY build/libs/*.war app.war
CMD ["java", "-jar", "app.war"]