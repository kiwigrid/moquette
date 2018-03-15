FROM openjdk:8-slim

RUN mkdir -p /kiwigrid/moquette-broker
ADD moquette-broker.jar /kiwigrid/moquette-broker/moquette-broker.jar

ADD https://github.com/ohjames/smell-baron/releases/download/v0.4.2/smell-baron /usr/bin/smell-baron
RUN chmod a+x /usr/bin/smell-baron
ENTRYPOINT ["smell-baron"]

HEALTHCHECK --interval=10s --timeout=3s CMD curl --fail http://localhost:8080/actuator/health || exit 1

CMD [ "java", "-Djava.security.egd=file:/dev/./urandom", "-jar", "/kiwigrid/moquette-broker/moquette-broker.jar", "--spring.config.location=file:/deployments/config/"]

EXPOSE 1883
