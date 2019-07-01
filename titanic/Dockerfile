# Base Alpine Linux based image with OpenJDK JRE only
FROM openjdk:8-jre-alpine
#RUN apt-get update
#RUN apt-get install bash
COPY target/titanic-0.1.jar  /titanic-0.1.jar
RUN mkdir /data
COPY train.csv /data/train.csv
# specify default command
CMD ["/usr/bin/java", "-jar", "/titanic-0.1.jar"]
# CMD ["/bin/bash"]
