FROM crail:daniel

ADD . /application

RUN echo "Crail-application build" && \
    cd application && \
    mvn -DskipTests package

# CONFIG HERE
#ENV NAMENODE_HOST=localhost
#ENV NAMENODE_PORT=9060
#ENV INTERFACE=eth0
#ENV DATAPATH=/dev/hugepages/data
#ENV STORAGELIMIT=1073741824
#ENV CACHEPATH=/dev/hugepages/cache
#ENV CACHELIMIT=0

ENV CLASSPATH=${CRAIL_HOME}/jars/*:${CRAIL_HOME}/conf:.

# CONFIG MAIN CLASS
ENV MAIN_CLASS=org.example.benchmark.actionbw.BenchActionBw

WORKDIR /application/target
ENTRYPOINT []
CMD java -cp ./*:$CLASSPATH $MAIN_CLASS