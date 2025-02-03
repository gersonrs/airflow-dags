FROM bitnami/spark:3

USER root

RUN install_packages curl

COPY dependencies.txt download-jars.sh .

RUN chmod +x download-jars.sh && ./download-jars.sh

RUN chown -R spark:spark $SPARK_HOME/jars

USER 1001
