# syntax=docker/dockerfile:1.7

FROM apache/spark@sha256:87e5d6062e955a045b68376bbf97487d5057ecd8f4f896fb0590339981645de3

LABEL org.opencontainers.image.title="NASA EO Spark Kafka runtime" \
      org.opencontainers.image.description="Spark 4.0.2 with the exact Scala 2.13 Structured Streaming Kafka connector" \
      org.opencontainers.image.version="4.0.2-kafka-v1"

USER root

ADD --checksum=sha256:faa232c017b0cb4480c14643eb82abf6d0cac4b4d032059bf999b9246d045eb6 \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.0.2/spark-sql-kafka-0-10_2.13-4.0.2.jar \
    /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.2.jar
ADD --checksum=sha256:cc5ccc2f8d0d600cb1c356f3de21fb8b21cfaae16e73e350f4fd25a2d727a7ce \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.0.2/spark-token-provider-kafka-0-10_2.13-4.0.2.jar \
    /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.2.jar
ADD --checksum=sha256:7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9 \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
    /opt/spark/jars/kafka-clients-3.9.1.jar
ADD --checksum=sha256:6d3bd18df8410f3e31b031aca582cc109342358a62a2759ebd0c4cdf30d06f8b \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar \
    /opt/spark/jars/commons-pool2-2.12.0.jar

RUN chmod 0644 \
    /opt/spark/jars/spark-sql-kafka-0-10_2.13-4.0.2.jar \
    /opt/spark/jars/spark-token-provider-kafka-0-10_2.13-4.0.2.jar \
    /opt/spark/jars/kafka-clients-3.9.1.jar \
    /opt/spark/jars/commons-pool2-2.12.0.jar

USER spark
