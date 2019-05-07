# Vigilancia-Distributed

# inside kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
.\bin\windows\kafka-server-start.bat .\config\server.properties

# inside spark 
python producer.py
spark-submit --jars spark-streaming-kafka-0-8-assembly_2.11-2.3.3.jar consumer.py
