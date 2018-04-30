# Shunting Yard

Little Spring Boot app that read Java-serialized Hive MetaStore Events and build a YAML file with the information provided in the event which is then passed to Circus Train to perform the replication.

### How to install the emitter - SOURCE

On the source cluster, copy the file _circus-train-event-driven-binary/target/circus-train-event-emitter-0.0.1-SNAPSHOT-all.jar_ to _/usr/lib/hive/lib/_ and configure _/etc/hive/conf/hive-site.xml_ with the properties required by the listener to talk to the messaging infrastructure:

#### Kafka
    hive.metastore.event.listeners = com.hotels.bdp.circus.train.event.emitter.kafka.listener.KafkaMetaStoreEventListener
    com.hotels.bdp.circus.train.event.emitter.kafka.topic = <topic-name>
    com.hotels.bdp.circus.train.event.emitter.kafka.bootstrap.servers = <kafka-connection-string>

#### Kinesis
    hive.metastore.event.listeners = com.hotels.bdp.circus.train.event.emitter.kinesis.listener.KinesisMetaStoreEventListener
    com.hotels.bdp.circus.train.event.emitter.kinesis.stream = <stream-name>

#### SQS
    hive.metastore.event.listeners = com.hotels.bdp.circus.train.event.emitter.sqs.listener.SqsMetaStoreEventListener
    com.hotels.bdp.circus.train.event.common.aws.sqs.queue = https://sqs.<region>.amazonaws.com/<account-id>/<topic-name>-queue.fifo
    com.hotels.bdp.circus.train.event.common.aws.sqs.group.id = <group-id>

Once this is done restart Hive:

    sudo restart hive-hcatalog-server

### How to install the receiver - TARGET

On the target cluster, download and install the latests version of Circus Train and set the `CIRCUS_TRAIN_HOME` environment variable:

    export CIRCUS_TRAIN_HOME=/home/hadoop/circus-train-<circus-train-version>

Export the location of the Hive libraries:

    export HIVE_LIB=/usr/lib/hive/lib/
    export HCAT_LIB=/usr/lib/hive-hcatalog/share/hcatalog/

Copy the file _circus-train-event-driven-binary/target/circus-train-event-driven-binary-0.0.1-SNAPSHOT-bin.tgz_ and unzip it, then set the `CIRCUS_TRAIN_EVENT_DRIVEN_HOME` environment variable:

    export CIRCUS_TRAIN_EVENT_DRIVEN_HOME=/home/hadoop/circus-train-event-driven-0.0.1-SNAPSHOT

Create a `.yml` file under `$CIRCUS_TRAIN_EVENT_DRIVEN_HOME/conf/` with the correct settings to talk to the messaging infrastructure. You can find some guidelines in the file _circus-train-event-driven-minimal.yml.template_. Once this is done you are all set up to run the service:

    $CIRCUS_TRAIN_EVENT_DRIVEN_HOME/bin/replicator.sh --config=$CIRCUS_TRAIN_EVENT_DRIVEN_HOME/conf/<my-config>.yml

