# Shunting Yard

A Spring Boot app that reads serialized Hive MetaStore Events and builds a YAML file with the information provided in the event which is then passed to [Circus Train](https://github.com/HotelsDotCom/circus-train) to perform the replication.

## System architecture

![Shunting Yard system diagram.](shunting-yard.png "Selected tables getting replicated by Shunting Yard based on Hive events.")

## Start using

You can obtain Shunting Yard from Maven Central:

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.hotels/shunting-yard/badge.svg?subject=com.hotels:shunting-yard)](https://maven-badges.herokuapp.com/maven-central/com.hotels/shunting-yard) [![Build Status](https://travis-ci.org/HotelsDotCom/shunting-yard.svg?branch=master)](https://travis-ci.org/HotelsDotCom/shunting-yard) [![Coverage Status](https://coveralls.io/repos/github/HotelsDotCom/shunting-yard/badge.svg?branch=master)](https://coveralls.io/github/HotelsDotCom/shunting-yard?branch=master) ![GitHub license](https://img.shields.io/github/license/HotelsDotCom/shunting-yard.svg)

### How to install the emitter - SOURCE

On the source cluster, copy the file respective emitter JAR to _/usr/lib/hive/lib/_ and configure _/etc/hive/conf/hive-site.xml_ with the properties required by the listener to talk to the messaging infrastructure:

#### SQS
    hive.metastore.event.listeners = com.hotels.shunting.yard.event.emitter.sqs.listener.SqsMetaStoreEventListener
    com.hotels.shunting.yard.event.emitter.sqs.queue = https://sqs.<region>.amazonaws.com/<account-id>/<topic-name>-queue.fifo
    com.hotels.shunting.yard.event.emitter.sqs.group.id = <group-id>

Once this is done restart Hive:

    sudo restart hive-hcatalog-server

### How to install the receiver - TARGET

On the target cluster, download and install the latests version of Circus Train and set the `CIRCUS_TRAIN_HOME` environment variable:

    export CIRCUS_TRAIN_HOME=/home/hadoop/circus-train-<circus-train-version>

Export the location of the Hive libraries:

    export HIVE_LIB=/usr/lib/hive/lib/
    export HCAT_LIB=/usr/lib/hive-hcatalog/share/hcatalog/

Copy the file _shunting-yard-binary/target/shunting-yard-binary-\<version\>-bin.tgz_ and unzip it, then set the `SHUNTING_YARD_HOME` environment variable:

    export SHUNTING_YARD_HOME=/home/hadoop/shuting-yard-<version>

Create a `.yml` file under `$SHUNTING_YARD_HOME/conf/` with the correct settings to talk to the messaging infrastructure. You can find guidelines in the file _shunting-yard-minimal.yml.template_. Once this is done you are all set up to run the service:

    $SHUNTING_YARD_HOME/bin/replicator.sh --config=$SHUNTING_YARD_HOME/conf/<my-config>.yml
    
### Graphite Metrics

Graphite configurations can be passed to Shunting Yard using an optional `--ct-config` argument which takes a YAML file and passes it directly to internal Circus Train instance. Refer to the [Circus Train README](https://github.com/HotelsDotCom/circus-train#graphite) for more details.

#### Sample ct-config.yml:

    graphite:
      host: graphite-host:2003
      namespace: com.company.shuntingyard
      prefix: dev

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2016-2019 Expedia Inc.