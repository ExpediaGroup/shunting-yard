# Shunting Yard

Shunting Yard reads serialized Hive MetaStore Events from a queue (currently supports [AWS SQS](https://aws.amazon.com/sqs/)) and replicates the data between two Data lakes. It does this by building a YAML file with the information provided in the event which is then passed to [Circus Train](https://github.com/HotelsDotCom/circus-train) to perform the replication.

## Start using

You can obtain Shunting Yard from Maven Central:

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.hotels/shunting-yard/badge.svg?subject=com.hotels:shunting-yard)](https://maven-badges.herokuapp.com/maven-central/com.hotels/shunting-yard) [![Build Status](https://travis-ci.org/HotelsDotCom/shunting-yard.svg?branch=master)](https://travis-ci.org/HotelsDotCom/shunting-yard) [![Coverage Status](https://coveralls.io/repos/github/HotelsDotCom/shunting-yard/badge.svg?branch=master)](https://coveralls.io/github/HotelsDotCom/shunting-yard?branch=master) ![GitHub license](https://img.shields.io/github/license/HotelsDotCom/shunting-yard.svg)

## System architecture

![Shunting Yard system diagram.](shunting-yard.png "Selected tables getting replicated by Shunting Yard based on Hive events.")

### Install

1. Download the version to use from [Maven Central](https://mvnrepository.com/artifact/com.hotels/shunting-yard-binary) and uncompress it in a directory of your choosing.

2. Download and install the latest version of [Circus Train](http://mvnrepository.com/artifact/com.hotels/circus-train/) and set the `CIRCUS_TRAIN_HOME` environment variable:

    export CIRCUS_TRAIN_HOME=/home/hadoop/circus-train-<circus-train-version>

## Usage
To run Shunting Yard you just need to execute the `bin/replicator.sh` script in the installation directory and pass the configuration file: 

    /home/hadoop/shunting-yard/bin/replicator.sh --config=/path/to/config/file.yml

### EMR
If you are planning to run Shunting Yard on EMR you will need to set up the EMR classpath by exporting the following environment variables before calling the `bin/replicator.sh` script:

    export HCAT_LIB=/usr/lib/hive-hcatalog/share/hcatalog/
    export HIVE_LIB=/usr/lib/hive/lib/

Note that the paths above are correct as of when this document was last updated but may differ across EMR versions, refer to the [EMR release guide](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-release-components.html) for more up to date information if necessary.

## Configuring Shunting Yard

The examples below all demonstrate configuration using YAML and provide fragments covering the most common use cases that should be useful as a basis for building your own configuration. A full configuration reference is provided in the following sections.

#### Configuring source, replica and SQS queue

The YAML fragment below shows some common options for setting up the base source (where data is coming from), replica (where data is going to) and the SQS queue to read hive events from.

    source-catalog:
      name: source-cluster
      hive-metastore-uris: thrift://emr-master-1.compute.amazonaws.com:9083
    replica-catalog:
      name: replica-cluster
      hive-metastore-uris: thrift://emr-master-2.compute.amazonaws.com:9083
    event-receiver:
      configuration-properties:
        com.hotels.shunting.yard.event.receiver.sqs.queue: https://sqs.us-west-2.amazonaws.com/123456789/sqs-queue
        com.hotels.shunting.yard.event.receiver.sqs.wait.time.seconds: 20
    source-table-filter:
      table-names:
        - ...
    table-replications:
      ...

#### Selecting tables to monitor

The YAML fragment below shows how to select the tables to be monitored by Shunting Yard.

    source-table-filter:
      table-names:
        - test_database.test_table_1
        - test_database.test_table_2

#### Specifying target database & table names

The YAML fragments below shows some common options for specifying the target database and table name for the selected tables.

##### Specify both target database and table name.

    table-replications:
      - source-table:
          database-name: source_database
          table-name: test_table
        replica-table:
          database-name: replica_database
          table-name: test_table_1    
          
##### Only Change the target database but the table name remains same as source

    table-replications:
      - source-table:
          database-name: source_database
          table-name: test_table
        replica-table:
          database-name: replica_database 

##### Only Change the target table name but the database remains same as source

    table-replications:
      - source-table:
          database-name: source_database
          table-name: test_table
        replica-table:
          table-name: test_table_1

### Shunting Yard configuration reference
The table below describes all the available configuration values for Shunting Yard.

|Property|Required|Description|
|:----|:----:|:----|
|`source-catalog.name`|Yes|A name for the source catalog for events and logging.|
|`source-catalog.hive-metastore-uris`|No|Fully qualified URI of the source cluster's Hive metastore Thrift service.|
|`replica-catalog.name`|Yes|A name for the replica catalog for events and logging.|
|`replica-catalog.hive-metastore-uris`|Yes|Fully qualified URI of the replica cluster's Hive metastore Thrift service.|
|`event-receiver.configuration-properties.com.hotels.shunting.yard.event.receiver.sqs.queue`|Yes|Fully qualified URI of the [AWS SQS](https://aws.amazon.com/sqs/) Queue to read the hive events from.|
|`event-receiver.configuration-properties.com.hotels.shunting.yard.event.receiver.sqs.wait.time.seconds`|No|Wait time in seconds for which the receiver will poll the SQS queue for a batch of messages. Read more about long polling with AWS SQS [here](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html)|
|`source-table-filter.table-names`|No|A list of tables selected for Shunting Yard replication. Supported Format: `database_1.table_1, database_2.table_2`|
|`table-replications[n].source-table.database-name`|No|The name of the database in which the table you wish to replicate is located.|
|`table-replications[n].source-table.table-name`|No|The name of the table which you wish to replicate.|
|`table-replications[n].replica-table.database-name`|No|The name of the destination database in which to replicate the table. Defaults to source database name.|
|`table-replications[n].replica-table.table-name`|No|The name of the table at the destination. Defaults to source table name.|

### Configuring Graphite Metrics

Graphite configurations can be passed to Shunting Yard using an optional `--ct-config` argument which takes a YAML file and passes it directly to internal Circus Train instance. Refer to the [Circus Train README](https://github.com/HotelsDotCom/circus-train#graphite) for more details.

#### Sample ct-config.yml for graphite metrics:

    graphite:
      host: graphite-host:2003
      namespace: com.company.shuntingyard
      prefix: dev

### Housekeeping

[Housekeeping](https://github.com/HotelsDotCom/housekeeping) is the process that removes expired and orphaned data on the replica. Shunting Yard delegates housekeeping responsibility to Circus Train. Similar to Graphite configuration, Housekeeping configuration can also be directly passed to the internal Circus Train instance using `--ct-config` argument. Refer to the [Circus Train README](https://github.com/HotelsDotCom/circus-train#configuring-housekeeping) for more details.

#### Sample ct-config.yml for housekeeping:

    housekeeping:
      expired-path-duration: P3D
      db-init-script: classpath:/schema.sql
      data-source:
        driver-class-name: org.h2.Driver 
        url: jdbc:h2:${housekeeping.h2.database};AUTO_SERVER=TRUE;DB_CLOSE_ON_EXIT=FALSE
        username: user
        password: secret

# Legal
This project is available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

Copyright 2016-2019 Expedia, Inc.