# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## [TBD] - TBD
### Changed
- Upgraded `hotels-oss-parent` to 4.1.0 (was 4.0.1).
- Upgraded `hcommon-hive-metastore` to 1.4.1 (was 1.0.0).
- Upgraded Circus Train version to 15.0.0 (was 12.0.0).

### Added
- Support for skipping Circus Train's housekeeping process.

## [3.0.0] - 2019-06-20
### Changed
- Maven group ID is now com.expediagroup.shuntingyard (was com.hotels.shunting.yard).
- All Java classes moved to com.expediagroup.shuntingyard (was com.hotels.shunting.yard).
- hotels-oss-parent version updated to 4.0.1 (was 2.3.5).
- Updated configuration properties to not have package names in them.

## [2.0.1] - 2019-06-12
### Fixed
* `FilteringMessageReader` now deletes filtered messages to mimic previous behaviour of delete on read.

### Changed
- Excluded `org.pentaho.pentaho-aggdesigner-algorithm` dependency as it's not available in Maven Central.

## [2.0.0] - 2019-04-16
### Added
* Support for specifying target database and table name.

### Changed
* Extracted Apiary SQS receiver logic to library in [`apiary-receivers`](https://github.com/ExpediaInc/apiary-extensions/tree/master/apiary-receivers).
* Moved SQS message deletion out of receiver and into `MessageReaderAdapter`.
* Upgraded `apiary-receiver-sqs` to 2.0.0 (was 1.4.0).

## [1.0.0] - 2019-03-19
### Added
* Support for handling new elements (location and table parameters) coming in from Apiary events.
* Support for performing metadata only sync in alter table and alter partition event.

## [0.0.8] - 2019-02-05
### Added
* Support for passing certain configurations like Graphite directly to internal CT instance using `--ct-config` argument.

## [0.0.7] - 2019-01-28
### Added
* log4j.xml to be packaged with Shunting-Yard Binary.

## [0.0.6] - 2019-01-22
### Changed
* Upgraded `hive` to 2.3.4 (was 2.3.0).
* ContainerCredentialsProvider to a more robust EC2ContainerCredentialsProvider.

## [0.0.5] - 2019-01-14
### Added
* Support to read AWS Credentials from within the Elastic Container Service Task using ContainerCredentialsProvider.

## [0.0.4] - 2019-01-08
### Changed
* Refactored project to remove checkstyle and findbugs warnings, which does not impact functionality.
* Upgraded `hotels-oss-parent` to 2.3.5 (was 2.1.0).
### Added
* Support for selecting the tables to be replicated [#6](https://github.com/HotelsDotCom/shunting-yard/issues/6).

## [0.0.3] - 2018-11-01
### Changed
* Enforce exception handling [#2](https://github.com/HotelsDotCom/shunting-yard/issues/2).

### Added
* Event aggregation based on time windows [#4](https://github.com/HotelsDotCom/shunting-yard/issues/4). This is a breaking change since the event model has been changed to a more suitable structure for [Circus Train](https://github.com/HotelsDotCom/circus-train).
* Support for handling Hive Metastore Events from Apiary.

### Removed
* Receivers & emitters for Kinesis and Kafka.

## [0.0.2] - 2018-06-06 - not ready for production
### Changed
* Generate a fat emitter JAR per emitter implementation [#13](https://github.com/HotelsDotCom/shunting-yard/issues/13).

## [0.0.1] - 2018-06-05 - not ready for production
### Added
* First release.
