# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/) and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## TBD
### Changed
* Refactored project to remove checkstyle and findbugs warnings, which does not impact functionality.
* Upgraded `hotels-oss-parent` to 2.3.5 (was 2.1.0).
* Enforce exception handling [#2](https://github.com/HotelsDotCom/shunting-yard/issues/2)

### Added
* Event aggregation based on time windows [#4](https://github.com/HotelsDotCom/shunting-yard/issues/4). This is a breaking change since the event model has been changed to a more suitable structure for [Circus Train](https://github.com/HotelsDotCom/circus-train).
* Support for handling Hive Metastore Events from Apiary

### Removed
* Receivers & emitters for Kinesis and Kafka.

## [0.0.2] 2018-06-06 - not ready for production
### Changed
* Generate a fat emitter JAR per emitter implementation [#13](https://github.com/HotelsDotCom/shunting-yard/issues/13)

## [0.0.1] 2018-06-05 - not ready for production
### Added
* First release
