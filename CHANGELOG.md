## v0.2.2
* Fix `nsqlookupd` attribute in `Consumer` and `Discovery`

## v0.2.0
* Fix the rest of the namespacing issues
* Start adding some tests
* Use better exception types (NotImplementedError instead of NoMethodError)
* Be smart about responses within connections
* Add snappy support
* Add deflate support
* Add TLS support
* Prevent division by zero in distribution
* Add query methods to lazy helper (`attribute_name`?)

## v0.1.2
* Include backoff support
* Remove `method_missing` magic
* Force message redistribution when connection removed
* Make discovery interval configurable
* Add support for HTTP producer
* Include namespace for custom exceptions #1 (thanks @copiousfreetime)
* Fix timeout method access in req command #1 (thanks @copiousfreetime)

## v0.1.0
* Add logging support
* Include valid responses within commands
* Segregate responses from messages
* Manage connections in consumer (closed/reconnect)
* Add message distribution support

## v0.0.1
* Initial release
