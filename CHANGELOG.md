## v0.3.10
* Remove exclusive from Connection#init!

## v0.3.8
* Remove locks and move logic to connection access
* Check for result within response prior to access (prevent slaying actor)

## v0.3.6
* Allow `:options` key within `Producer` to set low level connection settings
* Make snappy an optional dependency
* Add initial support for authentication
* Update allowed types for optional notifier

## v0.3.4
* Explicitly require version file (#11 and #12)

## v0.3.2
* Fix return value from Connection#wait_time_for (#9) (thanks @AlphaB and @davidpelaez)

## v0.3.0
* Include jitter to discovery interval lookups
* Typecast to String on PUB and MPUB
* Update exception types used for not implemented methods
* Add #confirm, #requeue, and #touch helpers to FrameType::Message instances
* Update Utils::Lazy implementation to be faster and clearer
* Add #safe_socket method on Connection to add stability
* Rebuild connections on error to prevent consumer teardown
* Reference connections without requirement of connection instance being alive
* Use #read over #recv on underlying socket to ensure proper number of bytes (thanks @thomas-holmes)
* Expand spec testing

A big thanks to @bschwartz for a large contribution in this changeset
including expanded spec coverage, message proxy helper methods, and
isolation of instability around Connection interactions.

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
