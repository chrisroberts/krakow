# Krakow

"KRAKOW! KRAKOW! Two direct hits!"

## Spiff

```ruby
require 'krakow'

producer = Krakow::Producer.new(
  :host => 'HOST',
  :port => 'PORT',
  :topic => 'target'
)
producer.write('KRAKOW!', 'KRAKOW!')
```

## Zargons

```ruby
require 'krakow'

consumer = Krakow::Consumer.new(
  :nsqlookupd => 'http://HOST:PORT',
  :topic => 'target',
  :channel => 'ship'
)

consumer.queue.size # => 2
2.times do
  msg = consumer.queue.pop
  puts "Received: #{msg}"
  consumer.confirm(msg.message_id)
end
```

## What is this?

It's a Ruby library for [NSQ][1] using [Celluloid][2] under the hood.

## Information and FAQ that I totally made up

### Max in flight for consumers is 1, regardless of number of producers

Yep, that's right. Just one lowly message at a time. And that's probably not what
you want, so adjust it when you create your consumer instance.

```ruby
require 'krakow'

consumer = Krakow::Consumer.new(
  :nsqlookupd => 'http://HOST:PORT',
  :topic => 'target',
  :channel => 'ship',
  :max_in_flight => 30
)
```

### Clean up after yourself

Since [Celluloid][2] is in use under the hood, and the main interaction points are
Actors (`Consumer` and `Producer`) you'll need to be sure you clean up. This simply
means terminating the instance (since falling out of scope will not cause it to be
garbage collected).

```ruby
consumer = Krakow::Consumer.new(
  :nsqlookupd => 'http://HOST:PORT',
  :topic => 'target',
  :channel => 'ship',
  :max_in_flight => 30
)

# do stuff

consumer.terminate
```

### Please make it shutup!

Sure:

```ruby
Krakow::Utils::Logging.level = :warn # :debug / :info / :warn / :error / :fatal
```

### Why is it forcing something called an "unready state"?

Because forcing starvation is mean. We don't want to be mean, so we'll ensure we
are consuming from all registered connections.

### I just want to connect to a producer, not a lookup service

Fine!

```ruby
consumer = Krakow::Consumer.new(
  :host => 'HOST',
  :port => 'PORT',
  :topic => 'target',
  :channel => 'ship',
  :max_in_flight => 30
)
```
Great for testing, but you really should use the lookup service in the "real world"

### Backoff support

NSQ has this backoff notion. It's pretty swell. Basically, if messages from a specific
producer get re-queued (fail), then message consumption from that producer is halted,
and slowly ramped back up. It gives time for downstream issues to work themselves out,
if possible, instead of just keeping the firehose of gasoline on. Neat.

By default backoff support is disabled. It can be enabled by setting the `:backoff_interval`
when constructing the `Consumer`. The interval is in seconds (and yes, floats are allowed
for sub-second intervals):

```ruby
consumer = Krakow::Consumer.new(
  :nsqlookupd => 'http://HOST:PORT',
  :topic => 'target',
  :channel => 'ship',
  :max_in_flight => 30,
  :backoff_interval => 1
)
```

### I need TLS!

OK!

```ruby
consumer = Krakow::Consumer.new(
  :nsqlookupd => 'http://HOST:PORT',
  :topic => 'target',
  :channel => 'ship',
  :connection_options => {
    :features => {
      :tls_v1 => true
    }
  }
)
```

### I need Snappy compression!

OK!

```ruby
consumer = Krakow::Consumer.new(
  :nsqlookupd => 'http://HOST:PORT',
  :topic => 'target',
  :channel => 'ship',
  :connection_options => {
    :features => {
      :snappy => true
    }
  }
)
```

*NOTE*: snappy support requires the snappy
gem and is not provided by default, so you
will need to ensure it is installed either
on the system, or within the bundle.

### I need Deflate compression!

OK!

```ruby
consumer = Krakow::Consumer.new(
  :nsqlookupd => 'http://HOST:PORT',
  :topic => 'target',
  :channel => 'ship',
  :connection_options => {
    :features => {
      :deflate => true
    }
  }
)
```

### I want to use TLS based authentication!

OK!

```ruby
consumer = Krakow::Consumer.new(
  :nsqlookupd => 'http://HOST:PORT',
  :topic => 'target',
  :channel => 'ship',
  :connection_options => {
    :features => {
      :tls_v1 => true
    },
    :config => {
      :ssl_context => {
        :certificate => '/path/to/cert',
        :key => '/path/to/key'
      }
    }
  }
)
```

### Running the tests

Run them all!

```
bundle exec ruby test/run.rb
```

Or, run part of them:

```
bundle exec ruby test/specs/consumer_spec.rb
```

*NOTE*: the specs expect that `nsqd` and `nsqlookupd` are available in `$PATH`

### It doesn't work

Create an issue on the github repository

* https://github.com/chrisroberts/krakow/issues

### It doesn't do `x`

Create an issue, or even better, send a PR.

* https://github.com/chrisroberts/krakow/pulls

# Info
* Repo: https://github.com/chrisroberts/krakow
* Docs: http://code.chrisroberts.org/krakow
* IRC: Freenode @ spox

[1]: http://bitly.github.io/nsq/ "NSQ: a realtime distributed messaging platform"
[2]: http://celluloid.io "Celluloid: Actor-based concurrent object framework for Ruby"

# Contributors

* Brendan Schwartz (@bschwartz)
* Thomas Holmes (@thomas-holmes)
* Jeremy Hinegardner (@copiousfreetime)