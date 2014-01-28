# Krakow

"KRAKOW! KRAKOW! Two direct hits!"

## Spiff

```ruby
require 'krakow'

producer = Krakow::Producer(
  :host => 'HOST',
  :port => 'PORT',
  :topic => 'target'
)
producer.write('KRAKOW!', 'KRAKOW!')
```

## Zargons

```ruby
require 'krakow'

consumer = Krakow::Consumer(
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

It's a Ruby library for NSQ[1] using Celluloid[2] under the hood.

## Information and FAQ that I totally made up

### Max in flight for consumers is 1, regardless of number of producers

Yep, that's right. Just one lowly message at a time. And that's probably not what
you want, so adjust it when you create your consumer instance.

```ruby
require 'krakow'

consumer = Krakow::Consumer(
  :nsqlookupd => 'http://HOST:PORT',
  :topic => 'target',
  :channel => 'ship',
  :max_in_flight => 30
)
```

### Please make it shutup!

Sure:

```
Krakow::Utils::Logging.level = :warn # :debug / :info / :warn / :error / :fatal
```

### Why is it forcing something called an "unready state"?

Because forcing starvation is mean.

### I just want to connect to a producer, not a lookup service

Fine!

```ruby
consumer = Krakow::Consumer(
  :host => 'HOST',
  :port => 'PORT',
  :topic => 'target',
  :channel => 'ship',
  :max_in_flight => 30
)
```
Great for testing, but you really should use the lookup service in the "real world"

### It doesn't work

Create an issue on the github repository.

#### It doesn't do `x`

Create an issue, or even better, send a PR. Just base it off the `develop` branch.

# Info
* Repo: https://github.com/chrisroberts/krakow
* IRC: Freenode @ spox

[1] https://github.com/bitly/nsq
[2] https://github.com/celluloid/celluloid
