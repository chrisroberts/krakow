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
producer.write('direct hit!')
```

## Zargons

```ruby
require 'krakow'

consumer = Krakow::Consumer(
  :nsqlookupd => 'http://HOST:PORT',
  :topic => 'target',
  :channel => 'ship'
)

message = consumer.queue.pop
# do stuff
consumer.confirm(message.message_id)
```

# Info
* Repo: https://github.com/chrisroberts/krakow
* IRC: Freenode @ spox
