# RPQ JS - Redis-backed Priority Queue for NodeJS
RPQ is a simple reliable priority queue for NodeJS backed by Redis.

It follows the normal expected behavior of a minimum priority queue.
An item (represented by a string) is queued with a specified priority value.
The dequeue operation will always take out an item with the lowest priority value first.
Items with the same priority are guaranteed to be dequeued in strictly FIFO order.

The priority queue is implemented using multiple Redis list of different priorities.
A sorted set is used to keep track of valid priorities and to get the priority order.
A dummy list is used to emulate blocking on multiple lists.
If an extra identifier (called reserve) is provided to the dequeue operation,
an extra list associated with the reserve is added to track the last item dequeued.
This means that an item is removed from the queue but is still reserved until acknowledged.
Given the same reserve identifier, every subsequent dequeue operation will return the same item
until the "done" callback or acknowledge method is called.

## Installation

    npm install rpq

## Usage

    var q = require('rpq')({name: 'test'});

    q.enqueue('test1', function(err){
      if (!err) console.log('test1 is queued');
    });

## API documentation

### Instance creation
The function returned by `require('rpq')` accept an object as an argument, with the following properties:

- **name** : Name of the queue (string). Default: 'default'
- **port** : Redis port number (number). Default: 6379
- **host** : Redis host name or IP Address. Default: '127.0.0.1'
- **db** : Redis db index. Default: 0
- **priorities** : Default values:

  - low: 100
  - normal: 50
  - high: 10
  - critical: 0

- **defaultPriority** : Default: 'normal'

### Enqueue
### Dequeue
### Count
### Peek
