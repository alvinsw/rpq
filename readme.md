# Redis-backed Priority Queue (rpq) for NodeJS
A simple priority queue for NodeJS backed by redis. Priorities are implemented using multiple list.
A sorted set is used to keep track of valid priorities.

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
