/*!
 * rpq - Redis Priority Queue
 * This library will use the following redis keys:
 * rpq:{qname}:priorities - sortedset
 * rpq:{qname}:queuedummy - list [1,1,1,1,1] This list is used to emulate blocking pop on multiple lists.
 * rpq:{qname}:queue:{priority} - list
 * rpq:{qname}:reserves - set, containing the keys of reserveId lists below
 * rpq:{qname}:reserve:{reserveId} - list of string/int (eg job id or stringified JSON)
 * where {qname} is the name of the queue specified when instantiating the queue object.
 */

"use strict";

// Module dependencies.
const EventEmitter = require('events').EventEmitter;
const redis = require('redis');

//const noop = function(){};

const DEFAULT_PRIORITIES = {
  low: 100,
  normal: 50,
  high: 10,
  critical: 0
};


module.exports = function(options) {
  return new PriorityQueue(options);
};

var scripts = {};
// x queue_n p1 p2 ... pn
scripts.count = {
  src : `
local n = 0
for i = 1, #KEYS do
  n = n + redis.call('llen', KEYS[i])
end
return n
` ,
  sha : ''
};

// x p1 p2 ... pn
// KEYS[1..n] = rpq:name:queue:1..n
scripts.peek = {
  src : `
for i,v in ipairs(KEYS) do
  local data = redis.call('lindex', v, 0)
  if data then return data end
end
` ,
  sha : ''
};

scripts.peekAll = {
  src : `
local p = redis.call('zrange', KEYS[1], 0, -1)
for i,v in ipairs(p) do
  local data = redis.call('lindex', ARGV[1] .. v, 0)
  if data then return data end
end
` ,
  sha : ''
};

//script 4 priorities queue_n queuedummy queue:{priority}
scripts.enqueue = {
  src : `
local p = string.match(KEYS[4], ":([^:]+)$")
if not redis.call('zscore', KEYS[1], p) then return {err="Priority not found"} end
local n = redis.call('incr', KEYS[2])
local dn = redis.call('llen', KEYS[3])
for i = 1, n - dn do redis.call('rpush', KEYS[3], '1') end
return redis.call('rpush', KEYS[4], ARGV[1])
` ,
  sha : ''
};

//KEYS[1] = queue_n
//KEYS[2] = reserves
//KEYS[3] = reserve:xxx
//KEYS[4,..n] = queue:critical, queue:normal, ...
//script n queue_n reserve_set_key [reserveKey] ..queuePrioritiesKey num_of_non_queue_keys
scripts.dequeue = {
  src : `
for i = ARGV[1], #KEYS do
  local data = redis.call('lpop', KEYS[i])
  if data then
    redis.call('decr', KEYS[1])
    if tonumber(ARGV[1]) == 4 then
      redis.call('sadd', KEYS[2], KEYS[3])
      redis.call('rpush', KEYS[3], data)
    end
    return data
  end
end
return false

` ,
  sha : ''
};

scripts.handover = {
  src : `
  local data = redis.call('lpop', KEYS[2])
  if data then
    if redis.call('llen', KEYS[2]) == 0 then
      redis.call('srem', KEYS[1], KEYS[2])
    end
    redis.call('rpush', KEYS[3], data)
    redis.call('sadd', KEYS[1], KEYS[3])
    return data
  end
` ,
  sha : ''
};

scripts.ack = {
  src : `
  local data = redis.call('lpop', KEYS[2])
  if data then
    if redis.call('llen', KEYS[2]) == 0 then
      redis.call('srem', KEYS[1], KEYS[2])
    end
    return data
  end
` ,
  sha : ''
};

/**
 * KEYS[1] = rpq:name:reserves
 * KEYS[2], KEYS[3], ... KEYS[n] = rpq:name:reserve:1, rpq:name:reserve:2, ... rpq:name:reserve:n
 */
scripts.peekReserves = {
  src : `
local a = {}
for i = 2, #KEYS do
  local data = redis.call('lindex', KEYS[i], 0)
  if data then
    table.insert(a, data)
  else
    table.insert(a, '')
    redis.call('srem', KEYS[1], KEYS[i])
  end
end
return a
` ,
  sha : ''
};

class PriorityQueue extends EventEmitter {
  /**
   * Create a new PriorityQueue.
   * @param {number} [options.port=6379] Redis port
   * @param {string} [options.host='127.0.0.1'] Redis host
   * @param {number} [options.db=0] Redis db number
   * @param {string} [options.url=null] Redis connection string, eg: redis://user:password@127.0.0.1:6379/1
   * @param {string} [options.name='default']
   * @param {Object} [options.priorities]
   * @param {string} [options.defaultPriority='normal']
   * @param {string} [options.keyPrefix='rpq']
   * @api public
   */
  constructor(options) {
    super();
    var self = this;
    var p = options || {};
    var rc = self._rc = redis.createClient(p);
    self._errorCallback = function(err) {
      if (err) self.emit('error', err);
    };
    /*
    var initScripts = function() {
      //console.log('connect');
      var count = 0;
      function done(){
        --count;
        if (count === 0) self.emit('ready');
      }
      Object.keys(scripts).forEach(function(k) {
        ++count;
        rc.script('exists', scripts[k].sha, function(err, results){
          if (err) self.emit('error', err);
          if (!results[0]) {
            rc.script('load', scripts[k].src, function(err, sha) {
              scripts[k].sha = sha;
              done();
              //console.log(err); console.log('%s %s', k, sha);
            });
          } else {
            done();
          }
        });
      });
    }
    */
    //rc.on('connect', initScripts);
    rc.on('ready', function(){ self.emit('ready'); });
    rc.on('error', self._errorCallback);
    var name = p.name || 'default';
    var prefix = p.keyPrefix || 'rpq';
    var rkeyPrefix = prefix + ':' + name + ':';
    var rkeyPriorities = rkeyPrefix + 'priorities';
    var rkeyDummy = rkeyPrefix + 'queuedummy';
    var rkeyQueueCount = rkeyPrefix + 'queue_n';
    var rkeyReserves = rkeyPrefix + 'reserves';

    Object.defineProperty(self, 'name', { value: name, enumerable:true });
    Object.defineProperty(self, '_rkeyPrefix', { value: rkeyPrefix });
    Object.defineProperty(self, '_rkeyPriorities', { value: rkeyPriorities }); // redis sorted set
    Object.defineProperty(self, '_rkeyDummy', { value: rkeyDummy }); // redis list
    Object.defineProperty(self, '_rkeyQueueCount', { value: rkeyQueueCount }); // redis int
    Object.defineProperty(self, '_rkeyReserves', { value: rkeyReserves }); // redis list
    self.setPriorities((p.priorities || DEFAULT_PRIORITIES), self._errorCallback);
    self.defaultPriority = p.defaultPriority || 'normal';
  }

  _rcevalsha(script, keys, args, cb) {
    var rc = this._rc;
    var params = [script.sha, keys.length];
    keys.forEach((v)=>params.push(v));
    args.forEach((v)=>params.push(v));
    if (script.sha) {
      rc.evalsha(params, function(err, result){
        if (err && err.code === 'NOSCRIPT') {
          load();
        } else {
          cb(err, result);
        }
      });
    } else {
      load();
    }
    function load(){
      rc.script('load', script.src, function(err, sha) {
        if (err) return cb(err);
        script.sha = params[0] = sha;
        rc.evalsha(params, cb);
      });
    }
  }

  _rkeyQueue(priorityKey) {
    return this._rkeyPrefix + 'queue:' + priorityKey;
  }
  _rkeyReserve(reserveId) {
    return this._rkeyPrefix + 'reserve:' + reserveId;
  }

  /* pvs is list of priorities */
  _priorityKeys(priorities_, cb_) {
    var priorities;
    var cb = cb_;
    if (!cb) {
      cb = priorities_;
    } else {
      priorities = priorities_;
    }
    var self = this;
    self._rc.zrange(self._rkeyPriorities, 0, -1, (err, epriorities) => {
      cb(err, epriorities.reduce( (arr, p) => {
        if (!priorities || priorities.indexOf(p) >= 0) arr.push( self._rkeyQueue(p) );
        return arr;
      }, []));
    });
  }

  setPriorities(priorities, cb_) {
    var cb = cb_ || this._errorCallback;
    var values = [];
    for (let key in priorities) {
      if (typeof priorities[key] != 'number') return cb(new Error('priority value must be number'));
      values.push(priorities[key], key);
    }
    this._priorities = priorities;
    this._rc.zadd(this._rkeyPriorities, values, cb);
  }

  getPriorities() {
    return this._priorities;
  }
  /*
  getPriorityKeys(priorities) {
    var p = this._priorities;
    if (Array.isArray(priorities) && priorities.length > 0) {
      p = {};
      for (let v of priorities) {
        if (v in this._priorities) p[v] = this._priorities[v];
      }
    }
    return Object.keys(p).sort( (a,b) => (p[a] - p[b]) ).map( (k) => (this._rkeyQueue(k)) );
  }
  */

  getOnlinePriorities(cb_) {
    var cb = cb_ || this._errorCallback;
    this._rc.zrange([this._rkeyPriorities, 0, -1, 'WITHSCORES'], function(err, result) {
      //logger.debug('priorities='+priorities);
      var pm = {};
      if (!err && result) {
        for (let i=0; i<result.length; i+=2) {
          pm[result[i]] = result[i+1];
        }
        this._priorities = pm;
      }
      return cb(err, pm);
    });
  }

  setPriority(label, value, cb_) {
    var cb = cb_ || this._errorCallback;
    this._priorities[label] = value;
    this._rc.zadd(this._rkeyPriorities, value, label, cb);
  }

  getPriority(label, cb_) {
    var cb = cb_ || this._errorCallback;
    this._rc.zscore(this._rkeyPriorities, label, cb);
  }

  quit() {
    if (this._rc.connected) this._rc.quit();
  }
  end() {
    this._rc.end(true);
  }

  /**
   * Enqueue a string representing a job to the queue.
   * @param {string} data - arbitrary data representing the queue entry
   * @param {string} [priority] - the priority of the job
   * @param {function} cb
   */
  enqueue(data, priority_, cb_) {
    //var data = typeof data_ === 'string' ? data_ : data_.toJSON();
    var cb = cb_ || this._errorCallback;
    var priority = priority_;
    if (typeof priority_ !== 'string') {
      priority = this.defaultPriority;
      if (typeof priority_ === 'function') cb = priority_;
    }
    var self = this;
    if (typeof this._priorities[priority] !== 'number') return cb(new Error('priority not found'));
    //self._rc.evalsha(scripts.enqueue.sha, 4, self._rkeyPriorities, self._rkeyQueueCount, self._rkeyDummy, self._rkeyQueue(priority), data, cb);
    self._rcevalsha(scripts.enqueue, [self._rkeyPriorities, self._rkeyQueueCount, self._rkeyDummy, self._rkeyQueue(priority)], [data], cb);
  }

  /**
   * @param {Object}  [options]
   * @param {integer} [options.timeout=0] - maximum number of seconds to block, 0 means block indefinitely
   * @param {string}  [options.reserve] - An id of the client which consumed the queued item.
   * If specified, the taken item will be reserved by the particular id and must be acknowledged.
   * TODO: Dequeue from specified priorities only
   */
  dequeue(options_, cb_) {
    var self = this;
    //var rc = this._rc;
    var options = typeof options_ === "object" ? options_ : {};
    var cb = (typeof options_ === 'function') ? options_ : (cb_ || this._errorCallback);
    var done = cb;
    var timeout = options.timeout || 0;

    var params = [self._rkeyQueueCount];
    if (options.reserve) {
      let reserveKey = self._rkeyReserve(options.reserve);
      params.push(self._rkeyReserves, reserveKey);
      done = function(err, result) {
        // confirm that a job has been consumed successfully
        cb(err, result, (cb) => self._ack(reserveKey, cb));
      };
//      rc.evalsha(scripts.peek.sha, 1, reserveKey, (err, data) => {
      self._rcevalsha(scripts.peek, [reserveKey], [], (err, data) => {
        if (err) return cb(err);
        if (data) return done(err, data);
        else return run();
      });
    } else {
      run();
    }

    function run() {
      self._rc.blpop(self._rkeyDummy, timeout, function(err, result){
        if (err || !result) return cb(err);
        self._priorityKeys(function(err, keys){
          if (err || !keys || keys.length === 0) return cb(err);
          //rc.evalsha(scripts.dequeue.sha, params.length + keys.length, ...params, ...keys, params.length + 1, done);
          self._rcevalsha(scripts.dequeue, [...params, ...keys], [params.length + 1], done);
        });
      });
    }
    return this;
  }

  /**
   * Get the number items in queue.
   *
   * @param {Array|string} [priorities] - The list of priorities to include, if not specified it will include all priorities.
   * @param {Function} cb - function(err, {Number} @n the number of queue)
   */
  count(priorities_, cb_) {
    var self = this;
    var priorities, cb = cb_ || this._errorCallback;
    if (typeof priorities_ === 'string') priorities = [priorities_];
    else if (typeof priorities_ === 'function') cb = priorities_;
    else if (Array.isArray(priorities_) && priorities_.length) priorities = priorities_;
    var done = function(err1, result){
      var count, err;
      if (!err1) {
        try {
          count = parseInt(result);
        } catch (err2) {
           err = err2;
        }
      }
      cb(err, count);
    }
    if (priorities) {
      self._priorityKeys(priorities, (err, keys) => {
        //self._rc.evalsha(scripts.count.sha, keys.length, ...keys, done);
        self._rcevalsha(scripts.count, keys, [], done);
      });
    } else {
      self._rc.get(self._rkeyQueueCount, done);
    }
  }

  /** Removes all items contained in this queue. This will delete all data associated with the named rpq in redis. */
  clear(cb_) {
    var cb = cb_ || this._errorCallback;
    var self = this;
    var n = 0;
    var lastErr;
    var done = function(err){
      if (err) lastErr = err;
      ++n;
      if (n === 2) {
        if (lastErr) return cb(lastErr);
        else self._rc.del(self._rkeyReserves, self._rkeyDummy, self._rkeyQueueCount, cb);
      }
    }
    self._rc.smembers(self._rkeyReserves, (err, rkeys) => {
      if (rkeys.length) self._rc.del(rkeys, done);
      else done(err);
    });
    self._priorityKeys((err, pkeys) => {
      if (pkeys.length) self._rc.del(pkeys, done);
      else done(err);
    });
  }

  /**
   * Peek the first element in the queue of given priorities.
   *
   * @param {String|Array} [priorities] - One or more priorities to peek
   * @param {Function} cb - (@jobId {number} the returned Job)
   */
  peek(priorities_, cb_) {
    var self = this;
    var priorities, cb;
    if (typeof priorities_ === 'function') {
      if (!cb_) cb = priorities_;
    } else if (typeof priorities_ === 'string') {
      priorities = [priorities_];
    } else if (Array.isArray(priorities_) && priorities_.length > 0) {
      priorities = priorities_;
    }
    cb = cb || cb_ || this._errorCallback;
//    if (priorities) {
//      pvs = priorities.map((p)=>(self._priorities[p] || p));
//    }
    //var pkeys = self.getPriorityKeys(priorities);
    //console.log(pkeys);
    self._priorityKeys(priorities, function(err, keys) {
      if (err || !keys || keys.length === 0) return cb(err);
      //self._rc.evalsha(scripts.peek.sha, keys.length, ...keys, cb);
      self._rcevalsha(scripts.peek, keys, [], cb);

    });
    return this;
  }

  /**
   * Hand over a reserved item to other client
   * return the handed over item
   */
  handover(from, to, cb_){
    var cb = cb_ || this._errorCallback;
    var reserveKeyFrom = this._rkeyReserve(from);
    var reserveKeyTo = this._rkeyReserve(to);
    //this._rc.evalsha(scripts.handover.sha, 3, this._rkeyReserves, reserveKeyFrom, reserveKeyTo, cb);
    this._rcevalsha(scripts.handover, [this._rkeyReserves, reserveKeyFrom, reserveKeyTo], [], cb);

    return this;
  }

  _ack(reserveKey, cb) {
    //this._rc.evalsha(scripts.ack.sha, 2, this._rkeyReserves, reserveKey, cb);
    this._rcevalsha(scripts.ack, [this._rkeyReserves, reserveKey], [], cb);
  }

  acknowledge(reserve, cb_) {
    if (!reserve) return;
    var cb = cb_ || this._errorCallback;
    this._ack(this._rkeyReserve(reserve), cb);
    return this;
  }

  /** Peek the first entry of each reserve associated with a unique client
   */
  peekReserves(cb) {
    var self = this;
    var done = cb || this._errorCallback;

    self._rc.smembers(self._rkeyReserves, function(err, keys){
      if (err) return done(err);
      //self._rc.evalsha(scripts.peekReserves.sha, keys.length + 1, self._rkeyReserves, ...keys, function(err, data){
      self._rcevalsha(scripts.peekReserves, [self._rkeyReserves, ...keys], [], function(err, data){
        if (err) return done(err);
        var pos = self._rkeyReserve('').length;
        var result = data.map( (v, i) => [keys[i].slice(pos), v] );
        return done(null, result);
      });
    });
  }
}
