/*!
 * rpq - Redis Priority Queue
 * This library will use the following redis keys:
 * rpq:{qname}:priorities - sortedset
 * rpq:{qname}:queuedummy - list [1,1,1,1,1] This list is used to emulate blocking pop on multiple lists.
 * rpq:{qname}:queue:{priority} - list
 * rpq:{qname}:reserve:{reserveId} - string/int (eg job id or stringified JSON)
 *
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
if #KEYS == 1 then
  return redis.call('get', KEYS[1])
else
  local n = 0
  for i = 2, #KEYS do
    n = n + redis.call('llen', KEYS[i])
  end
  return n
end
` ,
  sha : 'e41e3ffc43548a3a3a4a35469faf3d63d29ff9d2'
};

// x p1 p2 ... pn
scripts.peek = {
  src : `
for i,v in ipairs(KEYS) do
  local data = redis.call('lindex', v, 0)
  if data then return data end
end
` ,
  sha : 'fed7e0d6e74675c569360bad350905165fad20aa'
};

// 1 priorities prefix
scripts.peekAll = {
  src : `
local p = redis.call('zrange', KEYS[1], 0, -1)
for i,v in ipairs(p) do
  local data = redis.call('lindex', ARGV[1] .. v, 0)
  if data then return data end
end
` ,
  sha : '0dd923a204ca56cb8d426b3e8b497f329535750d'
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
  sha : 'e25db3d163f8ed468e2b91b7d290137a4c685c98'
};
//script 4 priorities queue_n queuedummy reserve prefix
scripts.dequeue = {
  src : `
if KEYS[4] then
  local data = redis.call('get', KEYS[4])
  if data then
    redis.call('rpush', KEYS[3], '1')
    return data
  end
end
local p = redis.call('zrange', KEYS[1], 0, -1)
for i,v in ipairs(p) do
  local data = redis.call('lpop', ARGV[1] .. v)
  if data then
    redis.call('decr', KEYS[2])
    if KEYS[4] then
      redis.call('set', KEYS[4], data)
    end
    return data
  end
end
return false
` ,
  sha : '649ed6bdb74f03903eac5424c166adca2496d046'
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
    var initScripts = function() {
      //console.log('connect');
      Object.keys(scripts).forEach(function(k) {
        rc.script('exists', scripts[k].sha, function(err, results){
          if (err) self.emit('error', err);
          if (!results[0]) rc.script('load', scripts[k].src, function(err, sha) {
            scripts[k].sha = sha;
            //console.log(err); console.log('%s %s', k, sha);
          });
        });
      });
    }
    rc.on('error', self._errorCallback);
    rc.on('connect', initScripts);
    rc.on('ready', function(){ self.emit('ready'); });
    var name = p.name || 'default';
    var rkeyPrefix = 'rpq:' + name + ':';
    var rkeyPriorities = rkeyPrefix + 'priorities';
    var rkeyDummy = rkeyPrefix + 'queuedummy';
    var rkeyQueueCount = rkeyPrefix + 'queue_n';

    Object.defineProperty(self, 'name', { value: name, enumerable:true });
    Object.defineProperty(self, '_rkeyPrefix', { value: rkeyPrefix });
    Object.defineProperty(self, '_rkeyPriorities', { value: rkeyPriorities });
    Object.defineProperty(self, '_rkeyDummy', { value: rkeyDummy });
    Object.defineProperty(self, '_rkeyQueueCount', { value: rkeyQueueCount });
    self.setPriorities((p.priorities || DEFAULT_PRIORITIES), self._errorCallback);
    self.defaultPriority = p.defaultPriority || 'normal';
  }

  _rkeyQueue(priorityKey) {
    return this._rkeyPrefix + 'queue:' + priorityKey;
  }
  _rkeyReserve(reserveId) {
    return this._rkeyPrefix + 'reserve:' + reserveId;
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
    this._rc.quit();
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
    self._rc.evalsha(scripts.enqueue.sha, 4, self._rkeyPriorities, self._rkeyQueueCount, self._rkeyDummy, self._rkeyQueue(priority), data, cb);
  }

  /**
   * @param {Object}  [options]
   * @param {integer} [options.timeout=0] - maximum number of seconds to block, 0 means block indefinitely
   * @param {string}  [options.reserve] - An id of the client which consumed the queued item.
   * If specified, the taken item will be reserved by the particular id and must be confirmed.
   * TODO: Dequeue from specified priorities only
   */
  dequeue(options_, cb_) {
    var self = this;
    var rc = this._rc;
    var options = typeof options_ === "object" ? options_ : {};
    var cb = (typeof options_ === 'function') ? options_ : (cb_ || this._errorCallback);

    var timeout = options.timeout || 0;
    var reserveKey = typeof options.reserve === 'string' ? self._rkeyReserve(options.reserve) : undefined;
    var tryblocking = function(after) {
      rc.blpop(self._rkeyDummy, timeout, function(err, result){
        if (err || !result) return cb(err);
        after();
      });
    };
    if (reserveKey) {
      var done = function(cb) {
        // confirm that a job has been consumed successfully
        rc.del(reserveKey, cb);
      };
      // always check if a previously reserved item is still available
      rc.get(reserveKey, function(err, item) {
        if (err) return cb(err);
        if (item != null) {
          return cb(null, item, done);
        } else {
          tryblocking(function(){
            rc.evalsha(scripts.dequeue.sha, 4, self._rkeyPriorities, self._rkeyQueueCount, self._rkeyDummy, reserveKey, self._rkeyPrefix + 'queue:', (err, result) => cb(err, result, done));
          });
        }
      });
    } else {
      tryblocking(function(){
        rc.evalsha(scripts.dequeue.sha, 3, self._rkeyPriorities, self._rkeyQueueCount, self._rkeyDummy, self._rkeyPrefix + 'queue:', cb);
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
    else priorities = priorities_;
    var params = [scripts.count.sha];
    if (priorities && priorities.length) {
      let pkeys = self.getPriorityKeys(priorities);
      params.push(pkeys.length + 1, self._rkeyQueueCount, ...pkeys);
    } else {
      params.push(1, self._rkeyQueueCount);
    }
    self._rc.evalsha(params, cb);
  }

  /**
   * Peek the first element in the queue of given priorities.
   *
   * @param {String|Array} [priorities] - One or more priorities to peek
   * @param {Function} cb - (@jobId {number} the returned Job)
   */
  peek(priorities_, cb_) {
    var self = this;
    var priorities, cb = cb_ || this._errorCallback;
    if (typeof priorities_ === 'string') priorities = [priorities_];
    else if (typeof priorities_ === 'function') cb = priorities_;
    else priorities = priorities_;

    var pkeys = self.getPriorityKeys(priorities);
    //console.log(pkeys);
    self._rc.evalsha(scripts.peek.sha, pkeys.length, ...pkeys, cb);
    /*
    self._rc.zrange([self._rkeyPriorities, 0, -1], function(err, result) {
      if (!err && result && result.length > 0) {
        if (priorities.length === 0) {
          priorities = result;
        } else {
          let ps = new Set(result);
          priorities = priorities.filter(p => ps.has(p));
        }

        let multi = self._rc.multi();
        for (let p of priorities) {
          multi.lindex(self._rkeyQueue(p), 0);
        }
        multi.exec(function(err, items) {
          //  console.log(items);
          var item;
          if (items) {
            for (let i of items) {
              if (i != null) {
                item = i;
                break;
              }
            }
          }
          cb(err, item);
        });
      } else {
        cb(err, null);
      }
    });
    */
    //return this;
  }
}
