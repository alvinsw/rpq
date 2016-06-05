'use strict';

var assert = require('assert');
var rpq = require('../index.js');
var redis = require('redis');
var rc = redis.createClient();

describe('Queue', function() {
  var q;
  before(function(done){
    rc.keys('rpq:test:*', function(err, keys){
      rc.del(keys, function(){
        q = rpq({name: 'test'});
        q.on('ready', done);
      });
    });
  });

  describe('#constructor()', function() {
    it('should init redis keys correctly', function(done) {
      rc.exists(q._rkeyPriorities, function(err, r){
        assert.equal(r, 1);
        done();
      });
    });
  });

  describe('#_priorityKeys()', function() {
    var keys1 = [ 'critical', 'normal' ];
    var keys2 = [ 'critical', 'high', 'normal', 'low' ];
    before(function(){
      keys1 = keys1.map((k)=>q._rkeyQueue(k));
      keys2 = keys2.map((k)=>q._rkeyQueue(k));
    });
    //var keys1 = [ 'rpq:test:queue:critical', 'rpq:test:queue:normal' ];
    //var keys2 = [ 'rpq:test:queue:critical', 'rpq:test:queue:high', 'rpq:test:queue:normal', 'rpq:test:queue:low' ];

    it('should get specific priorities', function(done) {
      q._priorityKeys(['critical', 'normal'], (err, keys)=>{
        assert.deepEqual(keys1, keys);
        done();
      });
    });
    it('should get all priorities', function(done) {
      q._priorityKeys((err, keys)=>{
        assert.deepEqual(keys2, keys);
        done();
      });
    });
  });

  describe('#getOnlinePriorities()', function() {
    it('should get all priorities', function(done) {
      q.getOnlinePriorities((err, ps)=>{
        assert.deepEqual(q.getPriorities(), ps);
        done();
      });
    });
  });

  describe('#enqueue()', function() {
    it('should enqueue to default (normal) priority queue', function(done) {
      var count = 6;
      var cases = [];
      function finish(){
        --count;
        if (count > 0) return;
        rc.lrange(q._rkeyQueue('normal'), 0, -1, (err, data)=>{
          assert.deepEqual(data, cases);
          done();
        });
      }
      for (var i=1; i<=count; ++i) {
        cases.push(''+i);
        q.enqueue(''+i, finish);
      }
    });
    it('should enqueue to low priority queue', function(done) {
      var count = 2;
      var cases = [];
      function finish(){
        --count;
        if (count > 0) return;
        rc.lrange(q._rkeyQueue('low'), 0, -1, (err, data)=>{
          assert.deepEqual(data, cases);
          done();
        });
      }
      for (var i=1; i<=count; ++i) {
        cases.push('l'+i);
        q.enqueue('l'+i, 'low', finish);
      }
    });
    it('should enqueue to critical priority queue', function(done) {
      var count = 2;
      var cases = [];
      function finish(){
        --count;
        if (count > 0) return;
        rc.lrange(q._rkeyQueue('critical'), 0, -1, (err, data)=>{
          assert.deepEqual(data, cases);
          done();
        });
      }
      for (var i=1; i<=count; ++i) {
        cases.push('c'+i);
        q.enqueue('c'+i, 'critical', finish);
      }
    });
  });

  describe('#count()', function() {
    it('should have total 10 items in queue by now', function(done) {
      q.count((err, c)=>{
        assert.strictEqual(10, c);
        done();
      });
    });
    it('should have 4 items in critical and low queue by now', function(done) {
      q.count(['critical', 'low'], (err, c)=>{
        assert.strictEqual(4, c);
        done();
      });
    });
    it('should have 0 item in high queue', function(done) {
      q.count('high', (err, c)=>{
        assert.strictEqual(0, c);
        done();
      });
    });
  });

  describe('#peek()', function() {
    it('should peek from all queues', function(done) {
      q.peek((err, data)=>{
        assert.strictEqual('c1', data);
        done();
      });
    });
    it('should peek from normal and low queues', function(done) {
      q.peek(['normal', 'low'], (err, data)=>{
        assert.strictEqual('1', data);
        done();
      });
    });
    it('should peek from high queue', function(done) {
      q.peek('high', (err, data)=>{
        assert.equal(null, data);
        done();
      });
    });
  });

  describe('#dequeue()', function() {
    it('should dequeue without reserve', function(done) {
      q.dequeue((err, data)=>{
        assert.strictEqual('c1', data);
        done();
      });
    });
    it('should dequeue with reserve id 1, no ack', function(done) {
      q.dequeue({reserve: '1'}, (err, data)=>{
        assert.strictEqual('c2', data);
        rc.lindex(q._rkeyReserve('1'), 0, (err, data)=>{
          assert.strictEqual('c2', data);
          done();
        });
      });
    });
    it('should dequeue with reserve id 2, ack cb', function(done) {
      q.dequeue({reserve: '2'}, (err, data, ack)=>{
        assert.strictEqual('1', data);
        ack(done);
      });
    });
    it('should dequeue with reserve id 1, ack cb', function(done) {
      q.dequeue({reserve: '1'}, (err, data, ack)=>{
        assert.strictEqual('c2', data);
        ack(done);
      });
    });
    it('should dequeue with reserve id 1, no ack', function(done) {
      q.dequeue({reserve: '1'}, (err, data)=>{
        assert.strictEqual('2', data);
        done();
      });
    });
  });

  describe('#handover()', function() {
    it('should handover reserve from 3 to 1', function(done) {
      q.dequeue({reserve: '3'}, (err, data)=>{ //no need to ack here
        assert.strictEqual('3', data);
        q.handover('3', '1', (err, data2) => {
          assert.strictEqual('3', data2);
          rc.lrange(q._rkeyReserve('1'), 0, -1, (err, data3)=>{
            assert.deepEqual(['2','3'], data3);
            done();
          });
        });
      });
    });
    it('should handover reserve from 3 to 2', function(done) {
      q.dequeue({reserve: '3'}, (err, data)=>{ //no need to ack here
        assert.strictEqual('4', data);
        q.handover('3', '2', (err, data2) => {
          assert.strictEqual('4', data2);
          rc.lrange(q._rkeyReserve('2'), 0, -1, (err, data3)=>{
            assert.deepEqual(['4'], data3);
            done();
          });
        });
      });
    });
  });

  describe('#peekReserves()', function() {
    it('should have correct reserves', function(done) {
      q.peekReserves((err, data)=>{
        var m = new Map(data);
        assert.strictEqual(m.get('1'), '2');
        assert.strictEqual(m.get('2'), '4');
        done();
      });
    });
  });

  describe('#acknowledge()', function() {
    it('should remove reserve 1', function(done) {
      q.acknowledge('1', (err, data)=>{
        assert.strictEqual('2', data);
        done(err);
      });
    });
    it('should remove reserve 1 again', function(done) {
      q.acknowledge('1', (err, data)=>{
        assert.strictEqual('3', data);
        done(err);
      });
    });
    it('should remove reserve 2', function(done) {
      q.acknowledge('2', (err, data)=>{
        assert.strictEqual('4', data);
        done(err);
      });
    });
    it('should have nothing on reserve 2', function(done) {
      q.acknowledge('2', (err, data)=>{
        assert.equal(null, data);
        done(err);
      });
    });
    it('should give no reserves', function(done) {
      q.peekReserves((err, data)=>{
        assert.strictEqual(0, data.length);
        done(err);
      });
    });
  });

  describe('#clear()', function() {
    it('should clear everything', function(done) {
      q.clear((err)=>{
        if (err) return done(err);
        rc.keys(q._rkeyPrefix + '*', function(err, keys){
          assert.strictEqual(1, keys.length);
          done(err);
        });
      });
    });
  });

  describe('#dequeue() blocking', function() {
    var q2;
    before(function(done){
      q2 = rpq({name: 'test'});
      q2.on('ready', done);
    });

    it('should wait until new item enqueued', function(done) {
      //q.enqueue('n1');
      setTimeout(function(){
        q2.enqueue('n1');
      }, 500);

      q.dequeue({reserve:'1'}, (err, data, ack)=>{
        assert.strictEqual('n1', data);
        ack(done);
      });

    });

    it('should have 0 item in queue', function(done) {
      q.count((err, c)=>{
        assert.strictEqual(0, c);
        done(err);
      });
    });

  });
});
