"use strict";

var q = require('rpq')({name: 'test'});

setTimeout(test, 1000);

function test(){
  console.log('enqueue data: test1');
  q.enqueue('test1', function(err){
    if (err) {
      console.log(err.code);
      return;
    }
    console.log(arguments);
    q.count(function(err, c){
      if (err) console.log(err);
      console.log('count: %d', c);
      q.peek(function(err, data){
        if (err) console.log(err);
        console.log('peek: %s', data);
        q.dequeue({reserve: 'c1'}, function(err, data, done){
          if (err) console.log(err);
          console.log('dequeue data: %s', data);
          done(function(err){
            if (err) console.log(err);
            q.count(function(err, c){
              if (err) console.log(err);
              console.log('count: %d', c);
              q.quit();
            });
          });
        })
      });
    });
  });

}
