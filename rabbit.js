var amqp = require('amqplib/callback_api'),
    forEach = require('async-foreach').forEach,
    cnt = parseInt(process.argv[2]) + 1,
    msgNum = new Array(cnt).join('1').split('');

// if the connection is closed or fails to be established at all, we will reconnect
var //amqpUrl = 'amqp://einipgml:9CzP0hvje2jqn87qPv6H74devXJBWu5y@moose.rmq.cloudamqp.com/einipgml',
    amqpUrl = 'amqp://aaronj:3696@10.200.7.110:5672',
    amqpConn,
    channel;    
connect();
function connect() { 
    amqp.connect(amqpUrl, function(err, conn) {
        if (err) {
          console.error("[AMQP]", err.message);
        }
        conn.on("error", function(err) {
          if (err.message !== "Connection closing") {
            console.error("[AMQP] conn error", err.message);
            connect();
          }
        });
        conn.on("close", function() {
          console.error("[AMQP] reconnecting");
          connect();
        });
        console.log("[AMQP] connected");
        amqpConn = conn;
        openChannel();
      });
}
  
  function openChannel() {
      amqpConn.createChannel(function(err, ch) {
          channel = ch;
          //channel.prefetch(3);
          console.log("Channel Created");
          //setUpConsumers();
          setTimeout(sendMessages, 500);
       });
      
  }
  
  function sendMessage(queue, message) {
      var queue = queue,
          exchange = queue + '-Exchange';
          //var em = channel.assertQueue(queue, {durable: false});//returns this
          //var eq = channel.sendToQueue(queue, new Buffer(message));//returns bool
          channel.assertExchange(exchange, 'fanout', {durable: false});
          channel.publish(exchange, '', new Buffer(message), {persistent:false});
          console.log(" [x] Sent Message", message);
  }
  
  
function sendMessages() {
    /*console.log('sending messages');
    for(var i = 0; i < 1; i++) {
        var queues = ['Foo', 'Bar', 'Baz'],
            time = Math.floor(Math.random()*(10)+1),    
            index = i % 3;
        var msgTxt = 'Message ' + i + ' sent to ' + queues[index] + '-Exchange';    
        var message = JSON.stringify({number: i, time: time, text: msgTxt});
        sendMessage(queues[index], message);
    }*/
    forEach(msgNum, function(item, index) {
       var done = this.async();
       var msgTxt = 'Message ' + index + ' sent to Exchange',
           time = Math.floor(Math.random()*(10)+1),
           sentTime = new Date().getTime();
           message = JSON.stringify({number: index, sent: sentTime, time: time, text: msgTxt});
       sendMessage('Foo', message);
       setTimeout(done, 0);
    });
}  


function setUpConsumers() {
    var queues = ['Foo', 'Bar', 'Baz'];
    /*channel.bindQueue(queues[0], queues[0] + '-Exchange');
    channel.bindQueue(queues[0], queues[1] + '-Exchange');
    channel.bindQueue(queues[0], queues[2] + '-Exchange');
    
    channel.bindQueue(queues[1], queues[0] + '-Exchange');
    channel.bindQueue(queues[1], queues[1] + '-Exchange');
    channel.bindQueue(queues[1], queues[2] + '-Exchange');
    
    channel.bindQueue(queues[2], queues[0] + '-Exchange');
    channel.bindQueue(queues[2], queues[1] + '-Exchange');
    channel.bindQueue(queues[2], queues[2] + '-Exchange');*/
    
    console.log('Setting up consumers');
    queues.forEach(function(queue) {
        var exchange = queue + '-Exchange';
        channel.assertExchange(exchange, 'fanout', {durable: false}, function(err, ex) {
            channel.assertQueue(queue, {durable: false}, function(err, q) {
                var bound = channel.bindQueue(q.queue, ex.exchange);
                var consCount = 1;
                console.log('Set up consumer %s for queue %s', consCount, queue);
                channel.consume(queue, function(msg) {
                    var msgTxt = JSON.parse(msg.content.toString());
                    console.log(msg);
                    console.log("Consumer " + consCount + " for " + queue + " received message " + msgTxt.number);
                    console.log('Processing task ' + msgTxt.number + ' in ' + msgTxt.time + ' seconds');
                    setTimeout(function() {
                        console.log('Task ' + msgTxt.number + ' Processed!!!');
                        //channel.ack(msg);
                    }, msgTxt.time * 1000);
                }, {noAck: true});
            });
        });
    });
    queues.forEach(function(queue, i) {
        var exchange = queue + '-Exchange';
        channel.assertExchange(exchange, 'fanout', {durable: false}, function(err, ex) {
            channel.assertQueue(queue, {durable: false}, function(err, q) {
                channel.bindQueue(q.queue, ex.exchange);
                var consCount = 2;
                console.log('Set up consumer %s for queue %s', consCount, queue);
                channel.consume(queue, function(msg) {
                    var msgTxt = JSON.parse(msg.content.toString());
                    console.log("Consumer " + consCount + " for " + queue + " received message " + msgTxt.number);
                    console.log('Processing task ' + msgTxt.number + ' in ' + msgTxt.time + ' seconds');
                    setTimeout(function() {
                        console.log('Task ' + msgTxt.number + ' Processed!!!');
                        //channel.ack(msg);
                    }, msgTxt.time * 1000);
                }, {noAck: true});
            });
        });
    });
}