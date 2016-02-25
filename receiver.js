var amqp = require('amqplib/callback_api');

// if the connection is closed or fails to be established at all, we will reconnect
var //amqpUrl = 'amqp://einipgml:9CzP0hvje2jqn87qPv6H74devXJBWu5y@moose.rmq.cloudamqp.com/einipgml',
    amqpUrl = 'amqp://aaronj:3696@10.200.7.110:5672',
    amqpConn,
    //queues = ['Foo', 'Bar', 'Baz'],
    channel;    

amqp.connect(amqpUrl, function(err, conn) {
    if (err) {
        console.log(err);
        return;
    }
    console.log('Receiver connected');
    amqpConn = conn;
    setUpConsumer();
    //setUpConsumers();
});

/*function setUpConsumers() {
    queues.forEach(function(queue) {
        connectToQueue(queue, function(channel) {
            channel.consume(queue, function(msg) {
                console.log("Queue " + queue + " received %s", msg.content.toString());
            }, {noAck: true});
        });
    });
}*/

function createChannel(queue, callback) {
    amqpConn.createChannel(function(err, ch) {
        //ch.assertQueue(queue, {durable: false});
        callback(ch);
    });
}

var totalTime = 0,
    messages = 0,
    times = [],
    addedToTimes = false;
        
function setUpConsumer() {
    createChannel('Foo', function(channel) {
        channel.assertExchange('Foo-Exchange', 'fanout', {durable: false}, function(err, ex) {
            channel.assertQueue('Foo', {durable: false}, function(err, q) {
                var bound = channel.bindQueue(q.queue, ex.exchange);
                channel.consume('Foo', function(msg) {
                    //console.log("Queue received %s", msg.content.toString());
                    var msgObj = JSON.parse(msg.content.toString()),
                        sentTime = msgObj.sent,
                        recTime = new Date(),
                        diff = recTime - sentTime;
                    totalTime += diff;
                    times.push(diff);
                    addedToTimes = true;
                    var avg = totalTime / ++messages;
                    console.log("Queue received message %s in %s ms : Average %s ms", msgObj.number, diff, avg);
                    channel.ack(msg);
                }, {noAck: false});    
            });
        });
    });
}

function printRange() {
    if (times.length > 0 && addedToTimes) {
        var max = Math.max.apply(Math, times);
        var min = Math.min.apply(Math, times);
        addedToTimes = false;
        console.log('Range - Max: %s ms ~ Min %s ms', max, min);
    }
    setTimeout(printRange, 7000);
}
setTimeout(printRange, 7000);