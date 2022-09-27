const amqp = require("amqplib");
const rabbitmqUrl = "amqps://snadkezc:c6U-9h0F1m4WJVJw6DKhvzzgbKmYopZP@puffin.rmq2.cloudamqp.com/snadkezc";

const connect = async () => {
    try {
        const connection = await amqp.connect(rabbitmqUrl);
        this.channel = await connection.createChannel();
        console.log("connected RabbitMQ");
    } catch (error) {
        console.log("fail to connect RabbitMQ");
        throw error;
    }
};

const assertQueue = async (queueName, options = {}) => {
    await this.channel.assertQueue(queueName, options);
};

const sendMessage = async (queueName, payload) => {
    await this.channel.sendToQueue(
        queueName,
        Buffer.from(JSON.stringify(payload)),
        {
            persistent: true,
            deliveryMode: 2,
        }
    );
};

const assertExchange = async (exchange, exchangeType, options = {}) => {
    await this.channel.assertExchange(exchange, exchangeType, options);
};

const publishExchange = async ({ exchange, exchangeType, body, options = {} }) => {
    const { payload, properties } = body;
    await assertExchange(exchange, exchangeType, options);
    this.channel.publish(
        exchange,
        properties.routing_key,
        Buffer.from(JSON.stringify(payload)),
        options
    );
};


const bindExchangeQueue = async (exchange, routingKey, options = {}) => {
    /*
     *One of the reason put empty string is because `fanout` exchange will broadcast message to each queue,
     *If we assertQueue with same queue name, only 1 consumer will receive the message as there are in the same queue.name
     *Passing empty string will automatically create random queue name and exclusive flag will delete thr queue after it closed
     */
    const { queue } = await this.channel.assertQueue("", options);

    // channel.prefetch(1); //ensure the queue doesn't keep dispatch message to consumer until they've ack the job
    await this.channel.bindQueue(queue, exchange, routingKey, options.headers);

    return queue;
};

const consumeMessage = (queue) => {
    this.channel.consume(queue, (message) => {
        // Uncomment this to see the message properties and other info
        // console.log("message", message);
        console.log("Received", JSON.parse(message.content.toString()));
        this.channel.ack(message, false, true);
    });
};

const close = async() => {
    await this.channel.close();
};

const isValidExchangeType = (exchangeType) => {
    return [ "fanout", "direct", "topic", "headers" ].some(
        (exchange) => exchange === exchangeType
    );
};


const RabbitMQ = {
    connect,
    assertQueue,
    sendMessage,
    assertExchange,
    publishExchange,
    bindExchangeQueue,
    consumeMessage,
    close,
    isValidExchangeType,
};

module.exports = RabbitMQ;


