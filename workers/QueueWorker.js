const amqp = require("amqplib");
const QUEUE_METADATA = require("./data/exchange");
const rabbitmqUrl =  "";

  const connect = async () => {
    try {
      const connection = await amqp.connect(rabbitmqUrl);
      this.channel = await connection.createChannel();
      console.log("connected RabbitMQ");
    } catch (error) {
      console.log("fail to connect RabbitMQ");
      throw error;
    }
  }

  const subscribeToQueues = async () => {
    for (let data of QUEUE_METADATA) {
      await this.channel.assertExchange(data.exchange, data.exchangeType);

      await this.channel.assertQueue(data.queue, data.options);

      await this.channel.bindQueue(
        data.queue,
        data.exchange,
        data.routingKey,
        data.headers
      );

      this.channel.consume(data.queueName, (msg) => {
        data.handler(msg, this.channel);
      });
    }
  }

  const close = async () => {
    await this.channel.close();
  }


const queueWorker = {
  connect,
  subscribeToQueues,
  close
}

module.exports = queueWorker;
