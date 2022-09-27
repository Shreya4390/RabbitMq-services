const rabbitMQ = require('../utils/RabbitMQ');
const uniqueId = require('../utils/uniqueId');

const postExchange = async (req, res) => {
    try {
        const {
            body,
            params: { exchange, exchangeType }
        } = req;
        /*
            Message marked as 'persistent' that are delivered to 'durable' queues will be logged to disk.
            Durable queue are recovered in the event of a crash, along with any persistent message they stored prior crash.
        */
        const persistent = 2;
        const type = `${exchange}.created`;
        const headers = body.properties?.headers || {}
        const options = {
            appId: exchange,
            contentType: 'application/json',
            deliveryMode: persistent,
            durable: true,
            headers,
            messageId: uniqueId(),
            priority: 5,
            timestamp: Date.now(),
            type
        };

        const isValidExchangeType = rabbitMQ.isValidExchangeType(exchangeType);

        if (!isValidExchangeType) {
            throw new Error('Invalid exchnage type.')
        }

        await rabbitMQ.publishExchange({
            exchange,
            exchangeType,
            body,
            options,
        });
        res.send('Done');
    } catch (error) {
        console.log('post exchnage error', error);
        res.status(500).json(error);
    }
};


module.exports = postExchange;