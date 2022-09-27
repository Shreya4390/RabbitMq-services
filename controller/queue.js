const rabbitMQ = require("../utils/RabbitMQ");

async function postQueue(req, res) {
    try {
        const {
            body: payload,
            params: { queue },
        } = req;
        await rabbitMQ.sendMessage(queue, payload);

        res.status(200).json('Message send successfully.');
    } catch (error) {
        console.error("post queue error", error);
        res.status(500).json(error);
    }
}

module.exports = postQueue;
