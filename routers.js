const express = require("express");
const postQueue = require("./controller/queue");
const postExchange = require("./controller/exchange");

const router = express.Router();

router.post("/queue/:queue", postQueue);
router.post("/:exchangeType/exchange/:exchange", postExchange);

module.exports = router;
