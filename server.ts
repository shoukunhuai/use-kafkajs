import "dotenv/config";

const express = require("express");
const cors = require("cors");


import {useKafka} from "./lib/use-kafka";

const app = express();
app.use(cors());
app.use(express.raw({limit: "1mb"}));
app.use(express.json({limit: "1mb"}));


const kfk = useKafka({
    brokers: [process.env.KAFKA_BROKERS || 'localhost:9092'],
}, {
    expressApp: app
});

kfk.connect().then(() => {
    app.listen(9000);
});
