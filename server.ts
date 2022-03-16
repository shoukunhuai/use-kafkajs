import "dotenv/config";

const express = require("express");
const cors = require("cors");

import {Request} from "express";


import {useKafka} from "./lib/use-kafka";

const app = express();
app.use(cors());
app.use(express.raw({limit: "1mb"}));
app.use(express.json({limit: "1mb"}));

// apply security check
app.use("/kfk/", async (req: Request, res: any, next: () => any) => {
    console.warn("%s %s", req.method, req.url);
    next();
});

const kfk = useKafka({
    brokers: [process.env.KAFKA_BROKERS || "localhost:9092"],
    pullConfigs: [{
        alias: "logs",
        topic: "test_logs",
        deserializer: buf => JSON.parse(buf.toString()),
        maxBytes: 102400,
        maxBytesPerPartition: 10240,
        maxWait: 2000,
    }]
}, {
    expressApp: app,
});

kfk.connect().then(() => {
    app.listen(9000, ()=> {
        console.log("Have fun with Kafka!");
    });
});
