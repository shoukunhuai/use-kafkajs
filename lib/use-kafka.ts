import {Router, IRouter} from "express";

import {Cluster as ICluster} from "kafkajs";

const {
    createLogger,
    LEVELS: {INFO, DEBUG},
} = require("kafkajs/src/loggers");

const LoggerConsole = require("kafkajs/src/loggers/console");
const KafkaCluster = require("kafkajs/src/cluster");
const defaultSF = require("kafkajs/src/network/socketFactory");

export type HTTP_METHOD = "GET" | "POST" | "DELETE" | "PUT";

export type MESSAGE_ENCODING = "json";

export interface PushConfig {

    alias?: string;

    topic: string;

    groupId?: string;

    url: string;

    method?: HTTP_METHOD;

    autoCommitOffset?: boolean;

    encoding: MESSAGE_ENCODING;

    acceptBatch?: boolean;

    batchSize?: number;
}


export interface PullConfig {
    alias?: string;

    topic: string;

    groupId?: string;

}

export class Kfk {

    cluster: ICluster;

    pollTimeout?: number;

    maxBytes?: number;

    constructor(brokers: string[]) {
        this.cluster = new KafkaCluster({
            brokers,
            logger: createLogger({
                level: DEBUG,
                logCreator: LoggerConsole,
            }),
            socketFactory: defaultSF(),
        });

    }

    async connect(): Promise<void> {
        await this.cluster.connect();

        await this.cluster.addTargetTopic("test_logs");
    }

    async disconnect(): Promise<void> {
        await this.cluster.disconnect();
    }

    async fetch(topic: string, partition: number = 0, offset: string = "0", maxBytes: number = 1024 * 1024): Promise<any> {
        const md = this.cluster.findTopicPartitionMetadata(topic).filter(p => p.partitionId === partition);

        const b = await this.cluster.findBroker({
            nodeId: md[0].leader.toString(),
        });

        return b.fetch({
            topics: [{
                topic,
                partitions: [{
                    partition,
                    fetchOffset: offset,
                    maxBytes: 1024 * 1024,
                }],
            }],
        });

    }

    async subscribe(topic: string, handler: () => Promise<any>) {
        // subscribe to topic
    }

}

export interface KafkaOptions {
    brokers: string[];

    alias?: string;

    pushConfigs?: Array<PushConfig>;

    pullConfigs?: Array<PullConfig>;
}

export interface MountOptions {
    expressApp?: IRouter;
}

export function useKafka({
                             brokers,
                             alias,
                             pullConfigs = [],
                             pushConfigs = [],
                         }: KafkaOptions, options: MountOptions = {
    expressApp: undefined,
}): Kfk {

    console.log("brokers= %s", brokers);

    const kfk = new Kfk(brokers);

    const {expressApp} = options;

    if (expressApp) {
        const rt = Router();

        rt.get("/topics/:topic", (req, res) => {
            const {topic} = req.params;

            console.log("topic=%s", topic);

            res.json(kfk.cluster.findTopicPartitionMetadata(topic));
        });

        rt.get("/consumers/:group/topics/:topic/records", (req, res) => {
            const {topic} = req.params;
            const {offset} = req.query;

            kfk.fetch(topic, 0, typeof offset === "string" ? offset : "0").then(v => res.json(v));

        });

        pullConfigs.forEach(({alias, topic, groupId}) => {
            rt.get(`/consumers/${groupId}/${topic}/offsets`, (req, res) => {
                res.json(kfk.cluster.findTopicPartitionMetadata(topic));
            });
        });

        // GET /consumers/:group/:topic/offsets
        // GET /consumers/:group/:topic/:partition/records

        // rt.get("/consumers/:groupId/topics/:topic/offsets");

        expressApp.use("/kfk/", rt);
    }


    return kfk;
}
