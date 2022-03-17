import {Buffer} from "buffer";

import {
    Router,
    IRouter,
    Request as Req,
    Response as Resp,
    NextFunction as NextFn,
} from "express";

import {
    Cluster,
    TopicOffsets,
    PartitionMetadata,
    KafkaConfig,
    ConsumerConfig,
} from "kafkajs";

const {
    createLogger,
    LEVELS: {INFO},
} = require("kafkajs/src/loggers");

const MEGA: number = 1024 * 1024;

export type HTTP_METHOD = "GET" | "POST" | "DELETE" | "PUT";

export interface Decoder<T> {
    decode(buf: Buffer): T;
}

export interface Encoder<T> {
    encode(o: T): Buffer | string;
}

export class JSONDecoder implements Decoder<any> {

    encoding: BufferEncoding;

    constructor(enc: BufferEncoding = "utf8") {
        this.encoding = enc;
    }

    decode(buf: Buffer): any {
        return JSON.parse(buf.toString(this.encoding));
    }
}

export class JSONEncoder implements Encoder<any> {

    encoding: BufferEncoding;

    constructor(enc: BufferEncoding = "utf8") {
        this.encoding = enc;
    }

    encode(o: any): Buffer | string {
        if (typeof o === "object") {
            return Buffer.from(JSON.stringify(o), this.encoding);
        }

        return o.toString();
    }
}

export interface FetchConfig {

    maxWait?: number;
    minBytes?: number;
    maxBytes?: number;
    maxBytesPerPartition?: number;

    decoder: Decoder<any>;
}

export interface PushConfig {

    alias?: string;

    method?: HTTP_METHOD;
    url: string;

    batch?: boolean;
    batchSize?: number;

    autoCommitOffset?: boolean;

    decoder: Decoder<any>;

    bodyTransformer?: () => any;
}

export interface SendConfig {

    encoder: Encoder<any>;

}

export interface TopicConfig {

    name: string;

    alias?: string;

    fetch?: "off" | FetchConfig;

    send: "off" | SendConfig;
}

export interface KfkConfig extends KafkaConfig {

    maxWait?: number;
    maxBytes?: number;
    maxBytesPerPartition?: number;

    topics: TopicConfig[];

    consumers: {
        config: ConsumerConfig;
        topics: Map<string, PushConfig>;
    };
}

export interface MountOptions {
    expressApp?: IRouter;
    mountPoint?: string;
}

export class Kfk {

    cluster: Cluster;

    maxBytes: number;

    maxBytesPerPartition: number;

    maxWait: number;

    pullConfigs: PullConfig[];

    constructor({
                    brokers,
                    maxBytes = MEGA * 10,
                    maxBytesPerPartition = MEGA,
                    maxWait = 5000,
                    pullConfigs = [],
                }: KfkConfig) {


        this.maxWait = maxWait;
        this.maxBytes = maxBytes;
        this.maxBytesPerPartition = maxBytesPerPartition;
        this.pullConfigs = pullConfigs;

        this.cluster = new KafkaCluster({
            brokers,
            logger: createLogger({
                level: INFO,
                logCreator: LoggerConsole,
            }),
            socketFactory: defaultSF(),
        });

    }

    async connect(): Promise<void> {
        const {cluster, pullConfigs} = this;
        await cluster.connect();

        if (pullConfigs.length > 0) {
            await Promise.all(
                pullConfigs.map(({topic}) => cluster.addTargetTopic(topic)));
        }
    }

    disconnect(): Promise<void> {
        return this.cluster.disconnect();
    }

    getPartitionMetadata(topic: string): PartitionMetadata[] {
        const partitions = this.cluster.findTopicPartitionMetadata(topic);

        if (partitions.length === 0) {
            throw new Error("Topic not found, please try again later");
        }

        return partitions;
    }

    /**
     * Fetch offsets of a topic
     * @param topic The topic
     * @param from From where to fetch the offset, beginning/last, or a specific time.
     */
    async offsetFetch(topic: string, from: boolean | number = false):
        Promise<TopicOffsets | undefined> {

        const partitions = this.getPartitionMetadata(topic)
            .map(p => ({partition: p.partitionId}));

        if (typeof from === "boolean") {
            return this.cluster.fetchTopicsOffset([{
                topic,
                partitions,
                fromBeginning: from,
            }]).then(x => x.shift());
        } else {
            return this.cluster.fetchTopicsOffset([{
                topic,
                partitions,
                fromTimestamp: from,
            }]).then(x => x.shift());
        }
    }

    async fetch(config: FetchConfig,
                partitionId: number,
                offset?: string): Promise<any> {

        const {
            topic,
            maxBytes = this.maxBytes,
            maxBytesPerPartition = this.maxBytesPerPartition,
            maxWait = this.maxWait,
            decoder,
        } = config;

        const nodeId = this.cluster.findTopicPartitionMetadata(topic)
            .filter(p => p.partitionId === partitionId)
            .map(p => p.leader.toString())
            .shift();

        if (!nodeId) {
            throw new Error("Topic not found, please try again later");
        }

        if (!offset) {

            const offsets = await this.offsetFetch(topic, false);
            if (!offsets) {
                throw new Error("Topic not found");
            }

            offset = offsets.partitions.filter(p => p.partition === partitionId)
                .map(p => p.offset)
                .shift();
            if (!offset) {
                throw new Error("Partition not found: " + partitionId);
            }
        }
        const fetchOffset = offset;

        return this.cluster.findBroker({nodeId}).then(b => b.fetch({
            maxWaitTime: maxWait,
            maxBytes,
            topics: [{
                topic,
                partitions: [{
                    partition: partitionId,
                    fetchOffset,
                    maxBytes: maxBytesPerPartition,
                }],
            }],
        }).then(resp => {
            const {responses} = resp as {
                responses: FetchResponse[];
            };

            let messages = responses.shift()?.partitions?.shift()?.messages;
            let size = messages?.length || 0;
            if (messages && size > 0) {
                const seq = messages[size - 1].offset;
                if (seq === fetchOffset) {
                    size = 0;
                }

                return {
                    size,
                    ack: fetchOffset,
                    seq,
                    messages: size === 0 ? [] :
                        messages.map(({key, value, timestamp, offset}) => ({
                            offset,
                            timestamp,
                            key,
                            value: decoder ? decoder.decode(value) : value.toString(),
                        })),
                };
            } else {
                return {
                    size: 0,
                    ack: fetchOffset,
                    seq: fetchOffset,
                    messages: [],
                };
            }
        }));
    }
}

