import {
    Router,
    IRouter,
    Request as Req,
    Response as Resp,
    NextFunction as NextFn,
} from "express";


import {
    Cluster as ICluster,
    TopicOffsets,
    PartitionMetadata, ISocketFactory,
} from "kafkajs";

const {
    createLogger,
    LEVELS: {INFO},
} = require("kafkajs/src/loggers");

const LoggerConsole = require("kafkajs/src/loggers/console");
const KafkaCluster = require("kafkajs/src/cluster");
const defaultSF = require("kafkajs/src/network/socketFactory");

export type HTTP_METHOD = "GET" | "POST" | "DELETE" | "PUT";

export type MESSAGE_ENCODING = "json" | "raw";

const MEGA: number = 1024 * 1024;

const json = (h: (req: any, res: any, next: NextFn) => Promise<any>) => {
    return (req: Req, res: Resp, next: NextFn): Promise<any> => {
        return h(req, res, next).then(x => res.json(x))
            .catch((e: Error) => {
                console.error(e);
                res.status(500).json({
                    errorMsg: e.message,
                });
            });
    };
};

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

    minBytes?: number;
    maxBytes?: number;
    /**
     * Max wait time in milliseconds.
     */
    maxWait?: number;
    maxBytesPerPartition?: number;

    deserializer?: (buf: Buffer) => any;
}

interface FetchResponseMessage {
    timestamp?: string;
    offset: string;
    key?: string | Buffer;
    value: Buffer;
    // headers?
}

interface FetchResponse {
    topicName: string;
    partitions: {
        partition: number;
        messages: FetchResponseMessage[];
    }[];
}

export class Kfk {

    cluster: ICluster;

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
                }: KafkaOptions) {


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

    async fetch(config: PullConfig,
                partitionId: number,
                offset?: string): Promise<any> {

        const {
            topic,
            maxBytes = this.maxBytes,
            maxBytesPerPartition = this.maxBytesPerPartition,
            maxWait = this.maxWait,
            deserializer: des,
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
                            value: des ? des(value) : value.toString(),
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

export interface KafkaOptions {
    brokers: string[];

    maxWait?: number;

    maxBytes?: number;

    maxBytesPerPartition?: number;

    pushConfigs?: Array<PushConfig>;

    pullConfigs?: Array<PullConfig>;

    socketFactory?: ISocketFactory;
}

export interface MountOptions {
    expressApp?: IRouter;
    mountPoint?: string;
}

interface Query {
    ack?: string;
    seq?: string;
    timeout?: string;
    ts?: string;
    partition?: string;
}

export function useKafka(
    options: KafkaOptions,
    mountOptions: MountOptions = {}): Kfk {
    const kfk = new Kfk(options);

    const {pullConfigs} = options;

    const {expressApp, mountPoint = "/kfk/"} = mountOptions;

    if (expressApp) {
        const rt = Router();
        pullConfigs?.forEach((config) => {
            const {topic, alias} = config;
            const path = alias || topic;
            rt.get(`/topics/${path}/records`, json(req => {
                const {seq} = req.query as Query;
                let {partition} = req.query;
                partition = partition ? parseInt(partition) : 0;

                if (isNaN(partition)) {
                    return Promise.reject("Invalid partition: " + partition);
                }

                return kfk.fetch(config, partition, seq);
            }));

            rt.get(`/topics/${path}/offsets`, json((req) => {
                let {ts} = req.query;
                ts = ts ? isNaN(ts = parseInt(ts)) ? false :
                    ts === 0 ? true : ts : false;

                return kfk.offsetFetch(topic, ts);
            }));
        });

        expressApp.use(mountPoint, rt);
    }

    return kfk;
}
