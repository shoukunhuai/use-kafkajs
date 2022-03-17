import {
    Message,
} from "kafkajs";


export interface FetchResponse {
    responses: {
        topicName: string;
        partitions: {
            partition: number;
            messages: Message[];
        }[]
    }[];
}
