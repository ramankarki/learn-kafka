import {
  Injectable,
  OnModuleInit,
  OnApplicationShutdown,
} from '@nestjs/common';
import {
  Kafka,
  Consumer,
  Producer,
  EachMessageHandler,
  EachMessagePayload,
} from 'kafkajs';
import { TOPIC_POST } from './topics';

@Injectable()
export class AppService implements OnModuleInit, OnApplicationShutdown {
  private consumer: Consumer;
  private producer: Producer;
  constructor() {
    const kafka = new Kafka({
      brokers: ['localhost:9092'],
      clientId: 'badapp',
    });
    this.consumer = kafka.consumer({
      groupId: `badapp.postghf`,
      readUncommitted: true,
    });
    this.producer = kafka.producer({});
  }

  async onModuleInit() {
    await Promise.all([this.producer.connect(), this.consumer.connect()]);
    await this.consumer.subscribe({
      topics: [TOPIC_POST],
      fromBeginning: false,
    });
    await this.consumer.run({
      eachBatch: async ({
        batch,
        commitOffsetsIfNecessary,
        isRunning,
        isStale,
        resolveOffset,
        uncommittedOffsets,
      }) => {
        console.log(
          `Starting to process ${batch.messages.length} messages of ${batch.topic} partition ${batch.partition}`,
        );

        const uncommitted = {};

        console.log(batch);
        console.log(batch.firstOffset());
        console.log(batch.lastOffset());
        console.log(batch.offsetLag());
        console.log(batch.offsetLagLow());
        console.log(isRunning(), isStale());
        resolveOffset(batch.messages.at(-1).offset);

        console.log(
          uncommittedOffsets().topics.find((t) => t.topic === TOPIC_POST)
            ?.partitions,
        );
        await new Promise((resolve) => {
          setTimeout(async () => {
            // await Promise.all(
            //   batch.messages.map(async (m) => {
            //     resolveOffset(m.offset);
            //     return await commitOffsetsIfNecessary({
            //       topics: [
            //         {
            //           topic: TOPIC_POST,
            //           partitions: [
            //             { offset: m.offset, partition: batch.partition },
            //           ],
            //         },
            //       ],
            //     });
            //   }),
            // );

            resolve(null);
          }, 20000);
        });
      },
      autoCommit: false,
      eachBatchAutoResolve: false,
    });
  }

  async handleMessage({ topic, partition, message }: EachMessagePayload) {
    const body = JSON.parse(message.value.toString());

    console.log({ body });
    console.log({ message });
    console.log({ partition });
    console.log({ topic });

    // return await this.consumer
    //   .commitOffsets([{ topic, partition, offset: message.offset }])
    //   .catch(console.log);
  }

  async onApplicationShutdown(signal?: string) {
    await Promise.all([this.producer.disconnect(), this.consumer.disconnect()]);
  }

  async addToKafka(topic: string, content: any) {
    return this.producer.send({
      topic,
      messages: [{ value: JSON.stringify(content) }],
    });
  }
}
