jest.setTimeout(60e3);

const {
  secureRandom,
  createTopic,
  waitFor,
  createProducer,
  createConsumer,
} = require('../testhelpers');

describe('Consumer statistics', () => {
  let topicName, groupId, producer, consumer;

  let stats;
  beforeEach(async () => {
    stats = null;
    topicName = `test-topic-${secureRandom()}`;
    groupId = `consumer-group-id-${secureRandom()}`;
    await createTopic({ topic: topicName, partitions: 3 });

    producer = createProducer({});

    consumer = createConsumer({
        groupId,
        maxWaitTimeInMs: 100,
        fromBeginning: true,
        autoCommit: false,
        autoCommitInterval: 500,
      }, {
        'statistics.interval.ms': 100,
        'stats_cb': newStats => stats = newStats
      }
    );
  });

  afterEach(async () => {
    console.log('DISCONNECTING');
    consumer && (await consumer.disconnect());
    producer && (await producer.disconnect());
  });

  it('produce statistics', async () => {
    await consumer.connect();
    await consumer.subscribe({ topic: topicName });
    consumer.run(async () => {});
    function test() {
      return stats !== null;
    }
    await waitFor(test, () => null, { delay: 5e3 });
    expect(stats).toEqual(expect.objectContaining({
      client_id: 'rdkafka'
    }));
  });
});
