jest.setTimeout(60e3);

const {
    secureRandom,
    createProducer,
    createTopic, waitFor,
} = require('../testhelpers');

describe('Producer > Flush', () => {
    let producer, topicName, message;

    let stats;

    beforeEach(async () => {
      stats = null;
        producer = createProducer({
        }, {
          'stats_cb': newStats => stats = newStats,
          'statistics.interval.ms': 100,
          'linger.ms': 5000, /* large linger ms to test flush */
          'queue.buffering.max.kbytes': 2147483647, /* effectively unbounded */
        });

        topicName = `test-topic-${secureRandom()}`;
        message = { key: `key-${secureRandom()}`, value: `value-${secureRandom()}` };

        await createTopic({ topic: topicName });
    });

    afterEach(async () => {
        producer && (await producer.disconnect());
    });


    it('does not wait for linger.ms',
        async () => {
            await producer.connect();
            await producer.send({ topic: topicName, messages: [message] });

            await producer.flush({ timeout: 5000 });
            await waitFor(() => stats, () => null, { delay: 5e3 });
            expect(stats).toEqual(expect.objectContaining({
              client_id: 'rdkafka'
            }));
        }
    );
});
