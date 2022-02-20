const kafka = require('./kafka')

const groupId=process.env.GROUP_ID;
const topic=process.env.TOPIC; 

console.log("groupId="+groupId);
console.log("topic="+topic);

const consumer = kafka.consumer({ groupId })

const run = async () => {
  await consumer.connect()

  /*
   * 1. We need to know which partitions we are assigned.
   * 2. Which partitions have we consumed the last offset for
   * 3. If all partitions have 0 lag, we exit.
   */

  /*
   * `consumedTopicPartitions` will be an object of all topic-partitions
   * and a boolean indicating whether or not we have consumed all
   * messages in that topic-partition. For example:
   *
   * {
   *   "topic-test-0": false,
   *   "topic-test-1": false,
   *   "topic-test-2": false
   * }
   */
  let consumedTopicPartitions = {}
  consumer.on(consumer.events.GROUP_JOIN, async ({ payload }) => {
    const { memberAssignment } = payload
    consumedTopicPartitions = Object.entries(memberAssignment).reduce(
      (topics, [topic, partitions]) => {
        for (const partition in partitions) {
          topics[`${topic}-${partition}`] = false
        }
        return topics
      },
      {}
    )
  })

  /*
   * This is extremely unergonomic, but if we are currently caught up to the head
   * of all topic-partitions, we won't actually get any batches, which means we'll
   * never find out that we are actually caught up. So as a workaround, what we can do
   * is to check in `FETCH_START` if we have previously made a fetch without
   * processing any batches in between. If so, it means that we received empty
   * fetch responses, meaning there was no more data to fetch.
   *
   * We need to initially set this to true, or we would immediately exit.
   */
  let processedBatch = true
  consumer.on(consumer.events.FETCH_START, async () => {
    if (processedBatch === false) {
      await consumer.disconnect()
      process.exit(0)
    }

    processedBatch = false
  })

  /*
   * Now whenever we have finished processing a batch, we'll update `consumedTopicPartitions`
   * and exit if all topic-partitions have been consumed,
   */
  consumer.on(consumer.events.END_BATCH_PROCESS, async ({ payload }) => {
    const { topic, partition, offsetLag } = payload
    consumedTopicPartitions[`${topic}-${partition}`] = offsetLag === '0'

    if (Object.values(consumedTopicPartitions).every(consumed => Boolean(consumed))) {
      await consumer.disconnect()
      process.exit(0)
    }

    processedBatch = true
  })

  await consumer.subscribe({ topic, fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} ${message.value}`)
    },
  })
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

const errorTypes = ['unhandledRejection', 'uncaughtException']
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2']

errorTypes.map(type => {
  process.on(type, async e => {
    try {
      console.log(`process.on ${type}`)
      console.error(e)
      await consumer.disconnect()
      process.exit(0)
    } catch (_) {
      process.exit(1)
    }
  })
})

signalTraps.map(type => {
  process.once(type, async () => {
    try {
      await consumer.disconnect()
    } finally {
      process.kill(process.pid, type)
    }
  })
})
