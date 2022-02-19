const kafka = require('./kafka')

const GROUP_ID=process.env.GROUP_ID;
const TOPIC=process.env.TOPIC; 

console.log("GROUP_ID="+GROUP_ID);
console.log("TOPIC="+TOPIC);

const consumer = kafka.consumer({
  groupId: GROUP_ID
})

const main = async () => {
  await consumer.connect()

  var consumed=false;

  await consumer.subscribe({
    topic: TOPIC,
    fromBeginning: false
  })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log('Received message', {
        topic,
        partition,
        message: "Consumed: "+message.value.toString()
      })
    }
  })
}

main().catch(async error => {
  console.error(error)
  try {
    await consumer.disconnect()
  } catch (e) {
    console.error('Failed to gracefully disconnect consumer', e)
  }
  process.exit(1)
})
