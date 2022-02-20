FROM node
COPY package.json /app/
WORKDIR /app
RUN npm i
ENV TOPIC  mytopic
ENV KAFKA_BOOTSTRAP_SERVER kafka-cp-kafka:9092
ENV GROUP_ID mygroup
COPY kafka.js consumer.js  /app/
CMD ["node", "consumer.js"]
