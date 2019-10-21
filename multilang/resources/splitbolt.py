import json
import storm
from kafka import KafkaProducer

class SplitBolt(storm.BasicBolt):
    def initialize(self, conf, context):
        self._conf = conf
        self._context = context
        self._topic = 'temptopic_words'
        self._producer = KafkaProducer(
            bootstrap_servers=[
                '10.78.68.45:9092',
                '10.78.68.46:9092',
                '10.78.68.47:9092'
            ],
            value_serializer=lambda m: json.dumps(m).encode('utf-8')
        )
        storm.logInfo("Split bolt instance starting...")

    def process(self, tup):
        words = tup.values[0].split()
        data = {
            "data": words
        }
        self._producer.send(self._topic, data)
        self._producer.flush()
        storm.logInfo("sent %s" % data)

# Start the bolt when it's invoked
SplitBolt().run()
