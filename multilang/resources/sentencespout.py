import storm
import random
import time

# Define some sentences
SENTENCES = """
the cow jumped over the moon
an apple a day keeps the doctor away
four score and seven years ago
snow white and the seven dwarfs
i am at two with nature
""".strip().split('\n')

class SentenceSpout(storm.Spout):
    def initialize(self, conf, context):
        self._conf = conf
        self._context = context
        self._index = 0
        storm.logInfo("Spout instance starting...")
        
    def nextTuple(self):
        sentence = random.choice(SENTENCES)
        storm.logInfo("index %d - emiting: %s" % (self._index, sentence))
        self._index += 1
        storm.emit([sentence])
        time.sleep(0.1)

# Start the spout when it's invoked
SentenceSpout().run()
