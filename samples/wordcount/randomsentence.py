import time
import random
import logging

from petrel import storm
from petrel.components import Spout

log = logging.getLogger(__name__)

log.debug('randomsentence loading')

class RandomSentenceSpout(Spout):
    def __init__(self):
        super(RandomSentenceSpout, self).__init__()
        #self._index = 0

    def initialize(self, conf, context):
        log.info('conf: %s', str(conf))
        log.info('context: %s', str(context))

    @classmethod
    def declareOutputFields(cls):
        return ['sentence']

    sentences = [
        "the cow jumped over the moon",
        "an apple a day keeps the doctor away",
        "four score and seven years ago",
        "snow white and the seven dwarfs",
        "i am at two with nature"
    ]

    def next_tuple(self):
        #if self._index == len(self.sentences):
        #    # This is just a demo; keep sleeping and returning None after we run
        #    # out of data. We can't just sleep forever or Storm will hang.
        #    time.sleep(1)
        #    return None

        time.sleep(0.25);
        sentence = self.sentences[random.randint(0, len(self.sentences) - 1)]
        #sentence = self.sentences[self._index]
        #self._index += 1
        log.debug('randomsentence emitting: %s', sentence)
        self.emit([sentence])

def run():
    RandomSentenceSpout().run()
