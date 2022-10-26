
import sys

sys.path.insert(0, '../utils/')

from utils import logparsing

consLog = logparsing.ConsumerLog()
coord = consLog.getAllGroupCoordinators('test-data/cons/', 10)
print(coord)
