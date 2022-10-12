# command to run this script: sudo python3 <logDir> <noOfConsumers>
#!/usr/bin/python3

import sys
from datetime import datetime

logDir = sys.argv[1]
numberOfConsumers = int(sys.argv[2])

i = 1
avgPerConsumer = []
totalMsgConsumed = 0

while i<= numberOfConsumers:
    with open(logDir + '/cons'+str(i)+'.log', 'r') as fp:
        lines = fp.readlines()
        word = 'Message ID: '
        msgIDList = []
        msgTimeList = []
        for row in lines:
            # print('inside for loop')
            if word in row:
                msgTime = row.split('INFO')[0].strip() 
                msgID = row.split(word)[1].strip()
                msgIDList.append(msgID)
                msgTimeList.append(msgTime)
        # print('Message ID:', *msgIDList)
        # print('Message Timestamps:', msgTimeList[0])

        msgConsumed = len(msgIDList)
        if msgTimeList:
            t1 = datetime.strptime(msgTimeList[0], "%Y-%m-%d %H:%M:%S,%f")
            t2 = datetime.strptime(msgTimeList[-1], "%Y-%m-%d %H:%M:%S,%f")
            
            timeTaken = t2 -t1
            timeInSec = timeTaken.total_seconds()
            msgPerSec = msgConsumed / timeInSec
            # print('Message Consumed: ', str(msgConsumed))
            # print('Time taken: ', str(timeInSec))
            totalMsgConsumed += msgConsumed

            print('Average message/sec for consumer '+ str(i) +' : '+ str(msgPerSec))
            print('Message consumed by consumer '+ str(i) +' : '+ str(msgConsumed))
            avgPerConsumer.append(msgPerSec)
    i += 1

# print(*avgPerConsumer)
# avgThroughput = sum(avgPerConsumer)/numberOfConsumers
print("Average message/sec for total "+str(numberOfConsumers)+" consumers: " + str(sum(avgPerConsumer)))
print('Total msg consumed: '+str(totalMsgConsumed))