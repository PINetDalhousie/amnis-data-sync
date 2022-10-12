#!/bin/usr/python3

import matplotlib.pyplot as plt

fig = plt.figure()

numberOfConsumers = [1,2,4,8,10,16]
avgThroughput = [6438.606657652437, 12070.992524440138 ,22377.276640005733, 32338.656802826372, 31212.736600529177, 31750.23439558111 ]

plt.bar(numberOfConsumers,avgThroughput,color='blue',edgecolor='black')
# plt.xticks(numberOfConsumers,avgThroughput)
plt.xlabel('Number of consumers', fontsize=16)
plt.ylabel('Transfer throughput[images/sec]', fontsize=16)
plt.title('Transfer throughput of Kafka in one node',fontsize=20)
plt.show()

