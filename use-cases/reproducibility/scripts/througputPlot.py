#!/bin/usr/python3

import matplotlib.pyplot as plt

fig = plt.figure()

# partition fixed to 10
# single group values
numberOfConsumers = [1,2,4,8,10]
avgThroughput = [1319.957761351637, 740.1043680635613*2 ,377.65841890275095*4, 187.08560459438922*8, 168.02201182833863*10]
# msgConsumed = ~10,000

# individual group values
# numberOfConsumers = [2,4,6,8]
# avgThroughput = [1376.6520867384816, 1729.293597482546, 1781.699702685235, 1538.1015638218346]
# msgConsumed = [20000, 39988, 59790, 79984]
plt.bar(numberOfConsumers,avgThroughput,color='blue',edgecolor='black')
# plt.xticks(numberOfConsumers,avgThroughput)
plt.xlabel('Number of consumers', fontsize=16)
plt.ylabel('Transfer throughput[images/sec]', fontsize=16)
plt.title('Transfer throughput of Kafka in one node',fontsize=20)
plt.show()

