#!/bin/bash
req = '1fathemeL123#'
tb1 = 'ifath@israat-testbed1.research.cs.dal.ca'
tb2 = 'ifath@israat-testbed2.research.cs.dal.ca'
gpu5 = 'ifath@qiang-gpu5.research.cs.dal.ca'
gpu6 = 'ifath@qiang-gpu6.research.cs.dal.ca'

#run brokerScipt in gpu5
# ssh -f $gpu5 'cd /users/grad/ifath/amnis-data-sync && bash -s < brokerScript.sh enp101s0f0 100 inputTopic outputTopic'
sshpass -vvv -p $req ssh 'bash -s < /users/grad/ifath/amnis-data-sync/brokerScript.sh enp101s0f0 100 inputTopic outputTopic'

# sleep 5

# #run sparkScript in gpu6
# ssh -f $gpu6 'cd /users/grad/ifath/amnis-data-sync && bash -s < sparkScript.sh enp101s0f0 1 inputTopic outputTopic'

# sleep 5

# #run consumerScript in TB2
# ssh -f $tb2 'cd /users/grad/ifath/amnis-data-sync && bash -s < consumerScript.sh enp1s0np0 1 outputTopic'

# sleep 2

# #run producerScript in TB1
# ssh -f $tb1 'cd /users/grad/ifath/amnis-data-sync && bash -s < producerScript.sh enp1s0np0 1 inputTopic'

# # echo 'Simulation done.'

