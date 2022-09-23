#!/bin/bash

#run brokerScipt in gpu5
ssh -t ifath@qiang-gpu5.research.cs.dal.ca 'cd /users/grad/ifath/amnis-data-sync && bash -s < brokerScript.sh enp101s0f0 100 inputTopic outputTopic &'

# sleep 5

# #run sparkScript in gpu6
# ssh -t ifath@qiang-gpu6.research.cs.dal.ca 'cd /users/grad/ifath/amnis-data-sync && bash -s < sparkScript.sh enp101s0f0 1 inputTopic outputTopic &'

# sleep 5

# #run consumerScript in TB2
# ssh -t ifath@israat-testbed2.research.cs.dal.ca 'cd /users/grad/ifath/amnis-data-sync && bash -s < consumerScript.sh enp1s0np0 1 outputTopic &'

# sleep 2

# #run producerScript in TB1
# ssh -t ifath@israat-testbed1.research.cs.dal.ca 'cd /users/grad/ifath/amnis-data-sync && bash -s < producerScript.sh enp1s0np0 1 inputTopic &'

