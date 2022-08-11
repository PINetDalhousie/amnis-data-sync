# Finanncial fraud detection

## Architecture

### Application Chain
![image](https://user-images.githubusercontent.com/6629591/184164122-a17cf4e4-6adf-4990-aca9-21a7d76ea110.png)

### Topology
![image](https://user-images.githubusercontent.com/6629591/184164321-bf5a49fb-a657-46d4-a4b3-88ed6c81172b.png)



## Queries  
  

  
## Operations
  

  
## Input details
     
## Running

As the training data is almost 307MB, didn't upload it as part of this project repository.

To train the model:
 ```sudo python3 main.py use-cases/app-testing/fraud-detection/input_only_spark.graphml --only-spark 1```

 To predict on testing data:
  ```sudo python3 main.py use-cases/app-testing/fraud-detection/input.graphml --nzk 1 --nbroker 2```
   
