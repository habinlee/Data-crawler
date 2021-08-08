# Data-crawler
Zookeeper Data Structure
![image5](https://user-images.githubusercontent.com/88265967/128624537-bbbdcc1f-8416-4e95-94cf-55b17d549adc.png)

- Setting : System setting values
  - Status : Status of the whole system - whether is is running, paused, done
  - Interval : The interval between each crawling action of a worker
  - Activeworker : The number of workers
- Agent : The nodes that represent the actual instances created
  - Workers : Parent node that controls the actual worker instances
  - Controller : Node for the controller instance(server)
- Crawl : Crawl process / state information stored
  - Workers : Detailed settings for workers - state of each worker, ip, page the worker has been assigned
  - Pages : The pages of the target website that needs to be crawled
  - Seed : Seed address of the target


Crawl Action
![image3](https://user-images.githubusercontent.com/88265967/128624571-cf9921e2-9d41-448a-8a4b-07ec8128076b.png)

- Uses the Controller Node value as the command
- Changes in the status of the controller / worker nodes are how they communicate on basic operations
- Commands :
  - Set : Sets nodes with necessary values
  - Ready : Sets watchers and makes the system into a ready state
  - Start : Sets the controller / worker node status to RUNNING and starts the job
  - Stop : Status is set to STOP and the job is paused
  - Interval : Resets the interval to the value the user input
  - Active : Resets the number of active workers to the value the user input
  - Reset : Resets the system to the same state as Set - Ready
  - Quit : Status is set to QUIT and the process is terminated safely


Crawl Distribution / Resume Process
![image4](https://user-images.githubusercontent.com/88265967/128624589-18d9090e-aac1-48e1-bde7-d5c7891e93a0.png)

- Pages are allocated to workers from the locking queue provided by Zookeeper
- The information on a page is crawled and stored in the storage. When a worker instance is deleted or blocked during a job, the job is returned and given to a new worker. The new worker works on the page again.


Handling IP Block / Disconnection Situations
![image2](https://user-images.githubusercontent.com/88265967/128624608-204abb63-b1f9-484f-9751-2b7cddd0c883.png)

- The controller is responsible of requesting new external IPs, assigning them to new worker instances and dequeuing IPs


Controller Error / Network Disconnection Handling
![image1](https://user-images.githubusercontent.com/88265967/128624738-e0d10041-9cda-4eb8-a259-f49530b342ab.png)

- When the controller goes down, the user has to restart the program or restore the instance directly
- Workers are suspended at controller error situations

