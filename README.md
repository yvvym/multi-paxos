# Multi-paxos

To run the server and client with configureation of different test cases, call
```
    bash server_batch_mode.sh <number of replicas> <testfile name>
    bash client_batch_mode.sh <number of clients> <testfile name>
```

Specifically, the commands for running each test case is listed below:

* Normal operation
```
    bash server_batch_mode.sh 7 ../config/testcase1.json
    bash client_batch_mode.sh 5 ../config/testcase1.json
```

* The primary dies
```
    bash server_batch_mode.sh 7 ../config/testcase2.json
    bash client_batch_mode.sh 5 ../config/testcase2.json
```
* The new primary dies
```
    bash server_batch_mode.sh 7 ../config/testcase3.json
    bash client_batch_mode.sh 5 ../config/testcase3.json
```
* The skipped slot
```
    bash server_batch_mode.sh 7 ../config/testcase4.json
    bash client_batch_mode.sh 5 ../config/testcase4.json
```
* Simulating message loss
```
    bash server_batch_mode.sh 7 ../config/testcase5.json
    bash client_batch_mode.sh 5 ../config/testcase5.json
```

To check correctness, look into the `` exe/*.txt `` files for the saved message of each server and the ``logs/*.log`` files for the log of each server.