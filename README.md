# Amazon Dynamo Implementation

This is a sample implementation of Amazon Dynamo using 5 android emulators as nodes.

# Design:

	1. Membership
	Every node knows every other node. This means that each node knows all other nodes in the system 
	and also knows exactly which partition belongs to which node; any node can forward a request to the 
	correct node without using a ring-based routing.
		
	2. Request routing
	Unlike Chord, each Dynamo node knows all other nodes in the system and also knows exactly which 
	partition belongs to which node.
	Under no failures, a request for a key is directly forwarded to the coordinator (i.e., the successor 
	of the key), and the coordinator is in charge of serving read/write operations.
	
	3. Quorum replication
	Linearizability is ensured by implementing a quorum-based replication used by Dynamo.
	The replication degree N is 3. This means that given a key, the key’s coordinator as well as the 
	2 successor nodes in the Dynamo ring  store the key.
	Both the reader quorum size R and the writer quorum size W is 2.
	The coordinator for a get/put request always contacts other two nodes and get a vote from each 
	(i.e., an acknowledgement for a write, or a value for a read).
	For write operations, all objects are versioned in order to distinguish stale copies from the most 
	recent copy.
	For read operations, if the readers in the reader quorum have different versions of the same object, 
	the coordinator picks the most recent version and returns it.
	
	4. Failure handling
	Just as the original Dynamo, each request is used to detect a node failure.
	If a node does not respond within the timeout interval, it is considered to have failed.
	Since the reader quorum size R and the writer quorum size W is 2, the system can handle 1 failure 
	without impact on availability and linearizability.
	When a coordinator for a request fails and it does not respond to the request, its successor is 
	contacted next for the request.
		
# Compilation:
	
	Import the project into Android studio and build apk.

# Testing:

	The tests are provided by professor Steve Ko as a part of the CSE 586: Distributed
	Systems course at the University at Buffalo.

	In bash, do the following to set-up the testing environment. 
	The files are present in the test/ directory

	1. Execute "python create_avd.py 5 <path_to_android_SDK>" to create 5 emulators.
	2. Execute "python run_avd.py 5" to start 5 android emulators. 
	3. Execute "python set_redir.py 10000" for port redirection.
	4. Execute "./simpledynamo-grading.linux <path_to_apk>" to start the tests.

# Test Cases:

	1. Testing basic ops
	This phase will test basic operations, i.e., insert, query, delete, @, and *. 
	This will test if everything is correctly replicated. There is no concurrency in 
	operations and there is no failure either.
	
	2. Testing concurrent ops with different keys
	This phase will test if your implementation can handle concurrent operations under 
	no failure.
	The tester will use independent (key, value) pairs inserted/queried concurrently on 
	all the nodes.
	
	3. Testing concurrent ops with same keys
	This phase will test if your implementation can handle concurrent operations with 
	same keys under no failure.
	The tester will use the same set of (key, value) pairs inserted/queried concurrently 
	on all the nodes.
	
	4. Testing one failure
	This phase will test one failure with every operation.
	One node will crash before operations start. After all the operations are done, the 
	node will recover.
	This will be repeated for each and every operation.
	
	5.Testing concurrent operations with one failure
	This phase will execute operations concurrently and crash one node in the middle of 
	the execution. After some time, the failed node will also recover in the middle of 
	the execution.
	
	6. Testing concurrent operations with one consistent failure
	This phase will crash one node at a time consistently, i.e., one node will crash then 
	recover, and another node will crash and recover, etc.
	There will be a brief period of time in between the crash-recover sequence.
