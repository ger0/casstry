# casstry
Cassandra project allowing for creation of some ordered lists. One can make a proposal as lists into such list. A proposal contains preferences expressed as list starting with favourite position and ending with the least favourite postion.
## Context
This project was created as a part of large scale distributed systems course at Poznań University of Technology. The purpouse of this project is to experience what challenges and tools are tied to designing NoSQL application with Cassandra. This project was supposed to be a console application with non-trivial design allowing temporary data inconsistency in case of network partition in distributed system.
## Issues
Inserting into proposals contains "if" statement which indeuces usage of lightweight tarnsations which are unwanted considering performance.