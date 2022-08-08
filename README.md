# Local and Global Validation for Optimistic Concurrency Control in a Distributed-Database System

In this project an optimistic approach to concurrency control has been implemented. We have simulated a Distributed system by creating four sites having a fully replicated database and implemented local and global validation. In order to achieve this task, we have made use of data-structures like Dynamic Conflict Graphs, set of committed semi-committed and active transactions at each site. The project that we have created will be able to handle multiple concurrent transactions at different sites and will still leave the database at every site in a consistent state.

## Get Started
- Download or clone this git repository.
- Open the project using any IDE and make sure all the requirements and dependencies stated below are satisfied.
- In <a href="https://github.com/vsehra04/CS542_OCC_DistributedDatabase/blob/main/src/main/java/occ/DistributedDatabase.java">DistributedDatabase.java</a> include the desired test cases in the given space. Sample test cases have been provided <a href="https://github.com/vsehra04/CS542_OCC_DistributedDatabase/blob/main/test%20cases.txt">here</a>(text cases.txt).
- Build and run DistributedDatabase.java
- Note : Make sure to stop the code after getting the desired result as the program will not stop on its own.

## Requirements and Dependencies
- Java 11
- Gradel 7.1
- JUnit Jupiter 5.8.1
