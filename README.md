This is the final project for Advanced Databases Systems.

Topic : Implement a distributed database, complete
with multiversion concurrency control, deadlock detection, replication, and failure recovery. 

Major Functions/Components of Project are:
  •	Transaction Manager (TM)
  •	Database Manager(DM)
  •	Lock Manager(LM)
  •	Failure/Recovery Manager(FRM)
  •	Cycle Deadlock Detector


Individual functionalities of these components will be:

  1.	Transaction Manager(TM)

  Transaction manager does the following jobs:

    •	Sending incoming operation to transaction parser.
    •	Keeps list of active sites in database.
    •	Keeps variable-site availability information.
    •	Keeps list of active transactions.
    •	Keeps track of locks that all ongoing transactions are holding.
    •	Works closely with Failure/Recovery Manager to keeps track of aborted transactions and waiting operations.
    •	Time Stamping
    •	The transaction manager also consists of the Transaction Parser which takes the instructions to be executed and converts them to instructions that the Transaction Manager understands.


  2.	Database Manager(DM)

    Database Manager keeps track of the site status, data, allowed transactions and locks held by transactions on data. It maintains consistent state of database and takes care of abort, Commit and recovery. 

  3.	Lock Manager(LM):
    Lock manager handles all aspects related to locks. It enables locking, disables locking and ensures gives consent about accessing particular fields in database.

  4.	Failure/Recovery Manager(FRM):

    •	Failure Recovery Manager is meant to recover from failed state. 
    •	It works closely with the transaction manager. When a site fails, it takes the list of operations on the site and aborts all the transactions for those operations and keeps a record of it. 
    •	Updates the information regarding transactions and locks.

5.	Cycle Deadlock Detector

    •	As soon as the transactions are put in wait status, the transaction and for whom this transaction is waiting for, this both information is sent here which starts creating a graph.
    •	At every iteration, it checks if a cycle is being formed by the waiting transactions. If yes, then youngest transaction is aborted. 

Algorithms to be used:

  •	Available Copies using two phase locking
  •	Deadlock detection by cycle detection and aborting the youngest transaction in the cycle
  •	Recovery Protocol
