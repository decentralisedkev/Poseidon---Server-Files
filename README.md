# ReadMe

## Usage

In order to run, set the block counter in both txt files to the desired block height and run using a cron job.

## Why

The server files ensure that if only one of the specified nodes in the list are in sync, then it is also in sync.
This way, your program will not rely on one node to get the utxo information needed. This has not been implemented for the transer nodes.

In order to implement for the transfer nodes, one must run the invocation transactions through a VM and store the results for each transfer type.

## Usability

The database being used is dynamodb, however any noSQL/SQL database will work with this implementation.


