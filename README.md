# Network Programming Assignment 1

Mitesh Kumar, Alex Fernandez

## Usage

Run `python3 hw3.py <nodeid> <port> <k>`

Example: `python3 hw3.py 0 9000 2`

## Overall Description

This program is broken up into 3 parts: user input handlers, remote procedure call handlers, and 
abstractions for internal data structures to provide the k-buckets and k-closest query functionality.

We utilize a linked dictionary to implement a fast least recently used eviction based key-value store,
as well as a binary trie to implement fast queries to find k elements that are closest to a node. 

The LRUCache has O(1) time complexity on all basic operations. The binary trie has an approximate O(K + log(NK)) time complexity
on the k-closest query.

## Issues Encountered

- Pseudo-code given in the hand-out is very difficult to understand
- Had to write a script to parse and run automated tests in order to efficiently debug our solution
- Took a lot of time to fix small inconsistencies in the ordering of our operations

## Team work

Time spent: 12 total between team members

Alex Fernandez: Created basic working version of assignment
Mitesh Kumar: Created basic working version of assignment