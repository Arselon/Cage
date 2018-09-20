Cage System

Purpose
Support for remote access of client programs to files on servers using the cache in the client-side RAM. The system allows creation of distributed applications for processing files with optimized performance by caching most frequently used chunks of files in client-side memory.

Applications
- Developing specialized applications (i.e. mobile devices) that require distributed access to files in potentially unreliable networks leading to extended offline periods.
- Developing distributed NoSQL DBMS, when processing requests on several nodes of the network, and database fragments (tables) are also located on different servers - as an alternative to the traditional architecture of systems with SQL servers.
- Developing enterprise software, which demands redundancy and reliability of information storage.

Structure
The Cage system (implementation in Python 3) includes:
a) a set of functions that form programs for the file server,
b) class Cage - a tool for developers of client software.
Realization of remote Internet access:
TCP protocol implemented using  ZeroMQ library - the system for receiving and transmitting messages based on sockets.

Basic usage on client side
The foundation is the Cage class, which implements access to remote data in files on servers and buffers the "pages" of files used. Each page is an array of bytes of fixed length.
An instance of the Cage class can communicate with multiple servers simultaneously and access multiple files on each server. Thus, the data is being cached using client’s shared memory. Cache size is defined dynamically as a number of pages and their size upon creation of instance of the Cage class. For example, a cache of 1GB is 1000 pages of 1 MB, or 10 thousand pages of 100 KB, or 1 million pages of 1 KB. Number of pages and their size is selected depending on application.
An application can instantiate any number of Cage class objects.
Basic operations for clients:
- creating / deleting files on servers;
- opening / closing files with the formation of virtual "channels" of input-output.
- reading / writing byte arrays in files by referencing the position of the first byte and length, that is, similar to typical software interfaces available in most OS and programming languages.
Principles of buffering:
After a given amount of memory is exhausted, the new necessary pages displace the old ones by the principle of retirement with the minimum number of hits.
Buffering is especially effective in case of statistically uneven shared access, firstly, to different files, and, secondly, to fragments of each file. Such cases are typical for large database management systems, when the functional processing of requests occurs on the client side.
Two levels of work with data:
The client program can read and write data using Cage methods not only by data addresses - first byte and length (traditional level), but also by page numbers (lower level), or jointly at both levels.
Hibernation:
The function of "hibernation" ("sleep") for each instance of the Cage class is supported - it can be "minimized" (with disconnection from the servers) to a file on the client side and restored from this file (with the establishment of a new session with the servers). This allows for a significant reduction in traffic when the client program resumes its work, since all frequently used fragments of files will already be in the cache.
The ease of use of the system for developers is based on the fact that the result of opening a file is a number, it’s "channel number", which is referenced further in read / write methods.

Basic usage on server side
The server starts with two main processes:
"Connections" is the process for performing operations of establishing initial communication with clients and ceasing communication channel in case of failure in "Operation" process.
"Operations" is a process for executing client’s requests and closing communication session at the end.
Both processes are implemented as asynchronous infinite loops those sending and receiving messages (based on ZeroMQ sockets, multi process queues, and proxy objects).
The "Connections" process allocates a port for each client that will be used for receiving and transmitting . The number of ports is set when the server starts. Port to client relations are stored in shared memory between the main processes.
The "Operations" process supports all client operations with files on the server, with possibility of sharing resources. Several clients can share (quasi-parallel, as access is managed by locks) reading data from one file, when the "first" client permits it. Files can also be opened for use in exclusive mode.
Processing operations creating / deleting / opening / closing files on the server is performed in the process "Operations" strictly in sequence using the file subsystem OS.
To speed up the work with I/O, processing of arbitrary byte arrays with additional threads ("read-write" threads) is included (as part of the "Operations" process). The number of such streams at any time is equal to the total number of open files. Read / write requests from clients are submitted to the common queue, and first available thread is fetching a job from the top of this queue.
The "Operation" process is also monitoring the clients’ activity. It can stop their service by remote request or in case of timeout.
If the system is used in increased load condition, the "Operation" can be distributed into multiple processes.

Further development
It is planned to develop the system in two directions:
- implementation of server functions and Cage class in Java and C ++ languages;
- Creation of a NoSQL HTMS® DBMS that supports the storage and simplest processing of multidimensional tables (tensors) in a distributed network environment with the maintenance of explicit address references (on tables and individual rows of tables) within the "network" database model. The HTMS prototype has already been created by the author in C ++, and work is underway to modernize its data structures, algorithms, and translate it into Python.

