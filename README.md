## Cage - remote file access system

#### Remote file access on computers on the network. The system supports all major file operations (create, open, delete, read, write etc.) by exchanging transactions via TCP.

##### Tags: _remote, file system, file server, file sharing, file system api_

###  Field of application

The functionality of the system is effective in the following cases:

  * in native applications for mobile and embedded devices (smartphones, on-board control systems, etc.) requiring quick access to files on remote servers in conditions of probable temporary interruptions in the connection (with going offline);
  * in loaded DBMS, if request processing is performed on some servers, but data storage on others;
  * in distributed corporate networks requiring high speed data exchange, redundancy and reliability;
  * in complex systems with microservice architecture, where delays in the exchange of information between modules are critical.

###  Structure

Cage system includes two main parts:

  1. **Cageserver** \- a server-side program (Python functions with the main program), which runs on computers on the network whose files require remote access;
  2. Class **Cage** (in Python package cage_api), whose methods provide remote file operations in applications.

![Block diagram](https://business-on.herokuapp.com/static/images/article_images/cage_1.jpg)

#### Repository Arselon/Cage includes only Part 2: Cage API, that is used on the client side (in applications). 

###  Using the system on the client side

The methods of the Cage class replace the usual, "routine" operations of the
local file system: _creating, opening, closing, deleting_ files, as well as
_reading/writing data in binary format_. Conceptually, these methods are close
to the file functions of the C language, where file actions is done "through
the channels" of input/output.

In other words, the programmer does not work with methods of built-in file
functions or classes (for example, class **_io** in Python), but with methods
of class Cage.

When the Cage object is created (initialized), it establishes the initial
connection with the server (or several servers), is authorized by the client
identificator, and receives a confirmation with a dedicated port number for
all file operations. When Cage object is deleted, it issues a command to the
server to terminate the connection and close the files. Termination of
communication can also be initiated by the servers themselves.

A single Cage object can communicate with multiple files on multiple servers.
Communication parameters (server IP address or DNS, primary port for
authorization, path and file name) are set when creating the object.

Because each Cage object can handle multiple files at the same time, a common
memory space is used for buffering. The system improves read/write performance
by buffering frequently used by the client application file fragments in the
RAM cache (memory buffer).

Cache size - the number of pages and their size is set dynamically when
creating a Cage object. For example, 1 GB cache means 1,000 pages of 1 MB
each, or 10,000 pages of 100 KB, or 1 million pages of 1 KB. The choice of
page size and number is a specific optimization task for each application.

Client software can use any number of Cage objects with different settings
(memory buffer size, block (chunk) size, etc.). Can be used multiple Cage
objects at the same time to define different buffer memory settings depending
on how you access information in different files.

As a basic, the simplest buffering algorithm is used: after exhausting a given
amount of memory, new pages displace old ones on the principle of recycling
with a minimum number of hits. Buffering is especially effective in the case
of uneven (in a statistical sense) sharing, first, to different files, and,
secondly, to fragments of each file. To speed up the application for reading
data, you can enable a special mode for pushing pages to the file server in a
separate thread.

Cage class supports input/output not only by data addresses (specifying the
position and length of the array in a file), but also at a lower level - by
page numbers.

For Cage objects, the original _hibernation_ (sleep) function is supported.
They can be "minimized" (collapsed) to a local dump file on the client side
and quickly restored from this file (after resuming communication, when the
application restarts). For example, in case of disconnection with servers, or
when the application is stopped, etc. This makes it possible to significantly
reduce traffic when activating the client program after a temporary "offline",
as often used fragments of files will already be in application memory.

###  Server operation

**Cageserver** program can be run with an arbitrary number of ports, one of
which ("primary" or "main") is used only for authorization of all clients, and
the others - for data exchange.

The server starts as two main processes:

  1. **"Connections"** \- the process for establishing communication with clients and for its termination;
  2. **"Operations"** \- the process for execution of tasks (operations) of clients on working with files, and also for closing of communication sessions on commands of clients.

Both processes are are not synchronized and organized as endless cycles of
receiving and sending messages based on multi-process queues, proxy objects,
locks, and sockets.

The Connections process provides each client with one "secondary" port for
data transfer. The total number of secondary ports is set at server startup.
The correspondence between secondary ports and clients is stored in proxy
memory between processes.

The Operations process supports the separation of file resources, and several
different clients can read data from one file together ( _quasi-parallel_ ,
since access is controlled by locks) if it was allowed when the "first" client
initially opened the file.

Processing of commands (tasks) to create/delete/open/close files on a server
is performed in Operations process strictly sequentially using the file
subsystem of the server OS.

For general read/write acceleration, these tasks are performed in threads
created by the Operations process. The number of threads is usually equal to
the number of open files. But read/write tasks from clients are submitted to
the common shared queue and the first thread that is freed takes the task out
of the queue head. Special logic excludes data rewriting operations in server
memory.

The Operations process monitors the activity of clients and stops their
service both by their commands and when the inactivity timeout is exceeded.

To ensure reliability, Cageserver may keeps logs of all transactions. One
common log may contains copies of all messages from clients with tasks to
create/open/rename/delete files. For each working file, a separate log may
contains copies of messages with tasks for reading and writing data. Arrays of
written (new) data and arrays of data that were destroyed when overwriting can
be saved. These logs can provide the ability to both restore new changes to
backups and to "roll back" from the current content to the desired point in
the past.  
_Note: In the published version, there is no functionality for logging and
rollbacks, since it is in the debugging stage. It is also planned to improve
authentication mechanisms and control access levels to files._

###  Transaction formats

Messaging between servers and client applications is based on the
"Request–Reply" protocol of  [ZeroMq system](https://github.com/zeromq/pyzmq ). Servers as receivers wait
messages from senders - clients via TCP.

There are two transaction channels:

  1. To establish a connection. 
  

  2. To perform file operations. 

**Communication procedure.**

  1. Client requests a TCP connection from the socket on the server's main (primary) port (common to all clients).
  2. If the connection is established, the client requests the port number, allocated to it by the server for performing file operations:  
Request from client: ( { _ident. client_ }, "connect", { _ident. request_ })  
{ _ident. client_ } - a unique token for authorizing client access to file
server resources  
{ _ident. request_ } - the sequence number of the client's request to this
server (used for additional protection against attempts to interfere).

  3. The server responds: ({ _ident. client_ }, { _N port_ }, { _ident. request_ }, { _status_ } ). If { _status_ } = "connected", the the second element of the tuple contains the number of the secondary port for transaction exchange.
  4. The client requests a second TCP connection socket from the server on the secondary port.
  5. If the connection is established, the Cage object is ready to go.

**The procedure for the exchange of transactions.**

  * _For all operations except write/read:_  
Request from the client: ({ _operation_ }, { _ident. client_ }, { _op.1_ }, {
_op.2_ }, { _op.3_ }, { _ident. request_ })  
  { _operation_ } is the operation code, { _op.1_ }, { _op.2_ } and { _op.3_ }
is the operands that are specific to each operation  
"n" - create a new file, { _op.3_ } - path and file name  
"o" - open the file, { _op.3_ } - path and file name,{ _op.2_ } - opening
status: "wm" - full monopoly (read/write), "rs" - read-only, and shared read-
only for other clients, "ws" - read/write, and shared read-only for other
clients.  
"c" - close file { _op.2_ } - number of the file's "channel" (see below the
description of the Cage class)  
"u" - rename the file, { _op.2_ } - the new file name, { _op.3_ } - the path
and the old file name  
"d" - delete file, { _op.3_ } - path and file name  
"x" - get information (statistics) about all channels  
"i" - get information about the channel, { _op.2_ } - file channel number  
"t" - end of communication with the server  
"e" - execute of the script on the server, { _op.3_ } - text of a Python
script.  
After attempting to perform the requested operation, the server responds to
the client with a message in the same format, and if the result is successful,
the first element contains the operation code, if not successful - the
operation code + the character "e", for example, "ne", "oe", and the like. In
case of an error, the fourth element of the server response (instead of {
_op.3_ }) contains a detailed diagnostics of the error (or sequence of errors)
from the server.

  * _For write/read operations:_  
Reading data from a file and writing to a file are performed not in 2 steps,
like other operations (request-response), but in 4 steps.

  * For writing: 
    1. request from the client to write ("w", { _ident. client_ }, { _channel number_ }, { _(offset, length)_ },"", { _ident. request_ })
    2. server response about readiness to recieve data: (b"\x0F"\*4) - can continue, (b"\x00"\*4) - error 
    3. sending data to the server: ({ _binary array_ }), in which data and metadata are serialized
    4. confirmation from the data acquisition server and the result - is successful or failed.
  * For reading: 
    1. request from the client to read ("r", { _ident. client_ }, { _channel number_ }, { _(offset, length)_ },"", { _ident. request}_ )
    2. server response about readiness to send data: (b"\x0F"\*4) - can continue, (b"\x00"\*4) - error
    3. confirmation from the client that it is ready to accept data
    4. sending data to the client: ({ _binary array_ }), in which data and metadata are serialized, or contain information about the error.

###  Cage API

**class Cage** ( _cage_name = "...", pagesize = 0, numpages = 0, maxstrlen =
0, server_ip = {...}, wait = 0 , awake = False, cache_file = ""_ )

From this class objects are created that interact with file servers. Each
object (instance) of the Cage class for any Cageserver - is one independent
"client" in the network.

  * **cage_name** ( _str_ ) - the conditional name of the object used to identify clients on the server side; 
  * **pagesize** ( _int_ ) - size of one page of buffer memory (in bytes); 
  * **numpages** ( _int_ ) - number of pages of buffer memory; 
  * **maxstrlen** ( _int_ ) - maximum length of the byte string in write and read operations; 
  * **server_ip** ( _dict_ ) - a dictionary with the addresses of the servers used, where the _key_ is the _conditional name_ of the server (server name used in program code of the application), and the _value_ is a string with _real server address: "ip address:port" or " DNS:port "_. Matching names and real addresses is temporary, it can be changed; 
  * **wait** ( _int_ ) - time to wait for a responses from the server (in seconds); 
  * **awake** ( _boolean_ ) - the flag of the method of creating the object: _False_ \- if a new object is created, _True_ \- if the object is recovered from previously hibernated (default _False_ ); 
  * **cache_file** ( _str_ ) - local file name for hibernation. 

**Cage methods**

Cage. **file_create** ( _server, path_ ) - **create a new file**

  * _server_ \- the conditional server name; 
  * _path_ \- full path to the file on the server. 

R e t u r n s:

  * _True_ \- on success; 
  * _False_ \- for a system error (I/O, communication, etc.); 
  * _ReturnCode_ \- if the file was not created, but this is **not** an error:   
-1 - if a file with the same name already exists and is closed,   
-2 - if a file with the same name already exists and is opened by this client;   
"wm", "rs" or "ws" - if a file with the same name already exists and is opened
by another client in the corresponding mode (see the Cage.open method below).

Cage. **file_rename** ( _server, path, new_name_ ) - **rename the file**

  * _new_name_ \- new file name. 

R e t u r n s:

  * _True_ \- if the file was successfully renamed; 
  * _False_ \- if the file does not exist, or the file with the new name already exists,, or cannot be renamed due to blocking by another client, or due to a system error. 

Cage. **file_remove** ( _server, path_ ) - **delete file**

R e t u r n s:

  * _True_ \- if the file was deleted successfully; 
  * _False_ \- if the file does not exist, or cannot be deleted due to blocking by another client, or due to a system error. 

Cage. **open** ( _server, path, mod_ ) - **open file**

  * _mod_ \- file open mode:   
"wm" - exclusive (read and write);  
"rs" - read only, and shared read-only by other clients;  
"ws" - read/write, and shared read-only by other clients.  
Files in "rs" and "ws" modes are physically opened on the server only by the
first client (let's call it "owner"), for the second and subsequent clients
the server not opens the file physically, but uses it in the shared mode (that
is, it opened "virtually").

R e t u r n s:

  * _ReturnCode = fchannel_ \- if the file is open, fchannel - is a positive integer - "channel number" assigned or already existing (that is, the file was previously opened by this client); 
  * _False_ the file does not exist, or the file is already opened by this client in another mode, or it cannot be opened in the desired mode due to blocking by another client, or when the limit on the number of open files is exceeded, or when the system error. 

Cage. **close** ( _fchannel_ ) - **close the file**

  * _fchannel_ \- channel number. 

R e t u r n s:

  * _True_ \- if the file was successfully closed for the client (physically or "virtually"); 
  * _False_ \- if the file is not closed due to an error in the channel number or due to a system error.   
If the first client closes the file that other clients use in addition to it,
the server does not physically close the file, and the first in order
remaining client becomes the "owner".

Cage. **write** ( _fchannel, begin, data_ ) - **write the byte string to the
file**

  * _begin_ \- the first position in the file (from zero or more); 
  * _data_ \- string of bytes. 

R e t u r n s:

  * _True_ -if the recording was successful; 
  * _False_ in case of an error in the parameter values, or inability to record due to the file channel mode, or in case of a system error. 

Cage. **read** ( _fchannel, begin, len_data_ ) - **read the byte string from
the file**

  * _len_data_ \- string length (bytes). 

R e t u r n s:

  * _byte string_ \- if reading was successful; 
  * _False_ \- if there is an error in the parameter values, or with a system error. 

Cage. **remote** ( _server_ ) - **get general information about all channels
opened on the server**

R e t u r n s:

  * _dictionary with information_ \- if successful; 
  * _False_ in case of an error in the parameter value, or in case of a system error. 

Cage. **put_pages** ( _fchannel_ ) - **pushes from the buffer to the server
all pages of the specified channel** that have been modified. It is used at
those points in the algorithm when you need to be sure that all operations on
the channel are physically saved in a file on the server.

R e t u r n s:

  * _True_ \- if the recording was successful; 
  * _False_ \- if there is an error in the parameter value, or an inability to write due to the channel mode of the file, or with a system error. 

Cage. **push_all** () - **push from the buffer to the server all pages of all
channels** for the Cage class instance that have been modified. Used at those
points in the algorithm when you need to be sure that all operations on all
channels are saved on the server.

R e t u r n s:

  * _True_ \- if the recording was successful; 
  * _False_ \- if there is an error in the parameter value, or an inability to write due to the channel mode of the file, or with a system error. 
____________________
####  Copyright 2020 [Arslan S. Aliev](http://arslan-aliev.com)

#####  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this software except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

###### Cageserver v.3.0, Cage package v.2.6, readme.md red.15.02.2020

