# Cage® class v. 2.7 (Cage file server v.3.0)
#   Parameters
# © A.S.Aliev, 2019

PAGESIZE = 64 * 2 ** 10  # 64Kb     size of one page in buffer

NUMPAGES = 2 ** 10  # 1024    number of pages in buffer

MAXSTRLEN = 64 * 2 ** 20  # 64Mb   max length (amount) of byte's data arrays
#   in read/write file operations

CAGE_SERVER_NAME = "cage_server"

DEFAULT_SERVER_PORT = "127.0.0.1:3570"  # "192.168.99.100:3570"  #  # default file server ip:port ("main" port)

ATTEMPTS_CONNECT = 3  # max. number of attempts to connect with each file server     
ATTEMPT_TIMEOUT = 10  # timeout after attempt to connect with file server (sec.)                 

ATTEMPTS_WAIT_RESPONSE = 5  # max. number of attempts to get response from server      
WAIT_RESPONSE = 3  # timeout for recieving common & client ports from server (sec.)

RESPONSE_TIMEOUT = 1000  # timeout get response from server (sec)
                                       
WRITE_THREAD =       False  #   True  #  or no threading while write pushed page

CACHE_FILE = "cage"  #  default name for cash during cage sleep

CACHE_FILE2 = "cage2"

SPLITTER=b"\x00" * 4        # split cage_id and JWT in client_id 

CAGE_SERVER_WWW ="127.1.0.0:3570"  # ":3570"# "cageserver.ddns.net:3570"

CAGE_DEBUG =  True  #  False #                                                  
