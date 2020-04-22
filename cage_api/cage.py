# Cage® class v. 2.7 (Cage file server v.3.1)
# © A.S.Aliev, 2019


import pickle
import time
import threading
import queue

import zmq
import jwt

from cage_par_cl import *
from cage_err import *

from cage_page import *
from cage_channel import *
from thread_write_page import *

Mod_name = "*" + __name__

# ---------------------------------------------



class Cage:
    def __init__(
        self,
        Kerr=[],  # list of tuples with error descriptors, normally empty
        cage_name="",  # name, used for login in servers
        pagesize=0,  # buffer page size (bytes)
        numpages=0,  # number of pages in common buffer of cage instance
        maxstrlen=0,  # maximum length of any object (bytes)
        server_ip={},  # dict. of servers connecting throw ZeroMQ:
        # server alias name -> ip address:port
        wait=0,  # time to wait connection with file server socket (sec.)
        awake=False,
        cache_file=CACHE_FILE,
    ):

        global WRITE_THREAD
        self.awake = awake
        self.cache_file = cache_file
        self.pagesize = int(pagesize)
        self.numpages = int(numpages)
        self.maxstrlen = int(maxstrlen)
        self.server_ip = server_ip
        self.cage_name = cage_name
        self.wait = int(wait)
        self.asleep = False
        if not self.awake:
            if self.pagesize == 0:
                self.pagesize = PAGESIZE
            if self.numpages == 0:
                self.numpages = NUMPAGES
            if self.maxstrlen == 0:
                self.maxstrlen = MAXSTRLEN
            if self.server_ip == {}:
                self.server_ip = {"default_server_and_main_port": DEFAULT_SERVER_PORT}
            if self.wait == 0:
                self.wait = WAIT_RESPONSE

        self.obj_id = id(self)
        self.pr_create = time.time()
        self.zero_page = b"\x00" * self.pagesize

        # dict. for keeping of ZeroMQ "client" objects:
        self.clients = {}  # server conditional name -> ZMQ object

        # { 'server name' : ( Common socket, Temp_socket, Temp_socket_thread) }

        self.set_act_serv = {}  # self.set_act_serv = set( self.clients.keys() )
        # after sleep and before wake up
        if self.awake:
            if not self.wakeup1(Kerr):
                set_err_int(
                    Kerr,
                    Mod_name,
                    "__init__ " + self.cage_name,
                    1,
                    message="Error during download cache memory."
                    '\n and Cage "%s" NOT created' % self.cage_name,
                )
                raise CageERR(
                    "01 CageERR   Error during download cache memory."
                    '\n and Cage "%s" NOT created' % self.cage_name
                )
            old_Kerr = self.uplog["Kerr"]
            if is_err(old_Kerr):
                pr(" Errors before sleep :" + str(old_Kerr))
            if len(self.uplog) > 1:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "__init__ " + self.cage_name,
                    2,
                    message="There are differences in the parameter values."
                    '\n and Cage "%s" NOT created' % self.cage_name,
                )
                raise CageERR(
                    "02 CageERR   There are differences in the parameter values."
                    '\n and Cage "%s" NOT created' % self.cage_name
                )

        else:
            # dict. with index for fast access:
            self.hash2nat = {}  # (no. of page in file, channel) -> buffer page

            # dict. for renumerate session's cage channels numbers into
            # session's servers files  channels (unique files "numbers"):
            self.cage_ch = (
                {}
            )  # cage channel number -> (server, server internal channel number)

            #  page descriptor's dict. initialization
            self.binout = [
                {
                    "nf": -1,  # unique cage "channel" number for each opened
                    # file among all servers - range ( 0 : maxchannels-1)
                    "prmod": False,  # flag - page was modified or no in buffer
                    "nbls": -1,  # physical no. of relevant page in file
                    "kobs": 0,  # number of requests to page
                    "prty": 0,  # page priority ( future reserve)
                    "time": 0,  # page last get/put time
                }
                for i in range(self.numpages)
            ]

            # page's buffer initialization
            self.masstr = [self.zero_page for i in range(self.numpages)]

            # stat. total counters (for cage lifetime)
            self.kobr = 0  # number of requests to cage
            self.kzag = 0  # number of pages downloads from files
            self.kwyg = 0  # number of pages uploads to files

            self.num_cage_ch = 0  # number of last created cage channel

            self.req_id = 0  # number of last request to servers (common for all)

            if self.cage_name == "":
                self.client_id = str(self.obj_id) + str(
                    self.pr_create
                )  # secure id. for access to servers from
                # cage instance
            else:
                self.client_id = self.cage_name            

            cage_id_and_JWT = self.client_id.encode('utf-8')
            pos_splitter=cage_id_and_JWT.find( SPLITTER)
            jwtoken= None

            self.payload={}

            """ 
                        cl_name=  payload [ 'name']
                        cl_surname= payload [ 'surname'] 
                        cl_email= payload [ 'email']
                        token_datetime= payload [ 'iat'] 
                        cl_permission= payload [ 'permission']
                        token_issuer=payload [ 'iss'] 
            """

            if pos_splitter > -1 and len(cage_id_and_JWT) > pos_splitter +4:
                jwtoken = cage_id_and_JWT[ (pos_splitter+4): ].decode('utf-8')
                try:
                    self.payload = jwt.decode(jwtoken, algorithm='HS256', verify=False)
                except InvalidTokenError as err:
                    raise CageERR(
                        "03 CageERR   Cage name contains invalid JW token, error: %s" % err
                    )
                if pos_splitter == 0:
                    self.cage_id= jwtoken.decode('utf-8')
                else:
                    self.cage_id= cage_id_and_JWT[ : pos_splitter].decode('utf-8')

                if  'permission' not in  self.payload  or \
                    self.payload['permission'] not in ('low', 'standard', 'high',  'admin'):
                        raise CageERR(
                            "04 CageERR   Payload in JW token invalid."
                        )                    
            else:
                self.cage_id=  self.client_id
                self.payload = {'JWT': None, 'permission' : 'low'}

            if self.payload['permission'] == 'low' :
                WRITE_THREAD = False

            if WRITE_THREAD :
                self.req_id_thread = 0   # number of last request to servers for write thread
            else:
                 self.req_id_thread = None

        cerr = False
        try:
            self.context = zmq.Context()
        except zmq.ZMQError as err:
            set_err_int(
                Kerr,
                Mod_name,
                "__init__ " + self.cage_id,
                3,
                message="ZMQ context NOT started with error: %s" % err
                + '\n and Cage "%s" NOT created' % self.cage_id,
            )
            cerr = True
        if cerr:
            cerr = False
            raise CageERR(
                "05 CageERR   ZMQ context NOT started with error: %s" % err
                + '\n and Cage "%s" NOT created' % self.cage_id
            )

        self.context.setsockopt(zmq.LINGER, 0)

        if not self.bind(self.context, Kerr):
            set_err_int(
                Kerr,
                Mod_name,
                "__init__ " + self.cage_id,
                4,
                message='No ZMQ connections established and Cage "%s" NOT created.'
                % self.cage_id,
            )
            raise CageERR(
                '06 CageERR   No ZMQ connections established and Cage "%s" NOT created.'
                % self.cage_id
            )

        if WRITE_THREAD :

            self.Pages_to_write = queue.Queue()
            self.Pages_clean = queue.Queue()

            self.lock_write = threading.Lock()
            self.lock_memory = threading.Lock()

            cerr = False
            try:
                self.context_thread = zmq.Context()
            except zmq.ZMQError as err:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "__init__ " + self.cage_id,
                    3,
                    message="ZMQ context_thread NOT started with error: %s" % err
                    + '\n and Cage "%s" NOT created' % self.cage_id,
                )
                cerr = True
            if cerr:
                cerr = False
                raise CageERR(
                    "07 CageERR   ZMQ context_thread NOT started with error: %s" % err
                    + '\n and Cage "%s" NOT created' % self.cage_id
                )

            self.context_thread.setsockopt(zmq.LINGER, 0)

            if not self.bind_thread(self.context_thread, Kerr):
                set_err_int(
                    Kerr,
                    Mod_name,
                    "__init__ " + self.cage_id,
                    4,
                    message='No ZMQ thread connections established and Cage "%s" NOT created.'
                    % self.cage_id,
                )
                raise CageERR(
                    '08 CageERR   No ZMQ thread connections established and Cage "%s" NOT created.'
                    % self.cage_id
                )
        #
        if self.awake:

            problem_serv = self.wakeup2(Kerr)
            # pr(' problem_serv='+str( problem_serv))
            if problem_serv == True:
                pass
            else:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "__init__ " + self.cage_id,
                    5,
                    message="Error reopening mandatory channel %d ( %s ) on server %s when wake up."
                    % problem_serv
                    + '\n and Cage "%s" NOT created' % self.cage_id,
                )
                raise CageERR(
                    "09 CageERR   Error reopening mandatory channel %d ( %s ) on server %s when wake up."
                    % problem_serv
                    + '\n and Cage "%s" NOT created' % self.cage_id
                )
            pr(
                '  Cage "%s" WOKE UP'
                % (self.cage_id,)
            )
        else:
            pr(
                ' Cage "%s" CREATED'
                % (self.cage_id,)
            )

        if WRITE_THREAD :

            # start write page thread

            self.thr = threading.Thread(
                target=page_write,
                daemon=True,
                args=(
                    Kerr,
                    self.pagesize,
                    self.clients,
                    self.hash2nat,
                    self.cage_ch,
                    self.binout,
                    self.masstr,
                    self.client_id,
                    self.cage_name,
                    self.Pages_to_write,
                    self.Pages_clean,
                    self.lock_write,
                    self.lock_memory,
                    self.req_id_thread,
                ),
            )

            self.lock_write.acquire()
            self.thr.start()

        servs = ""
        for serv in self.clients:
            servs += ' "%s" on %s' % (serv, self.server_ip[serv]) + "\n"
        pr(" Servers connected:\n %s" % servs)
        # pr (str(self.clients))
        #time.sleep(0.1)

    # ------------------------------------------------------------

    # open clients ZeroMQ sockets for specified servers
    def bind(self, zmq_context, Kerr):

        for serv in self.server_ip:

            if serv in self.set_act_serv:
                mandatory_connection = True
            else:
                mandatory_connection = False

            p = self.server_ip[serv].find(":")
            host = self.server_ip[serv][:p]
            common_port = self.server_ip[serv][p + 1 :]

            # 1 step connect with common port of server

            if not  WRITE_THREAD  :
                self.clients[serv] = [False, False]
            else:
                self.clients[serv] = [False, False, False]

            common_sock = zmq_context.socket(zmq.REQ)

            for at1 in range(ATTEMPTS_CONNECT):

                try:
                    common_sock.connect("tcp://%s:%s" % (host, common_port))
                    # socket REQ type
                except zmq.ZMQError as err:
                    pr(
                        'Cage "%s". Common socket server %s (%s : %s) '
                        % (self.cage_id, serv, host, common_port)
                        + "\n temporarily not connected with ZMQ error: %s . Waiting ..."
                        % err
                    )
                    Error = str(err)
                    time.sleep(ATTEMPT_TIMEOUT)
                    continue

                else:
                    if not WRITE_THREAD :
                        self.clients[serv] = [common_sock, False]
                    else:
                        self.clients[serv] = [common_sock, False, False]       
                    pr(
                        'Cage "%s". Common socket for communication with server %s (%s : %s) READY.'
                         % (self.cage_id, serv, host, common_port)
                    )
                    break

            if self.clients[serv][0] == False:
                common_sock.close()

                if mandatory_connection:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "bind " + self.cage_id,
                        1,
                        message='Cage "%s". Common socket server %s (%s : %s) '
                        % (self.cage_id, serv, host, common_port)
                        + "\n NOT connected with ZMQ error: %s . Connection with server failed."
                        % Error
                        + "\n Connection has mandatory status, therefore Cage can not be created",
                    )
                    del self.clients
                    return False    
                else:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "bind " + self.cage_id,
                        2,
                        message='Cage "%s". Common socket server %s (%s : %s) '
                        % (self.cage_id, serv, host, common_port)
                        + "\n NOT connected with ZMQ error: %s . Connection with server failed."
                        % Error
                    )
                    del self.clients[serv]
                    continue

            # 2 step connect with temp port of server - for i/o server messaging

            self.req_id += 1
            first_request = pickle.dumps((self.client_id, "connect", self.req_id))
            # pr ( 'client %s, first_request = %s '% (cl_name, str (pickle.loads ( first_request) ) ) )

            for at2 in range(ATTEMPTS_WAIT_RESPONSE):

                try:
                    self.clients[serv][0].send(first_request, zmq.DONTWAIT)

                except zmq.ZMQError as err:
                        # send() in non-blocking mode, it raises zmq.error.Again < if err.errno == zmq.EAGAIN: > to inform you,
                        # that there's nothing that could be done with the message and you should try again later.
                    pr(
                        'Cage "%s". First request to server %s (%s : %s) '
                        % (self.cage_id, serv, host, common_port)
                        + "\n failed. Waiting to resend ..."
                    )
                    time.sleep(RESPONSE_TIMEOUT)
                    continue

                else:
                    # get client port for file processing
                    try:
                        event = self.clients[serv][0].poll(timeout=WAIT_RESPONSE *ATTEMPTS_WAIT_RESPONSE)

                    except zmq.ZMQError:
                        pr(
                            'Cage "%s". First response from server %s (%s : %s) '
                            % (self.cage_id, serv, host, common_port)
                            + "\n  not recieved. Fail server connection ..."
                        )
                        break

                    else:
                        first_response = pickle.loads(self.clients[serv][0].recv())
                        # pr ( 'client %s, first_response = %s'% (cl_name, str(first_response) ) )     
                        if first_response[0] != self.client_id or first_response[2] != self.req_id:
                            pr(
                                'Cage "%s". First response from server %s (%s : %s) '
                                % (self.cage_id, serv, host, common_port)
                                + "\n  invalid. Program error. Fail server connection ..."
                            )
                            break

                        status = first_response[3]

                        if status == "connected":
                    
                            port_client = str(first_response[1])

                            try:
                                temp_client = zmq_context.socket(zmq.REQ)
                                temp_client.connect("tcp://%s:%s" % (host, port_client))
                            except zmq.ZMQError as err:
                                pr(
                                    'Cage "%s". Client\'s socket server %s (%s : %s) '
                                    % (self.cage_id, serv, host, port_client)
                                    + " not connected with client port. ZMQ error: %s . Waiting ..."
                                     % err
                                )
                                time.sleep(WAIT_RESPONSE)
                                continue

                            else:
                                self.clients[serv][1] = temp_client
                                pr(
                                    'Cage "%s". Client\'s socket server %s (%s : %s)'
                                    % (self.cage_id, serv, host, port_client)
                                    +  ' CONNECTED to temporary client\'s port for files operations.'
                                )
                                break                     
                        else:
                            pr(
                                'Cage "%s". Client\'s socket server %s (%s : %s ) '
                                    % (self.cage_id, serv, host, '*undefined*')
                                    + "with port for files operations NOT connected. Waiting ..."
                            )
                            time.sleep(WAIT_RESPONSE)
                continue                             

            if  self.clients[serv][1] == False:
                common_sock.close()
                if mandatory_connection:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "bind " + self.cage_id,
                        4,
                        message='Cage "%s". Server %s (%s : %s) '
                        % (self.cage_id, serv, host, common_port)
                        + "\n NOT connected with port. Failed server"
                        + "\n has mandatory status, therefore Cage can not be created",
                    )
                    del self.clients
                    return False
                else:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "bind " + self.cage_id,
                        5,
                        message='Cage "%s". Server %s (%s : %s) '
                        % (self.cage_id, serv, host, common_port)
                        + "\n NOT connected during port error."
                    )
                    del self.clients[serv]

            continue

        if len(self.clients) == 0:
            return False
        else:
            return True

 # ------------------------------------------------------------

    # open clients ZeroMQ socket for threads for specified servers
    def bind_thread(self, zmq_context, Kerr):

        for serv in self.clients:

            p = self.server_ip[serv].find(":")
            host = self.server_ip[serv][:p]
            common_port = self.server_ip[serv][p + 1 :]

            self.req_id += 1
            second_request = pickle.dumps(("@$&%"+self.client_id, "connect", self.req_id))
            # pr ( 'client %s, second_request = %s '% (cl_name, str (pickle.loads ( second_request) ) ) )

            for at4 in range(ATTEMPTS_WAIT_RESPONSE):

                try:
                    self.clients[serv][0].send(second_request, zmq.DONTWAIT)

                except zmq.ZMQError as err:
                        # send() in non-blocking mode, it raises zmq.error.Again < if err.errno == zmq.EAGAIN: > to inform you,
                        # that there's nothing that could be done with the message and you should try again later.
                    pr(
                        'Cage "%s". Second request to server %s (%s : %s) '
                        % (self.cage_id, serv, host, common_port)
                        + "\n failed. Waiting to resend ..."
                    )
                    time.sleep(RESPONSE_TIMEOUT)
                    continue

                else:
                    # get client port for file processing
                    try:
                        event = self.clients[serv][0].poll(timeout=WAIT_RESPONSE *ATTEMPTS_WAIT_RESPONSE)

                    except zmq.ZMQError:
                        pr(
                            'Cage "%s". Second response from server %s (%s : %s) '
                            % (self.cage_id, serv, host, common_port)
                            + "\n  not recieved. Fail server connection ..."
                        )
                        break

                    else:
                        second_response = pickle.loads(self.clients[serv][0].recv())
                        # pr ( 'client %s, second_response = %s'% (cl_name, str(second_response) ) )     
                        if second_response[0] != self.client_id or second_response[2] != self.req_id:
                            pr(
                                'Cage "%s". second response from server %s (%s : %s) '
                                % (self.cage_id, serv, host, common_port)
                                + "\n  invalid. Program error. Fail server connection ..."
                            )
                            break

                        status = second_response[3]

                        if status == "connected":
                    
                            port2_client = str(second_response[1])

                            try:
                                temp_client = zmq_context.socket(zmq.REQ)
                                temp_client.connect("tcp://%s:%s" % (host, port2_client))
                            except zmq.ZMQError as err:
                                pr(
                                    'Cage "%s". Client\'s socket server %s (%s : %s) '
                                    % (self.cage_id, serv, host, port2_client)
                                    + " not connected with second client\'s port. ZMQ error: %s . Waiting ..."
                                     % err
                                )
                                time.sleep(WAIT_RESPONSE)
                                continue

                            else:
                                self.clients[serv][2] = temp_client
                                pr(
                                    'Cage "%s". Client\'s socket server %s (%s : %s)'
                                    % (self.cage_id, serv, host, port2_client)
                                    +  ' for thread files operations CONNECTED with second client\'s port.'
                                )
                                break                     
                        else:
                            pr(
                                'Cage "%s". Client\'s socket server %s (%s : %s ) '
                                    % (self.cage_id, serv, host, '*undefined*')
                                    + "for thread files operations NOT connected. Waiting ..."
                            )
                            time.sleep(WAIT_RESPONSE)
                continue                             

            if  self.clients[serv][2] == False:
                set_warn_int(
                        Kerr,
                        Mod_name,
                        "bind_thread " + self.cage_id,
                        1,
                        message='Cage "%s". Server %s (%s : %s) '
                        % (self.cage_id, serv, host, common_port)
                        + "\n NOT connected with second port for thread file operations."
                    )
            continue

        return True

    # ------------------------------------------------------------

    def get_page(
        self, fchannel, fpage, Kerr  # cage channel  # physical page number in file
    ):
        return get_p(self, fchannel, fpage, Kerr=[])

    def put_pages(self, fchannel, Kerr=[]):
        return put_p(self, fchannel, Kerr)

    def mod_page(self, nsop, Kerr=[]):
        return mod_p(self, nsop, Kerr)

    def push_all(self, Kerr=[]):
        return push_p(self, Kerr)

    def refresh(self, Kerr=[]):
        return reload_p(self, Kerr)

    # ------------------------------------------------------------

    # create new file on server. if success - file be closed
    def file_create(self, server="default_server_and_main_port", path="", Kerr=[]):

        kerr = []
        rc = f_create(self, server, path, kerr)
        if rc == True:

            time.sleep(0.01)

            return True

        elif rc != False and rc == -1:
            #   file already exist and not opened
            set_warn_int(
                Kerr,
                Mod_name,
                "file_create " + self.cage_id,
                1,
                message="File  %s  already exist and not opened." % path,
            )
            return -1

        elif rc != False and rc == -2:
            #   file already exist and  opened by this client !
            set_warn_int(
                Kerr,
                Mod_name,
                "file_create " + self.cage_id,
                2,
                message="File  %s  already exist and opened by this client." % path,
            )
            return -2

        elif rc != False:
            #   file already exist and opened by another client
            set_warn_int(
                Kerr,
                Mod_name,
                "file_create " + self.cage_id,
                2,
                message='File  %s  already exist and opened by another client with mode = " %s ".'
                % (path, rc),
            )

           # time.sleep(0.1)

            return rc  # mode of opened file

        else:  # if rc == False
            if (
                kerr[0][3] == "f_create " + self.cage_id
            ):  # Cage client error (generated by cage_channel.f_create)
                if CAGE_DEBUG:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_create " + self.cage_id,
                    3,
                    message="Internal error in cage before file  %s  creation on server %s. \n"
                    % (path, server),
                )
                #  kerr[0][4]  codes:           1:  server with specified name is not accessible
                #  2:  server with specified name is not connected
                #  3:  file path not specified
                #  4:  connection problem

            elif kerr[0][3] == "join " + self.cage_id and kerr[0][4] in (
                "5",
            ):  #  connection error   (generated by cage.join)
                if CAGE_DEBUG:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_create " + self.cage_id,
                    4,
                    message='Connection problem with server "%s" .' % server,
                )

            elif kerr[0][3] == "new_f" and kerr[0][4] in (
                "4",
                "5",
            ):  #  system file OS error on server    (generated by Cage Server)
                if CAGE_DEBUG:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_create " + self.cage_id,
                    5,
                    message='OS file system error in file server "%s" . File possibly not created.'
                    % server,
                )
                #  4:  file OS open error
                #  5:  file OS close error
            else:  #  internal error
                if CAGE_DEBUG:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_create " + self.cage_id,
                    6,
                    message="Error during file  %s  creation. rc = %s\n"
                    % (path, str(rc)),
                )
            return False

    # ------------------------------------------------------------

    # open file

    def open(self, server="default_server_and_main_port", path="", Kerr=[], mod="wm"):

        if mod not in {"rm", "rs", "wm", "ws", "sp" }:
            set_err_int(
                                Kerr,
                                Mod_name,
                                "open " + self.cage_id,
                                1,
                                message='Attempt open file with invalid mode "%s".'
                                % mod
            )
            return False

        if self.cage_id !=  self.client_id:

            if     self.payload [ 'permission'] == 'low' and \
                        mod != "rs" or\
                    self.payload [ 'permission'] == 'standard' and \
                        mod in {  "rm", "wm", "sp" }  or\
                    self.payload [ 'permission'] == 'high' and \
                        mode == "sp":
 
                                #                 rm  - open read/close with monopoly for channel owner
                                #                 wm  - open read/write/close with monopoly for channel owner
                                #                 rs  - open read/close and only read for other clients
                                #                 ws  - open read/write/close and only read for other clients
                                #                 sp  - need special external conditions for open and access
                                #                         (attach existing channel for other clients)

                set_err_int(
                                Kerr,
                                Mod_name,
                                "open " + self.cage_id,
                                2,
                                message='Attempt open file with mode "%s"  invalid by status "%s".'
                                % (mod, self.payload [ 'permission']),
                )
                return False
        """
        if WRITE_THREAD and self.payload['permission'] != 'low'  and mod [0] == "w" or mod == "sp":
            file_for_write = f_open(self, server, path, Kerr, "w"+mod[1])
            if  file_for_write == False:
                return False
            file_for_read =  f_open(self, server, path, Kerr, "rs")
            if  file_for_read == False:
                return False
            return file_for_write
        
        else:
        """
        
        return f_open(self, server, path, Kerr, mod)

    # --------------------------------------------------------

    def close(self, fchannel=-1, Kerr=[]):

        return f_close(self, fchannel, Kerr)

        """
        if not(WRITE_THREAD and self.payload['permission'] != 'low' ):
            return f_close(self, fchannel, Kerr)
        else:
            if  f_close(self, fchannel, Kerr) == False:
                return False
            return f_close(self, fchannel+1, Kerr)
        """
    # --------------------------------------------------------

    def is_active(self, fchannel=-1, Kerr=[], get_f_status=False):
        return is_open(self, fchannel, Kerr, get_f_status)

    # --------------------------------------------------------

    def write(self, fchannel, begin, data, Kerr):
        return w_cage(self, fchannel, begin, data, Kerr)

    def read(self, fchannel, begin, len_data, Kerr):
        return r_cage(self, fchannel, begin, len_data, Kerr)

    # --------------------------------------------------------

    def remote(self, server="default_server_and_main_port", Kerr=[]):
        return ch_copy(self, server, Kerr)

    def info(
        self, server="default_server_and_main_port", path="", fchannel=-1, Kerr=[]
    ):
        return inform(self, server, path, fchannel, Kerr)

    def stat(self, Kerr):
        return statis(self, Kerr)

    # ------------------------------------------------------------


    def file_remove(self, server="default_server_and_main_port", path="", Kerr=[]):

        kerr = []
        rc = f_remove(self, server, path, kerr)
        if rc == 1:

            time.sleep(0.1)

            return True
        # errors
        elif (
            rc == -1
        ):  #  file was only "virtually" closed for this client,  but not deleted on server
            if CAGE_DEBUG:
                Kerr += kerr
            set_err_int(
                Kerr,
                Mod_name,
                "file_remove " + self.cage_id,
                1,
                message="Channel of the file %s  was closed for this client, \
                         but file was not deleted on server = $s."
                % (path, server),
            )
            return False
        elif rc == 0:
            if (
                kerr[0][3] == "f_remove " + self.cage_id
            ):  # Cage server error (generated by cage_channel.f_remove)
                if CAGE_DEBUG:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_remove " + self.cage_id,
                    2,
                    message="Cage server  %s  error during file  %s  deletion. \n"
                    % (server, path),
                )
                #  kerr[0][4]  codes:           1:  server with specified name is not accessible
                #  2:  server with specified name is not connected
                #  3:  file path not specified
                #  4:  connection problem
            elif kerr[0][3] == "join " + self.cage_id and kerr[0][4] in (
                "5",
            ):  #  connection error   (generated by cage.join)
                if CAGE_DEBUG:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_remove " + self.cage_id,
                    3,
                    message='Connection problem with server "%s" .' % server,
                )
            elif kerr[0][3] == "del_f" and kerr[0][4] in (
                "1",
            ):  #  error   (generated by Cage Server)
                if CAGE_DEBUG:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_remove " + self.cage_id,
                    4,
                    message='OS file system error in server "%s" . File %s possibly not removed.'
                    % (server, path),
                )
                #  1:  : file OS delete error
            elif kerr[0][3] == "del_f" and kerr[0][4] in (
                "2",
            ):  #  error   (generated by Cage Server)
                if CAGE_DEBUG:
                    Kerr += kerr
                set_warn_int(
                    Kerr,
                    Mod_name,
                    "file_remove " + self.cage_id,
                    5,
                    message="File  %s  not found." % path,
                )
                #  2:  file not found
            else:  #  internal error   (generated by cage.join)
                if CAGE_DEBUG:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_remove " + self.cage_id,
                    6,
                    message="Cage internal error during file  %s  deletion. \n" % path
                    + "Possible connection/timeout problem.",
                )
            return False


    # --------------------------------------------------------

    def file_rename(
        self, server="default_server_and_main_port", path="", new_name="", Kerr=[]
    ):

        cage_id_and_JWT = self.client_id.encode('utf-8')
        self.cage_id= self.client_id

        pos_splitter=cage_id_and_JWT.find( SPLITTER)

        if pos_splitter > -1 and len(cage_id_and_JWT) > pos_splitter +4:
            jwtoken = cage_id_and_JWT[ (pos_splitter+4): ].decode('utf-8')
            if pos_splitter == 0:
                self.cage_id= jwtoken.decode('utf-8')
            else:
                self.cage_id= cage_id_and_JWT[ : pos_splitter].decode('utf-8')

        kerr = []
        rc = f_rename(self, server, path, new_name, kerr)
        if rc == -1:  #  file renamed
    
            time.sleep(0.01)

            return True

        elif rc == -2:
            if CAGE_DEBUG:
                Kerr += kerr
            set_warn_int(
                Kerr,
                Mod_name,
                "file_rename " + self.cage_id,
                1,
                message="File %s  not renamed, because already exist file with name %s."
                % (path, new_name),
            )
            return -2
        elif rc == -3:
            if CAGE_DEBUG:
                Kerr += kerr
            set_warn_int(
                Kerr,
                Mod_name,
                "file_rename " + self.cage_id,
                2,
                message="File %s  not renamed, because in use by other clients." % path,
            )
            return -3
        elif rc == False:
            if (
                kerr[0][3] == "f_rename " + self.cage_id
            ):  # Cage server error (generated by cage_channel.f_create)
                if CAGE_DEBUG:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_rename " + self.cage_id,
                    3,
                    message="Cage server  %s  error during file  %s  renaming. \n"
                    % (server, path),
                )
                #  kerr[0][4]  codes:           1:  server with specified name is not accessible
                #  2:  server with specified name is not connected
                #  3:  file path not specified
                #  4:  connection problem
            elif kerr[0][3] == "join " + self.cage_id and kerr[0][4] in (
                "5",
            ):  #  error   (generated by cage.join)
                if CAGE_DEBUG:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_rename " + self.cage_id,
                    4,
                    message='Connection problem with server "%s" .' % server,
                )
            elif kerr[0][3] == "ren_f" and kerr[0][4] in (
                "1",
            ):  #  error   (generated by Cage Server)
                #  1:  : file OS rename error  ( may be alredy exist file with new_name )
                if CAGE_DEBUG:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_rename " + self.cage_id,
                    5,
                    message="OS system file  %s  rename error on server." % path,
                )
            elif kerr[0][3] == "ren_f" and kerr[0][4] in (
                "2",
            ):  #  error   (generated by Cage Server)
                #  2:  file not found
                if CAGE_DEBUG:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_rename " + self.cage_id,
                    6,
                    message="File  %s  not found." % path,
                )
            else:  #  internal error   (generated by cage.join)
                if CAGE_DEBUG:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_rename " + self.cage_id,
                    7,
                    message="Cage internal error during file  %s  deletion. \n" % path
                    + "Possible connection/timeout problem.",
                )
            return False

        time.sleep(0.01)

        return True

    # --------------------------------------------------------

    def __del__(self):

        # pr (self.cage_ch)
        # cage_ch[channel] = (server, kw, mod)
        channels = list(self.cage_ch.keys())
        for nf in channels:
            self.close(nf)
            # pr ('__del__ Cage "%s". Files closed.'% (self.cage_id )     )
        del self.cage_ch
        for serv in self.clients:  # delete all client's sockets and close files

            try:  # if  self.clients[serv] != [False, False]:
                self.req_id += 1
                # send order to File i/o Cage server to terminate threads in file server
                # (belongs of this client) and disconnect with Working Cage server ZeroMQ
                p = self.server_ip[serv].find(":")
                host = self.server_ip[serv][:p]
                common_port = self.server_ip[serv][p + 1 :]
                request = ("t", self.client_id, -1, "", "", self.req_id)
                req = pickle.dumps(request)

                try:  # 1 step: try send order to subproces File io server disconnect with this client
                    self.clients[serv][1].send(req)
                except zmq.ZMQError as err:
                    # pr ('__del__ Cage "%s". ZMQ temp socket on server "%s" can NOT accept order'% (self.cage_id, serv) + \
                    # '\n to terminate threads in file server. \n Code = %s.'% str(err) )
                    pass
                else:
                    resp = self.join(request, serv, [])
                    if resp:
                        # pr ('__del__ Cage "%s". All cage files on server "%s" ( %s ) closed and threads stopped.'% \
                        #  (self.cage_id, serv, self.server_ip[serv]))
                        self.clients[serv][0].close()
                        self.clients[serv][1].close()
                        self.clients[serv] = [False, False]
                        continue  # server ended service normally
                # 2 step - send order to Common Cage server ZeroMQ disconnect with this client
                self.req_id += 1
                last_request = pickle.dumps((self.client_id, "disconnect", self.req_id))
                # pr ( '__del__  client %s, last_request = %s '% ( self.client_id, str (pickle.loads ( last_request) ) ) )
                #
                try:
                    self.clients[serv][0].send(last_request)
                except zmq.ZMQError as err:
                    # cerr2 = True
                    # pr ('__del__ Cage "%s". ZMQ common socket on server "%s" can NOT accept order'% \
                    # (self.cage_id, serv) + \
                    #'\n to terminate threads in file server. \n Code = %s.'% str(err) )
                    self.clients[serv][0].close()
                    self.clients[serv][1].close()
                    self.clients[serv] = [False, False]
                    continue  # server ended service absolutely not normally
                else:
                    event = -1
                    try:
                        event = self.clients[serv][0].poll(timeout=self.wait * 1000)
                    except zmq.ZMQError:
                        pass
                    if event > 0:
                        last_response = pickle.loads(self.clients[serv][0].recv())
                        # server ended service not normally but good
                        # pr ( 'client %s, last_response = %s'% (self.client_id, str(last_response) ) )
                        # pr ('__del__  Cage "%s". Server %s (%s : ---- ) '% \
                        # (self.cage_id, serv, host) + \
                        #'gave answer: %s .'% last_response[1] )
                        if last_response[1] == "disconnected":
                            # pr ('__del__ Cage "%s". Common and temp ZMQ sockets of server %s (%s) DISCONNECTED.'% \
                            # (self.cage_id, serv, self.server_ip[serv]) )
                            pass
                    else:
                        # pr ('__del__  Cage "%s". Common server %s (%s : ---- ) '% \
                        # (self.cage_id, serv, host) + \
                        # ' not return info about disconnecting.')
                        # server ended service absolutely not normally
                        pass
                    self.clients[serv][0].close()
                    self.clients[serv][1].close()
                    self.clients[serv] = [False, False]
                    continue

                self.clients[serv][0].close()
                self.clients[serv][1].close()
                self.clients[serv] = [False, False]

            except Exception:
                pass
            del self.clients[serv]
            continue

        # pr ( str(self.clients) )
        del self.clients

        self.context.destroy(linger=None)
        del self.server_ip
        del self.hash2nat

        del self.binout, self.masstr

        if not self.asleep:
            pr(
                'Cage "%s"  DELETED.'
                % (self.cage_id,)
            )
        else:
            pr(
                'Cage "%s" FELL ASLEEP.'
                % (self.cage_id,)
            )

    # ------------------------------------------------------------

    # record cage memory into file and delete cage instance
    def sleep(self, Kerr=[]):

        if not push_p(self, Kerr):
            return False

        try:
            Cache_hd = open(self.cache_file + "_" + self.cage_id + ".cg", "wb")
        except OSError as err:
            set_err_int(
                Kerr,
                Mod_name,
                "sleep " + self.cage_id,
                1,
                message="Cache file not opened with err :" + str(err),
            )
            return False
        try:
            # pr( str( set( self.clients.keys( ) ) ) )
            # pr( str( self.server_ip) )

            mem = pickle.dumps(
                (
                    Kerr,
                    self.cage_name,
                    self.pagesize,
                    self.numpages,
                    self.maxstrlen,
                    set(self.clients.keys()),
                    self.server_ip,
                    self.wait,
                    self.obj_id,
                    self.binout,
                    self.masstr,
                    self.kobr,
                    self.kzag,
                    self.kwyg,
                    self.num_cage_ch,
                    self.req_id,
                    self.req_id_thread,
                    self.client_id,
                    self.cage_ch,
                    self.hash2nat,
                    self.cage_id,
                    self.payload,
                )
            )
            # pr (str ( pickle.loads (mem) ) )
        except pickle.PickleError as err:
            set_err_int(
                Kerr,
                Mod_name,
                "sleep " + self.cage_id,
                2,
                message="Memory not pickled with err :" + str(err),
            )
            return False
        try:
            Cache_hd.write(mem)
        except OSError as err:
            set_err_int(
                Kerr,
                Mod_name,
                "sleep " + self.cage_id,
                3,
                message="Cache file not upload with err :" + str(err),
            )
            return False
        try:
            Cache_hd.close()
        except OSError as err:
            set_err_int(
                Kerr,
                Mod_name,
                "sleep " + self.cage_id,
                4,
                message="Cache file not closed with err :" + str(err),
            )
            return False
        self.asleep = True
        del self

    # ------------------------------------------------------------

    # recover cage memory from file - first step of cage building from file
    def wakeup1(self, Kerr=[]):         
        
        if self.cage_name == "":
                self.client_id = str(self.obj_id) + str(
                    self.pr_create
                )  # secure id. for access to servers from
                # cage instance
        else:
                self.client_id = self.cage_name            

        cage_id_and_JWT = self.client_id.encode('utf-8')
        pos_splitter=cage_id_and_JWT.find( SPLITTER)
        jwtoken= None
        if pos_splitter > -1 and len(cage_id_and_JWT) > pos_splitter +4:
            jwtoken = cage_id_and_JWT[ (pos_splitter+4): ].decode('utf-8')
            try:
                self.payload = jwt.decode(jwtoken, algorithm='HS256', verify=False)
            except InvalidTokenError as err:
                    raise CageERR(
                        "03 CageERR   Cage name contains invalid JW token, error: %s" % err
                    )
            if pos_splitter == 0:
                self.cage_id= jwtoken.decode('utf-8')
            else:
                self.cage_id= cage_id_and_JWT[ : pos_splitter].decode('utf-8')
        else:
            self.cage_id=  self.client_id
            self.payload = None

        try:
            Cache_hd = open(self.cache_file + "_" + self.cage_id + ".cg", "rb")
        except OSError as err:
            set_err_int(
                Kerr,
                Mod_name,
                "wakeup1 " + self.cage_id,
                1,
                message="Cache file not opened with err :" + str(err),
            )
            return False
        try:
            mem = Cache_hd.read()
        except OSError as err:
            set_err_int(
                Kerr,
                Mod_name,
                "wakeup1 " + self.cage_id,
                2,
                message="Cache file not download with err :" + str(err),
            )
            return False
        try:
            Cache_hd.close()
        except OSError as err:
            set_err_int(
                Kerr,
                Mod_name,
                "wakeup1 " + self.cage_id,
                3,
                message="Cache file not closed with err :" + str(err),
            )
            return False

        try:
            memory = pickle.loads(mem)

        except pickle.PickleError as err:
            set_err_int(
                Kerr,
                Mod_name,
                "wakeup1 " + self.cage_id,
                4,
                message="Memory not pickled with err :" + str(err),
            )
            return False

        self.uplog = {"Kerr": memory[0]}
        if self.cage_name != "" and self.cage_name != memory[1]:
            self.uplog["cage_name"]: (memory[1], self.cage_name)
        self.cage_name = memory[1]
        if self.pagesize != 0 and self.pagesize != memory[2]:
            self.uplog["pagesize"]: (memory[2], self.pagesize)
        self.pagesize = memory[2]
        if self.numpages != 0 and self.numpages != memory[3]:
            self.uplog["numpages"]: (memory[3], self.numpages)
        self.numpages = memory[3]
        if self.maxstrlen != 0 and self.maxstrlen < memory[4]:
            self.uplog["maxstrlen"]: (memory[4], self.maxstrlen)

        self.set_act_serv = memory[5]

        if self.server_ip != "*":  # permission use servers as before sleep
            diff = DictDiffer(memory[6], self.server_ip)
            # Added:    diff.added() - no problem
            # Removed:  diff.removed()
            # Changed:  diff.changed()
            if (
                diff.removed() & self.set_act_serv != set()
                or diff.changed() & self.set_act_serv != set()
            ):
                self.uplog["server_ip"]: (self.set_act_serv, memory[6], self.server_ip)
                # List of servers contains not all active servers before cage sleep
                # and/or contains changed endpoints for active servers before cage sleep
        self.server_ip = memory[6]

        if self.wait == 0:
            self.wait = memory[7]
        self.obj_id = memory[8]
        self.binout = memory[9]
        self.masstr = memory[10]
        self.kobr = memory[11]
        self.kzag = memory[12]
        self.kwyg = memory[13]
        self.num_cage_ch = memory[14]
        self.req_id = memory[15]
        self.req_id_thread = memory[16]
        self.client_id = memory[17]
        self.cage_ch = memory[18]
        self.hash2nat = memory[19]
        self.cage_id = memory[20]
        self.payload = memory[21]

        return True

    # ------------------------------------------------------------

    # open channels after sleeping and bebuild dict cage_ch
    #  with new server channels numbers - third step of cage building from file
    # (second step - socket connecting with server executes in __init__)
    def wakeup2(self, Kerr=[]):

        for nf in self.cage_ch:
            server = self.cage_ch[nf][0]
            mod = self.cage_ch[nf][2]
            path = self.cage_ch[nf][3]
            self.req_id += 1
            request = ("o", self.client_id, -1, mod, path, self.req_id)
            req = pickle.dumps(request)
            # pr ('\n ch_open === Kerr :%s' % str(Kerr) )
            try:
                self.clients[server][1].send(req)
            except zmq.ZMQError as err:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "wakeup2 " + self.cage_id,
                    1,
                    message='ZMQ temp socket on server "%s" can NOT accept order with command "%s".\n Code = %s.'
                    % (server, request[0], str(err)),
                )
                return (nf, path, server)
            kw = self.join(request, server, Kerr)
            if kw == False:
                return (nf, path, server)

            self.cage_ch[nf] = (server, kw, mod, path)

        if not reload_p(self, Kerr):
            return (nf, path, server)
        return True

    # ------------------------------------------------------------

    # recieve response from server for all operations
    def join(self, req="", server="default_server_and_main_port", Kerr=[]):

        """
        <------- w
        --------> b"\x0F" * 4
                        OR
                      b"\x00" * 4  + pickle.dumps(answer[:6])     -----ERROR
        <------- b"\x0F" * 4 + len_id (4 bytes) + Client ID +RequestID+ data 
                    len_id = struct.unpack(">L", len_id_byte)[0]
                    id = pickle.loads( message[8 : 8 + len_id])  # (self.client_id, self.req_id)
        --------> answer


        <------- r
        --------> b"\x0F" * 4
                        OR
                      b"\x00" * 4  + pickle.dumps(answer[:6])     -----ERROR
        <------- b"\x0F" * 4  + len_id (4 bytes) + Client ID +RequestID
        --------> b"\x0F" * 4 + data
                        OR
                      b"\x00" * 4  + pickle.dumps(answer[:6])     -----ERROR

        """

        for i in range(ATTEMPTS_WAIT_RESPONSE):
            event = -1
            try:
                event = self.clients[server][1].poll(timeout=RESPONSE_TIMEOUT)
            except zmq.ZMQError:
                event = -1

            #pr ('JOIN   ----  server "%s"   event =%d     req: %s' % (server, event, str(req)  ) )

            if event > 0:
                answer = self.clients[server][1].recv()

                if answer == b"\xFF" * 4:
                    time.sleep(1)
                    resend = pickle.dumps(req)
                    self.clients[server][1].send(resend)
                    continue

                if answer[:4] == b"\x00" * 4:
                    time.sleep(1)
                    respond = pickle.loads(answer[4:])
                    kerr = pickle.loads(respond[4])
                    serv = kerr[0]
                    cl_id = kerr[1]
                    Kerr_file_proc = kerr[2]
                    Kerr.append(tuple(Kerr_file_proc))
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "join " + self.cage_id,
                        1,
                        message="Cage client_id.: "
                        + self.client_id
                        + '\n      Recieved error message from file server "%s".'
                        % server,
                    )

                    return False

                # pr('\n JOIN request >>> ' + str(req) )
                respond = pickle.loads(answer)
                # pr(' JOIN respond <<< ' + str(respond)+'\n' )

                oper = respond[0]
                # id=         respond[1]
                # nf_serv=    respond[2]
                # Pointer=    respond[3]
                # data=       respond[4]
                req_id = respond[5]

                if req_id != req[5]:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "join " + self.cage_id,
                        2,
                        message="Respond Id "
                        + str(req_id)
                        + " not equal request Id "
                        + str(req[5]),
                    )
                    # pr('request >>> ' + str(req) )
                    # pr('respond <<< ' + str(respond) )
                    return False
                if oper == "o":
                    return respond[2]
                elif oper == "c":
                    return respond[4]
                elif oper == "d":
                    return respond[2]
                elif oper == "n":
                    return respond[3]
                # elif    oper == 'r':     return True
                elif oper == "w":
                    return True
                elif oper == "x":
                    return pickle.loads(respond[4])
                elif oper == "i":
                    return (respond[2], respond[3], respond[4])
                elif oper == "t":
                    return True
                elif oper == "e":
                    return True
                elif oper == "u":
                    return respond[2]

                # elif    oper == 'ze':

                elif len(oper) == 2 and oper[1] == "e":
                    kerr = pickle.loads(respond[4])
                    serv = kerr[0]
                    cl_id = kerr[1]
                    Kerr_file_proc = kerr[2]
                    Kerr.append(tuple(Kerr_file_proc))
                    if Kerr_file_proc[0] == "w":
                        set_warn_int(
                            Kerr,
                            Mod_name,
                            "join " + self.cage_id,
                            6,
                            message="Cage client_id.: "
                            + self.client_id
                            + '\n      Recieved warning message from file server "%s".'
                            % server,
                        )
                    else:
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "join " + self.cage_id,
                            3,
                            message="Cage client_id.: "
                            + self.client_id
                            + '\n      Recieved error message from file server "%s".'
                            % server,
                        )
                    return False

                else:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "join " + self.cage_id,
                        4,
                        message="Cage client_id.: "
                        + str(self.client_id)
                        + " Unsupported operation <"
                        + str(oper)
                        + "> detected.",
                    )
                    return False
            # pr (' ... Waiting ... Join: operation "%s" file channel :%d.'% (req[0],req[2]) )

        set_err_int(
            Kerr,
            Mod_name,
            "join " + self.cage_id,
            5,
            message="Cage client_id.: "
            + str(self.client_id)
            + ' Timing - not recieved respond from file server "%s" promptly.' % server
            + '\n Operation "%s" file channel :%d.' % (req[0], req[2]),
        )
        return False

    # -----------------------------------------------------
