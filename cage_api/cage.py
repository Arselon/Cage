# Cage® class v. 2.6 (Cage file server v.3.1)
# © A.S.Aliev, 2019


import pickle
import time
import threading
import queue

import zmq

from .cage_par_cl import *
from .cage_err import *

from .cage_page import *
from .cage_channel import *
from .thread_write_page import *

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

            if WRITE_THREAD:
                self.req_id_thread = 0   # number of last request to servers for write thread
            else:
                 self.req_id_thread = None

            if self.cage_name == "":
                self.client_id = str(self.obj_id) + str(
                    self.pr_create
                )  # secure id. for access to servers from
                # cage instance
            else:
                self.client_id = self.cage_name

        cerr = False
        try:
            self.context = zmq.Context()
        except zmq.ZMQError as err:
            set_err_int(
                Kerr,
                Mod_name,
                "__init__ " + self.cage_name,
                3,
                message="ZMQ context NOT started with error: %s" % err
                + '\n and Cage "%s" NOT created' % self.cage_name,
            )
            cerr = True
        if cerr:
            cerr = False
            raise CageERR(
                "03 CageERR   ZMQ context NOT started with error: %s" % err
                + '\n and Cage "%s" NOT created' % self.cage_name
            )

        self.context.setsockopt(zmq.LINGER, 0)

        if not self.bind(self.context, Kerr):
            set_err_int(
                Kerr,
                Mod_name,
                "__init__ " + self.cage_name,
                4,
                message='No ZMQ connections established and Cage "%s" NOT created.'
                % self.cage_name,
            )
            raise CageERR(
                '04 CageERR   No ZMQ connections established and Cage "%s" NOT created.'
                % self.cage_name
            )

        if WRITE_THREAD:

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
                    "__init__ " + self.cage_name,
                    3,
                    message="ZMQ context_thread NOT started with error: %s" % err
                    + '\n and Cage "%s" NOT created' % self.cage_name,
                )
                cerr = True
            if cerr:
                cerr = False
                raise CageERR(
                    "03 CageERR   ZMQ context_thread NOT started with error: %s" % err
                    + '\n and Cage "%s" NOT created' % self.cage_name
                )

            self.context_thread.setsockopt(zmq.LINGER, 0)

            if not self.bind_thread(self.context_thread, Kerr):
                set_err_int(
                    Kerr,
                    Mod_name,
                    "__init__ " + self.cage_name,
                    4,
                    message='No ZMQ thread connections established and Cage "%s" NOT created.'
                    % self.cage_name,
                )
                raise CageERR(
                    '04 CageERR   No ZMQ thread connections established and Cage "%s" NOT created.'
                    % self.cage_name
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
                    "__init__ " + self.cage_name,
                    5,
                    message="Error reopening mandatory channel %d ( %s ) on server %s when wake up."
                    % problem_serv
                    + '\n and Cage "%s" NOT created' % self.cage_name,
                )
                raise CageERR(
                    "05 CageERR   Error reopening mandatory channel %d ( %s ) on server %s when wake up."
                    % problem_serv
                    + '\n and Cage "%s" NOT created' % self.cage_name
                )
            pr(
                '  Cage "%s" WOKE UP for client_id = %s'
                % (self.cage_name, self.client_id)
            )
        else:
            pr(
                'Cage "%s" CREATED with client_id = %s'
                % (self.cage_name, self.client_id)
            )

        if WRITE_THREAD:

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

        suc_conn = False

        for serv in self.server_ip:

            if serv in self.set_act_serv:
                mandatory_connection = True
            else:
                mandatory_connection = False

            p = self.server_ip[serv].find(":")
            host = self.server_ip[serv][:p]
            common_port = self.server_ip[serv][p + 1 :]

            # 1 step connect with common port of server

            if not WRITE_THREAD:
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
                        % (self.cage_name, serv, host, common_port)
                        + "\n temporarily not connected with ZMQ error: %s . Waiting ..."
                        % err
                    )
                    Error = str(err)
                    time.sleep(ATTEMPT_TIMEOUT)
                    continue
                else:
                    if not WRITE_THREAD:
                        self.clients[serv] = [common_sock, False]
                    else:
                        self.clients[serv] = [common_sock, False, False]

                    pr(
                        'Cage "%s". Common socket for communication with server %s (%s : %s) READY.'
                        % (self.cage_name, serv, host, common_port)
                    )
                    break

            if self.clients[serv][0] == False:
                common_sock.close()
                if mandatory_connection:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "bind " + self.cage_name,
                        1,
                        message='Cage "%s". Common socket server %s (%s : %s) '
                        % (self.cage_name, serv, host, common_port)
                        + "\n NOT connected with ZMQ error: %s . Connection with it failed."
                        % Error
                        + "\n Connection has mandatory status, therefore Cage can not be created",
                    )
                    del self.clients
                    return False
                else:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "bind " + self.cage_name,
                        2,
                        message='Cage "%s". Common socket server %s (%s : %s) '
                        % (self.cage_name, serv, host, common_port)
                        + "\n NOT connected with ZMQ error: %s . Connection with it failed."
                        % Error,
                    )
                    del self.clients[serv]
                    continue

            # 2 step connect with temp port of server - for i/o server messaging
            self.req_id += 1
            first_request = pickle.dumps((self.client_id, "connect", self.req_id))
            # pr ( 'client %s, first_request = %s '% (cl_name, str (pickle.loads ( first_request) ) ) )
            try:
                self.clients[serv][0].send(first_request, zmq.DONTWAIT)
            except zmq.ZMQError as err:
                # send() in non-blocking mode, it raises zmq.error.Again < if err.errno == zmq.EAGAIN: > to inform you,
                # that there's nothing that could be done with the message and you should try again later.
                set_err_int(
                    Kerr,
                    Mod_name,
                    "bind " + self.cage_name,
                    3,
                    message='Cage "%s". Error during sendind first request to server %s (%s : %s ).'
                    % (self.cage_name, serv, host, common_port)
                    + "\n Connection with it failed.",
                )
                del self.clients[serv]
                continue

            first_response = ""
            for at2 in range(ATTEMPTS_CONNECT):
                # get client port for file processing
                try:
                    event = self.clients[serv][0].poll(timeout=RESPONSE_TIMEOUT)
                except zmq.ZMQError:
                    pr(
                        'Cage "%s". Fist response from server %s (%s : %s) '
                        % (self.cage_name, serv, host, common_port)
                        + "\n temporarily not recieved. Waiting ..."
                    )
                    time.sleep(self.wait)
                else:
                    first_response = pickle.loads(self.clients[serv][0].recv())
                    # pr ( 'client %s, first_response = %s'% (cl_name, str(first_response) ) )
                    break
                time.sleep(self.wait)

            if first_response == "":
                set_err_int(
                    Kerr,
                    Mod_name,
                    "bind " + self.cage_name,
                    4,
                    message='Cage "%s". First response from server %s (%s : %s) '
                    % (self.cage_name, serv, host, common_port)
                    + "\n not recieved. Connection with it failed.",
                )
                del self.clients[serv]
                continue

            if first_response[0] != self.client_id or first_response[2] != self.req_id:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "bind " + self.cage_name,
                    5,
                    message='Cage "%s". First response from server %s (%s : %s) was with error'
                    % (self.cage_name, serv, host, common_port)
                    + "\n Connection with it failed.",
                )
                del self.clients[serv]
                continue

            status = first_response[3]
            if status == "busy":
                del self.clients[serv]
                continue

            elif status == "connected":
                port_client = str(first_response[1])
                temp_endpoint = "tcp://" + host + ":" + port_client
                for at3 in range(ATTEMPTS_CONNECT):
                    try:
                        temp_client = zmq_context.socket(zmq.REQ)
                        temp_client.connect("tcp://%s:%s" % (host, port_client))
                    except zmq.ZMQError as err:
                        pr(
                            'Cage "%s". Client\'s socket server %s (%s : %s) '
                            % (self.cage_name, serv, host, port_client)
                            + "\n temporarily not connected with ZMQ error: %s . Waiting ..."
                            % err
                        )
                        time.sleep(ATTEMPT_TIMEOUT)
                        continue
                    else:
                        self.clients[serv][1] = temp_client
                        suc_conn = True
                        pr(
                            'Cage "%s". Client\'s socket server %s (%s : %s) for files operations CONNECTED.'
                            % (self.cage_name, serv, host, port_client)
                        )
                        break

            if self.clients[serv][1] == False:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "bind " + self.cage_name,
                    6,
                    message='Cage "%s". Client\'s socket server %s (%s ) '
                    % (self.cage_name, serv, host)
                    + "\n NOT connected with ZMQ . Connection with server failed.",
                )
                del self.clients[serv]
                continue
        if len(self.clients) == 0:
            return False
        else:
            return True

    # ------------------------------------------------------------

    # open clients ZeroMQ sockets for specified servers
    def bind_thread(self, zmq_context, Kerr):

        suc_conn = False

        for serv in self.clients:

            p = self.server_ip[serv].find(":")
            host = self.server_ip[serv][:p]
            common_port = self.server_ip[serv][p + 1 :]

            # 2 step connect with temp port of server - for i/o server messaging
            self.req_id_thread += 1
            first_request = pickle.dumps(
                (self.client_id, "connect", self.req_id_thread)
            )
            # pr ( 'client %s, first_request = %s '% (cl_name, str (pickle.loads ( first_request) ) ) )
            try:
                self.clients[serv][0].send(first_request, zmq.DONTWAIT)
            except zmq.ZMQError as err:
                # send() in non-blocking mode, it raises zmq.error.Again < if err.errno == zmq.EAGAIN: > to inform you,
                # that there's nothing that could be done with the message and you should try again later.
                set_err_int(
                    Kerr,
                    Mod_name,
                    "bind_thread " + self.cage_name,
                    3,
                    message='Cage thread"%s". Error during sendind first request to server %s (%s : %s ).'
                    % (self.cage_name, serv, host, common_port)
                    + "\n Connection with it failed.",
                )
                del self.clients[serv]
                continue

            first_response = ""
            for at2 in range(ATTEMPTS_CONNECT):
                # get client port for file processing
                try:
                    event = self.clients[serv][0].poll(timeout=RESPONSE_TIMEOUT)
                except zmq.ZMQError:
                    pr(
                        'Cage thread"%s". Fist response from server %s (%s : %s) '
                        % (self.cage_name, serv, host, common_port)
                        + "\n temporarily not recieved. Waiting ..."
                    )
                    time.sleep(self.wait)
                else:
                    first_response = pickle.loads(self.clients[serv][0].recv())
                    # pr ( 'client %s, first_response = %s'% (cl_name, str(first_response) ) )
                    break
                time.sleep(self.wait)

            if first_response == "":
                set_err_int(
                    Kerr,
                    Mod_name,
                    "bind_thread " + self.cage_name,
                    4,
                    message='Cage thread"%s". First response from server %s (%s : %s) '
                    % (self.cage_name, serv, host, common_port)
                    + "\n not recieved. Connection with it failed.",
                )
                del self.clients[serv]
                continue

            if (
                first_response[0] != self.client_id
                or first_response[2] != self.req_id_thread
            ):
                set_err_int(
                    Kerr,
                    Mod_name,
                    "bind_thread " + self.cage_name,
                    5,
                    message='Cage thread "%s". First response from server %s (%s : %s) was with error'
                    % (self.cage_name, serv, host, common_port)
                    + "\n Connection with it failed.",
                )
                del self.clients[serv]
                continue

            status = first_response[3]
            if status == "busy":
                del self.clients[serv]
                continue

            elif status == "connected":
                port_client = str(first_response[1])
                temp_endpoint = "tcp://" + host + ":" + port_client
                for at3 in range(ATTEMPTS_CONNECT):
                    try:
                        temp_client = zmq_context.socket(zmq.REQ)
                        temp_client.connect("tcp://%s:%s" % (host, port_client))
                    except zmq.ZMQError as err:
                        pr(
                            'Cage thread"%s". Client\'s socket server %s (%s : %s) '
                            % (self.cage_name, serv, host, port_client)
                            + "\n temporarily not connected with ZMQ error: %s . Waiting ..."
                            % err
                        )
                        time.sleep(ATTEMPT_TIMEOUT)
                        continue
                    else:
                        self.clients[serv][2] = temp_client
                        suc_conn = True
                        pr(
                            'Cage thread "%s". Client\'s socket server %s (%s : %s) for files operations CONNECTED.'
                            % (self.cage_name, serv, host, port_client)
                        )
                        break

            if self.clients[serv][2] == False:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "bind_thread " + self.cage_name,
                    6,
                    message='Cage thread"%s". Client\'s socket server %s (%s ) '
                    % (self.cage_name, serv, host)
                    + "\n NOT connected with ZMQ . Connection with server failed.",
                )
                del self.clients[serv]
                continue
        if len(self.clients) == 0:
            return False
        else:
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
                "file_create " + self.cage_name,
                1,
                message="File  %s  already exist and not opened." % path,
            )
            return -1

        elif rc != False and rc == -2:
            #   file already exist and  opened by this client !
            set_warn_int(
                Kerr,
                Mod_name,
                "file_create " + self.cage_name,
                2,
                message="File  %s  already exist and opened by this client." % path,
            )
            return -2

        elif rc != False:
            #   file already exist and opened by another client
            set_warn_int(
                Kerr,
                Mod_name,
                "file_create " + self.cage_name,
                2,
                message='File  %s  already exist and opened by another client with mode = " %s ".'
                % (path, rc),
            )

           # time.sleep(0.1)

            return rc  # mode of opened file

        else:  # if rc == False
            if (
                kerr[0][3] == "f_create " + self.cage_name
            ):  # Cage client error (generated by cage_channel.f_create)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_create " + self.cage_name,
                    3,
                    message="Internal error in cage before file  %s  creation on server %s. \n"
                    % (path, server),
                )
                #  kerr[0][4]  codes:           1:  server with specified name is not accessible
                #  2:  server with specified name is not connected
                #  3:  file path not specified
                #  4:  connection problem

            elif kerr[0][3] == "join " + self.cage_name and kerr[0][4] in (
                "5",
            ):  #  connection error   (generated by cage.join)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_create " + self.cage_name,
                    4,
                    message='Connection problem with server "%s" .' % server,
                )

            elif kerr[0][3] == "new_f" and kerr[0][4] in (
                "4",
                "5",
            ):  #  system file OS error on server    (generated by Cage Server)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_create " + self.cage_name,
                    5,
                    message='OS file system error in file server "%s" . File possibly not created.'
                    % server,
                )
                #  4:  file OS open error
                #  5:  file OS close error
            else:  #  internal error
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_create " + self.cage_name,
                    6,
                    message="Error during file  %s  creation. rc = %s\n"
                    % (path, str(rc)),
                )
            return False

    # ------------------------------------------------------------

    # open file
    def open(self, server="default_server_and_main_port", path="", Kerr=[], mod="wm"):
        # mod =     rm  - open read/close with monopoly for channel owner
        #           wm  - open read/write/close with monopoly for channel owner
        #           rs  - open read/close and only read for other clients
        #           ws  - open read/write/close and only read for other clients
        #           sp  - need special external conditions for open and access
        #                 (attach existing channel for other clients)
        kerr = []
        new_channel = ch_open(self, server, path, kerr, mod)
        if new_channel < 0:
            # file already opened by this client
            if __debug__:
                Kerr += kerr
            set_warn_int(
                Kerr,
                Mod_name,
                "open " + self.cage_name,
                8,
                message="File  %s  already opened by this client." % (path),
            )

            #time.sleep(0.1)

            return -new_channel

        elif new_channel == False:
            if kerr[0][3] == "open_f" and kerr[0][4] in (
                "2",
                "4",
                "5",
            ):  #   error    (generated by Cage Server)
                #  2:  file already opened by this client, but in another mode
                #  4:  exceptional status can not be set for file already opened with mod = "ws"
                #  5:  file already opened by another client and can not be opened with mode requested
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "open " + self.cage_name,
                    1,
                    message="File  %s  can not be opened with mode  %s. Change mode."
                    % (path, mode),
                )
            if (
                kerr[0][3] == "ch_open " + self.cage_name
            ):  # Cage server error (generated by cage_channel.ch_open)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "open " + self.cage_name,
                    2,
                    message="Cage server  %s  error during file  %s  opening. \n"
                    % (server, path),
                )
                #  kerr[0][4]  codes:           1:  server with specified name is not accessible
                #  2:  server with specified name is not connected
                #  3:  file path not specified
                #  4:  connection problem
            elif kerr[0][3] == "join " + self.cage_name and kerr[0][4] in (
                "5",
            ):  #  error   (generated by cage.join)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_create " + self.cage_name,
                    3,
                    message='Connection problem with server "%s" .' % server,
                )
            elif kerr[0][3] == "open_f" and kerr[0][4] in (
                "1",
                "3",
                "6",
                "7",
            ):  #   error   (generated by Cage Server)
                #  1:  file already opened with exceptional status. It is blocked now.
                #  3:  file already opened with monopoly by another client. It is blocked.
                #  6:  Max number of files exceeded
                #  7:  Max number of opened files exceeded
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "open " + self.cage_name,
                    4,
                    message="File  %s  can not be opened - busy or blocked. Wait."
                    % path,
                )
            elif kerr[0][3] == "open_f" and kerr[0][4] in (
                "8",
            ):  #   error   (generated by Cage Server)
                #  8: file OS open error
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "open " + self.cage_name,
                    5,
                    message="OS system file  %s  open error on server." % path,
                )
            elif kerr[0][3] == "open_f" and kerr[0][4] in (
                "9",
            ):  #   error   (generated by Cage Server)
                #  9:  file not found
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "open " + self.cage_name,
                    6,
                    message="File  %s  not found on server." % path,
                )
            else:  #  internal error   (generated by cage.join)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "open " + self.cage_name,
                    7,
                    message="Cage internal error during file  %s  opening. \n" % path,
                )
            return False

        time.sleep(0.01)

        return new_channel

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
            if __debug__:
                Kerr += kerr
            set_err_int(
                Kerr,
                Mod_name,
                "file_remove " + self.cage_name,
                1,
                message="Channel of the file %s  was closed for this client, \
                         but file was not deleted on server = $s."
                % (path, server),
            )
            return False
        elif rc == 0:
            if (
                kerr[0][3] == "f_remove " + self.cage_name
            ):  # Cage server error (generated by cage_channel.f_remove)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_remove " + self.cage_name,
                    2,
                    message="Cage server  %s  error during file  %s  deletion. \n"
                    % (server, path),
                )
                #  kerr[0][4]  codes:           1:  server with specified name is not accessible
                #  2:  server with specified name is not connected
                #  3:  file path not specified
                #  4:  connection problem
            elif kerr[0][3] == "join " + self.cage_name and kerr[0][4] in (
                "5",
            ):  #  connection error   (generated by cage.join)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_remove " + self.cage_name,
                    3,
                    message='Connection problem with server "%s" .' % server,
                )
            elif kerr[0][3] == "del_f" and kerr[0][4] in (
                "1",
            ):  #  error   (generated by Cage Server)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_remove " + self.cage_name,
                    4,
                    message='OS file system error in server "%s" . File %s possibly not removed.'
                    % (server, path),
                )
                #  1:  : file OS delete error
            elif kerr[0][3] == "del_f" and kerr[0][4] in (
                "2",
            ):  #  error   (generated by Cage Server)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_remove " + self.cage_name,
                    5,
                    message="File  %s  not found." % path,
                )
                #  2:  file not found
            else:  #  internal error   (generated by cage.join)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_remove " + self.cage_name,
                    6,
                    message="Cage internal error during file  %s  deletion. \n" % path
                    + "Possible connection/timeout problem.",
                )
            return False


    # --------------------------------------------------------

    def file_rename(
        self, server="default_server_and_main_port", path="", new_name="", Kerr=[]
    ):

        kerr = []
        rc = f_rename(self, server, path, new_name, kerr)
        if rc == -1:  #  file renamed
    
            time.sleep(0.01)

            return True

        elif rc == -2:
            if __debug__:
                Kerr += kerr
            set_warn_int(
                Kerr,
                Mod_name,
                "file_rename " + self.cage_name,
                1,
                message="File %s  not renamed, because already exist file with name %s."
                % (path, new_name),
            )
            return -2
        elif rc == -3:
            if __debug__:
                Kerr += kerr
            set_warn_int(
                Kerr,
                Mod_name,
                "file_rename " + self.cage_name,
                2,
                message="File %s  not renamed, because in use by other clients." % path,
            )
            return -3
        elif rc == False:
            if (
                kerr[0][3] == "f_rename " + self.cage_name
            ):  # Cage server error (generated by cage_channel.f_create)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_rename " + self.cage_name,
                    3,
                    message="Cage server  %s  error during file  %s  renaming. \n"
                    % (server, path),
                )
                #  kerr[0][4]  codes:           1:  server with specified name is not accessible
                #  2:  server with specified name is not connected
                #  3:  file path not specified
                #  4:  connection problem
            elif kerr[0][3] == "join " + self.cage_name and kerr[0][4] in (
                "5",
            ):  #  error   (generated by cage.join)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_rename " + self.cage_name,
                    4,
                    message='Connection problem with server "%s" .' % server,
                )
            elif kerr[0][3] == "ren_f" and kerr[0][4] in (
                "1",
            ):  #  error   (generated by Cage Server)
                #  1:  : file OS rename error  ( may be alredy exist file with new_name )
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_rename " + self.cage_name,
                    5,
                    message="OS system file  %s  rename error on server." % path,
                )
            elif kerr[0][3] == "ren_f" and kerr[0][4] in (
                "2",
            ):  #  error   (generated by Cage Server)
                #  2:  file not found
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_rename " + self.cage_name,
                    6,
                    message="File  %s  not found." % path,
                )
            else:  #  internal error   (generated by cage.join)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_rename " + self.cage_name,
                    7,
                    message="Cage internal error during file  %s  deletion. \n" % path
                    + "Possible connection/timeout problem.",
                )
            return False

        time.sleep(0.01)

        return True

    # --------------------------------------------------------

    def close(self, fchannel=-1, Kerr=[]):

        kerr = []
        server = self.cage_ch[fchannel][0]

        rc = put_p(self, fchannel, kerr)

        if rc == False:
            # pr(' Cage.close -1-  Kerr = '+str(Kerr) )
            if (
                kerr[0][3] == "put_p " + self.cage_name and kerr[0][4] == "1"
            ):  # Channel number is wrong
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "close " + self.cage_name,
                    1,
                    message="Channel number " + str(fchannel) + " is wrong.",
                )
            elif kerr[0][3] == "put_p " + self.cage_name and kerr[0][4] in (
                "2",
                "3",
                "4",
                "5",
                "6",
                "7",
                "8",
            ):  # Error in put page
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "close " + self.cage_name,
                    2,
                    message="Error during channel  %d  closing no. $s  in put_p. "
                    % (fchannel, kerr[0][4]),
                )
            return False

        for page in range(self.numpages):

            if self.binout[page]["nf"] == fchannel:

                # delete element from dict.
                if (self.binout[page]["nbls"], fchannel) in self.hash2nat:
                    del self.hash2nat[(self.binout[page]["nbls"], fchannel)]
                else:  #  internal error - in dict. no element for pushing page
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "close " + self.cage_name,
                        3,
                        message="There is no page being pushed out in the dictionary.",
                    )
                    return False  # putpage failed

                self.binout[page]["nbls"] = -1
                self.binout[page]["nf"] = -1
                self.binout[page]["prmod"] = False
                self.binout[page]["kobs"] = 0
                self.binout[page]["time"] = time.time()

        closed_channel = ch_close(self, fchannel, kerr)

        if closed_channel == True:
            set_warn_int(
                Kerr,
                Mod_name,
                "close " + self.cage_name,
                9,
                message="Channel %d was closed virtually for cage, not physically on server."
                % fchannel,
            )

           # time.sleep(0.01)

            return True  # file was closed virtually (only for this cage
            # and remains opened for other clients of file server)

        elif closed_channel == False:
            # pr(' Cage.close -2-  Kerr = '+str(Kerr) )
            if (
                kerr[0][3] == "ch_close " + self.cage_name and kerr[0][4] == "1"
            ):  # Channel number is wrong
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "close " + self.cage_name,
                    3,
                    message="Channel number " + str(fchannel) + " is wrong.",
                )
                return False
            elif (
                kerr[0][3] == "ch_close " + self.cage_name
            ):  # Cage server error (generated by cage_channel.ch_open)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "close " + self.cage_name,
                    4,
                    message="Cage server  %s  error during  file channel  %d  closing. \n"
                    % (server, fchannel),
                )
                #  kerr[0][4]  codes:           2:  server with specified name is not connected
                #  3:  connection problem
                return False
            elif kerr[0][3] == "join " + self.cage_name and kerr[0][4] in (
                "5",
            ):  #  error   (generated by cage.join)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "file_create " + self.cage_name,
                    5,
                    message='Connection problem with server "%s" .' % server,
                )
                return False
            elif kerr[0][3] == "close_f" and kerr[0][4] in (
                "1",
                "2",
                "3",
                "4",
            ):  #   error   (generated by Cage Server)
                #  1:  channel on server not exist
                #  2:  File with exceptional status can not closed on server
                #  3:  Channel number is wrong
                #  4:  Channel number is wrong
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "close " + self.cage_name,
                    6,
                    message="File channel  %s  not closed due to server %s error."
                    % (fchannel, server),
                )
            elif kerr[0][3] == "close_f" and kerr[0][4] in (
                "5",
            ):  #   error   (generated by Cage Server)
                #  5: file OS close error
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "close " + self.cage_name,
                    7,
                    message="OS system file channel %s  close error on server %s."
                    % (fchannel, server),
                )
            else:  #  internal error   (generated by cage.join)
                if __debug__:
                    Kerr += kerr
                set_err_int(
                    Kerr,
                    Mod_name,
                    "close " + self.cage_name,
                    8,
                    message="Cage internal error during file channel %s  opening on server %s. \n"
                    % (fchannel, server)
                    + "Possible connection/timeout problem.",
                )
            return False

        else:  # file was physically closed on server

            time.sleep(0.1)

            return True

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

    # --------------------------------------------------------

    def __del__(self):
        # pr (self.cage_ch)
        # cage_ch[channel] = (server, kw, mod)
        channels = list(self.cage_ch.keys())
        for nf in channels:
            self.close(nf)
            # pr ('__del__ Cage "%s". Files closed.'% (self.cage_name )     )
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
                    # pr ('__del__ Cage "%s". ZMQ temp socket on server "%s" can NOT accept order'% (self.cage_name, serv) + \
                    # '\n to terminate threads in file server. \n Code = %s.'% str(err) )
                    pass
                else:
                    resp = self.join(request, serv, [])
                    if resp:
                        # pr ('__del__ Cage "%s". All cage files on server "%s" ( %s ) closed and threads stopped.'% \
                        #  (self.cage_name, serv, self.server_ip[serv]))
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
                    # (self.cage_name, serv) + \
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
                        # (self.cage_name, serv, host) + \
                        #'gave answer: %s .'% last_response[1] )
                        if last_response[1] == "disconnected":
                            # pr ('__del__ Cage "%s". Common and temp ZMQ sockets of server %s (%s) DISCONNECTED.'% \
                            # (self.cage_name, serv, self.server_ip[serv]) )
                            pass
                    else:
                        # pr ('__del__  Cage "%s". Common server %s (%s : ---- ) '% \
                        # (self.cage_name, serv, host) + \
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
                'Cage "%s" of client_id = %s DELETED.'
                % (self.cage_name, self.client_id)
            )
        else:
            pr(
                'Cage "%s" of client_id = %s FELL ASLEEP.'
                % (self.cage_name, self.client_id)
            )

    # ------------------------------------------------------------

    # record cage memory into file and delete cage instance
    def sleep(self, Kerr=[]):

        if not push_p(self, Kerr):
            return False
        try:
            Cache_hd = open(self.cache_file + "_" + self.cage_name + ".cg", "wb")
        except OSError as err:
            set_err_int(
                Kerr,
                Mod_name,
                "sleep " + self.cage_name,
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
                )
            )
            # pr (str ( pickle.loads (mem) ) )
        except pickle.PickleError as err:
            set_err_int(
                Kerr,
                Mod_name,
                "sleep " + self.cage_name,
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
                "sleep " + self.cage_name,
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
                "sleep " + self.cage_name,
                4,
                message="Cache file not closed with err :" + str(err),
            )
            return False
        self.asleep = True
        del self

    # ------------------------------------------------------------

    # recover cage memory from file - first step of cage building from file
    def wakeup1(self, Kerr=[]):

        try:
            Cache_hd = open(self.cache_file + "_" + self.cage_name + ".cg", "rb")
        except OSError as err:
            set_err_int(
                Kerr,
                Mod_name,
                "wakeup1 " + self.cage_name,
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
                "wakeup1 " + self.cage_name,
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
                "wakeup1 " + self.cage_name,
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
                "wakeup1 " + self.cage_name,
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
                    "wakeup2 " + self.cage_name,
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
                    resend = pickle.dumps(req)
                    self.clients[server][1].send(resend)
                    continue

                if answer[:4] == b"\x00" * 4:
                    respond = pickle.loads(answer[4:])
                    kerr = pickle.loads(respond[4])
                    serv = kerr[0]
                    cl_id = kerr[1]
                    Kerr_file_proc = kerr[2]
                    Kerr.append(tuple(Kerr_file_proc))
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "join " + self.cage_name,
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
                        "join " + self.cage_name,
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
                            "join " + self.cage_name,
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
                            "join " + self.cage_name,
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
                        "join " + self.cage_name,
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
            "join " + self.cage_name,
            5,
            message="Cage client_id.: "
            + str(self.client_id)
            + ' Timing - not recieved respond from file server "%s" promptly.' % server
            + '\n Operation "%s" file channel :%d.' % (req[0], req[2]),
        )
        return False

    # -----------------------------------------------------
