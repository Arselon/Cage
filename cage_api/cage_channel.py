# Cage® class v. 2.5 (Cage file server v.3.0)
# functions for methods:
#   file_create (f_create), file_remove (f_remove),
#   open (ch_open), close (ch_close), is_active (is_open),
#   write (w_cage), read (r_cage)
#   remote (ch_copy), info (inform), stat (statis)
# © A.S.Aliev, 2019

import pickle

from .cage_par_cl import *
from .cage_err import *

from .cage_page import *

Mod_name = "*" + __name__

# ------------------------------------------------------------

# get info about all channels from server - list of tuples (one per channel)
# Channels_info = [ (nf, kzag, kwyg, namef, mod ), (...), ... ]
def ch_copy(self, server, Kerr):
    cerr = False
    if server not in self.clients:
        set_warn_int(
            Kerr,
            Mod_name,
            "ch_copy " + self.cage_name,
            1,
            message="Specified server name " + server + " is not accessible.",
        )
        return False
    if self.clients[server][1] == False:
        set_warn_int(
            Kerr,
            Mod_name,
            "ch_copy " + self.cage_name,
            2,
            message="Server %s ( %s ) " % (server, self.server_ip[server])
            + " not connected. Operation skipped",
        )
        return False
    if server == "":
        set_warn_int(
            Kerr,
            Mod_name,
            "ch_copy " + self.cage_name,
            3,
            message="Server name " + " and channel number not specified",
        )
        return False
    self.req_id += 1
    request = ("x", self.client_id, -1, "", "", self.req_id)
    req = pickle.dumps(request)
    try:
        self.clients[server][1].send(req)
    except zmq.ZMQError as err:
        set_err_int(
            Kerr,
            Mod_name,
            "ch_copy " + self.cage_name,
            4,
            message='ZMQ temp socket on server "%s" can NOT accept order with command "%s". Code = %s.'
            % (server, request[0], err),
        )
        return False

    return self.join(request, server, Kerr)


# ------------------------------------------------------------

# create new file
def f_create(self, server, path, Kerr):

    if Kerr != []:
        return False

    if server not in self.clients:
        set_warn_int(
            Kerr,
            Mod_name,
            "f_create " + self.cage_name,
            1,
            message="Specified server name " + server + " is not accessible.",
        )
        return False

    if self.clients[server][1] == False:
        set_warn_int(
            Kerr,
            Mod_name,
            "f_create " + self.cage_name,
            2,
            message="Server %s ( %s ) " % (server, self.server_ip[server])
            + " not connected. Operation skipped",
        )
        return False

    if server == "" or path == "":
        set_warn_int(
            Kerr,
            Mod_name,
            "f_create " + self.cage_name,
            3,
            message="Server name " + " and/or path not specified",
        )
        return False

    self.req_id += 1
    request = ("n", self.client_id, -1, "", path, self.req_id)

    # print ('\n CAGE-- f_create, client_id= %s, path=%s,  Kerr =%s'%(self.client_id, path, Kerr)) #############

    req = pickle.dumps(request)
    try:
        self.clients[server][1].send(req)
    except zmq.ZMQError as err:
        set_err_int(
            Kerr,
            Mod_name,
            "f_create " + self.cage_name,
            4,
            message='ZMQ temp socket on server "%s" can NOT accept order with command "%s". Code = %s.'
            % (server, request[0], err),
        )
        return False

    return self.join(request, server, Kerr)


# ------------------------------------------------------------

# rename closed file
def f_rename(self, server, path, new_name, Kerr):

    if is_err(Kerr) >= 0:
        return False

    if server not in self.clients:
        set_warn_int(
            Kerr,
            Mod_name,
            "f_rename " + self.cage_name,
            1,
            message="Specified server name " + server + " is not accessible.",
        )
        return False

    if self.clients[server][1] == False:
        set_warn_int(
            Kerr,
            Mod_name,
            "f_rename " + self.cage_name,
            2,
            message="Server %s ( %s ) " % (server, self.server_ip[server])
            + " not connected. Operation skipped",
        )
        return False

    if server == "" or path == "":
        set_warn_int(
            Kerr,
            Mod_name,
            "f_rename " + self.cage_name,
            3,
            message="Server name " + " and/or path not specified",
        )
        return False

    self.req_id += 1
    request = ("u", self.client_id, -1, new_name, path, self.req_id)
    req = pickle.dumps(request)
    # print ('\n ch_open === Kerr :',Kerr)
    try:
        self.clients[server][1].send(req)
    except zmq.ZMQError as err:
        set_err_int(
            Kerr,
            Mod_name,
            "f_rename " + self.cage_name,
            4,
            message='ZMQ temp socket on server "%s" can NOT accept order with command "%s". Code = %s.'
            % (server, request[0], err),
        )
        return False

    return self.join(request, server, Kerr)


# ------------------------------------------------------------


# open file and create corresponding cage channel
def ch_open(self, server, path, Kerr, mod):
    cerr = False
    # mod =     rm  - open read/close with monopoly for channel owner
    #           wm  - open read/write/close with monopoly for channel owner
    #           rs  - open read/close and only read for other clients
    #           ws  - open read/write/close and only read for other clients
    #           sp  - need special external conditions for open and access
    #                 (attach existing channel for other clients)

    if is_err(Kerr) >= 0:
        return False

    if server not in self.clients:
        set_warn_int(
            Kerr,
            Mod_name,
            "ch_open " + self.cage_name,
            1,
            message="Specified server name " + server + " is not accessible.",
        )
        return False

    if self.clients[server][1] == False:
        set_warn_int(
            Kerr,
            Mod_name,
            "ch_open " + self.cage_name,
            2,
            message="Server %s ( %s ) " % (server, self.server_ip[server])
            + " not connected. Operation skipped",
        )
        return False

    if server == "" or path == "":
        set_warn_int(
            Kerr,
            Mod_name,
            "ch_open " + self.cage_name,
            3,
            message="Server name " + " and/or path not specified",
        )
        return False

    self.req_id += 1
    request = ("o", self.client_id, -1, mod, path, self.req_id)
    req = pickle.dumps(request)

    # print ('\n CAGE-- ch_open, client_id= %s, path=%s,  Kerr =%s'%(self.client_id, path, Kerr)) #############

    try:
        self.clients[server][1].send(req)
    except zmq.ZMQError as err:
        set_err_int(
            Kerr,
            Mod_name,
            "ch_open " + self.cage_name,
            4,
            message='ZMQ temp socket on server "%s" can NOT accept order with command "%s". Code = %s.'
            % (server, request[0], err),
        )
        return False
    kw = self.join(request, server, Kerr)
    if kw == False or kw == 0:
        return False
    elif kw > 0:  # channel opened for this cage (phisically or virtually)
        self.num_cage_ch += 1
        self.cage_ch[self.num_cage_ch] = (server, kw, mod, path)
        return self.num_cage_ch
    else:  # channel already opened for this cage on server

        for ch in self.cage_ch:
            if self.cage_ch[ch][1] == -kw:
                return -ch

        self.num_cage_ch += 1
        self.cage_ch[self.num_cage_ch] = (server, -kw, mod, path)
        return self.num_cage_ch

    return False


# ------------------------------------------------------------
# delete cage channel and close file
def ch_close(self, fchannel, Kerr):

    if is_err(Kerr) >= 0:
        return False

    # print (fchannel, self.cage_ch)
    if fchannel not in self.cage_ch:
        set_err_int(
            Kerr,
            Mod_name,
            "ch_close " + self.cage_name,
            1,
            message="Channel number " + str(fchannel) + " is wrong",
        )
        return False

    ch = self.cage_ch[fchannel][1]
    server = self.cage_ch[fchannel][0]
    path = self.cage_ch[fchannel][3]

    if self.clients[server][1] == False:
        set_err_int(
            Kerr,
            Mod_name,
            "ch_close " + self.cage_name,
            2,
            message="Server %s ( %s ) " % (server, self.server_ip[server])
            + " not connected. Operation close skipped",
        )
        return False
    self.req_id += 1
    request = ("c", self.client_id, ch, "", "", self.req_id)
    req = pickle.dumps(request)
    try:
        self.clients[server][1].send(req)
    except zmq.ZMQError as err:
        set_err_int(
            Kerr,
            Mod_name,
            "ch_close " + self.cage_name,
            3,
            message='ZMQ temp socket on server "%s" can NOT accept order with command "%s". Code = %s'
            % (server, request[0], err),
        )
        return False

    kw = self.join(request, server, Kerr)
    if kw == False:
        return False
    elif kw == True:
        del self.cage_ch[fchannel]
        set_warn_int(
            Kerr,
            Mod_name,
            "ch_close " + self.cage_name,
            5,
            message="File %s was closed virtually for cage, not physically." % path,
        )
        return True  # file was closed virtually (only for this cage
        # and remains opened for other clients of file server)
    elif kw != path:
        set_err_int(
            Kerr,
            Mod_name,
            "ch_close " + self.cage_name,
            4,
            message="Path %s of closed cage channel %d on server %s NOT equal memorized %s."
            % (kw, fchannel, server, path),
        )
        return False
    else:
        del self.cage_ch[fchannel]
        return kw  # kw = path of really closed on file server file


# ------------------------------------------------------------

# close channels with specifed path and attempt to delete file on server
def f_remove(self, server, path, Kerr):

    if Kerr != []:
        return False

    if server not in self.clients:
        set_warn_int(
            Kerr,
            Mod_name,
            "f_remove " + self.cage_name,
            1,
            message="Specified server name " + server + " is not accessible.",
        )
        return False
    if self.clients[server][1] == False:
        set_warn_int(
            Kerr,
            Mod_name,
            "f_remove " + self.cage_name,
            2,
            message="Server %s ( %s ) " % (server, self.server_ip[server])
            + " not connected. Operation skipped",
        )
        return False
    if server == "" or path == "":
        set_warn_int(
            Kerr,
            Mod_name,
            "f_remove " + self.cage_name,
            3,
            message="Server name " + " and/or path not specified",
        )
        return False
    channels = set(self.cage_ch.keys())
    for nf in channels:  # if file have channel - close it
        if self.cage_ch[nf][3] == path:
            if not self.close(nf, Kerr):
                return False

    self.req_id += 1
    request = ("d", self.client_id, -1, "", path, self.req_id)
    req = pickle.dumps(request)
    try:
        self.clients[server][1].send(req)
    except zmq.ZMQError as err:
        set_err_int(
            Kerr,
            Mod_name,
            "f_remove " + self.cage_name,
            4,
            message='ZMQ temp socket on server "%s" can NOT accept order with command "%s". Code = %s.'
            % (server, request[0], err),
        )
        return False

    return self.join(request, server, Kerr)
    # nf ==  0  - error during file removing
    # nf == -1  - file was only "virtually" closed for this client
    #   but not deleted on server
    # nf ==  1  - file was closed and really deleted on file server


# ------------------------------------------------------------

# test number as cage channell and returm mode for cge operations and OS status on file server
def is_open(self, fchannel, Kerr, stat):
    cerr = 0
    try:
        fchannel += 0
    except TypeError:
        set_err_int(
            Kerr,
            Mod_name,
            "is_open " + self.cage_name,
            1,
            message="Number of cage channel not digital",
        )
        return False
    if fchannel not in self.cage_ch:
        return False
    ch = self.cage_ch[fchannel][1]
    server = self.cage_ch[fchannel][0]
    self.req_id += 1
    request = ("i", self.client_id, ch, "", "", self.req_id)
    req = pickle.dumps(request)
    try:
        self.clients[server][1].send(req)
    except zmq.ZMQError as err:
        set_err_int(
            Kerr,
            Mod_name,
            "is_open " + self.cage_name,
            2,
            message='ZMQ temp socket on server "%s" can NOT accept order with command "%s". Code = %s.'
            % (server, request[0], err),
        )
        return False
    r = self.join(request, server, Kerr)
    if r[0] == -1:  #  r[0]= server channel number
        set_warn_int(
            Kerr,
            Mod_name,
            "is_open " + self.cage_name,
            3,
            message="Internal error. Channel number "
            + str(fchannel)
            + " refers to a nonexistent server channel.",
        )
        return False

    if r[1] == "":  #  r[1]= mod parameter for opened channel
        set_warn_int(
            Kerr,
            Mod_name,
            "is_open " + self.cage_name,
            4,
            message="Internal error. Channel number "
            + str(fchannel)
            + " refers to server channel opened for another client.",
        )
        return False
    if stat:
        return (
            r[1],
            r[2],
        )  #  r[2] - os.path.stat - docs.python.org/3/library/os.html#os.stat
    else:
        return r[1]


# --------------------------------------------------------------------------------

# print statistics and info about opened files and corresponding channels
def statis(self, Kerr):  #   statistic printing (test method)
    print("\n        *** S T A T I S T I C S ***  %s  ***\n" % self.cage_name)
    print("   Buffered cage pages input - output")
    print("      all page requests  %6d" % self.kobr)
    print("              downloads  %6d" % self.kzag)
    print("                uploads  %6d" % self.kwyg)

    for serv in self.clients:
        print(
            "\n   Channels io on file server: %s ( %s )" % (serv, self.server_ip[serv])
        )
        Channels_info = self.remote(serv, Kerr)
        if Channels_info == False:
            print("\n Warning!!! Channels info inaccessible.\n")
            return
        print("      Server self name  ", Channels_info[0][0])
        print("             data port  ", Channels_info[0][1])
        print("             read  ops. ", Channels_info[0][2])
        print("             write ops. ", Channels_info[0][3])
        if len(Channels_info) > 1:
            print(
                "\n  cage_ch   Serv_ch  downloads  uploads           file ( modality )"
            )
            for ch_num in range(
                1, len(Channels_info)
            ):  # for server internal channel no. iteration

                for cage_channel in self.cage_ch:  # find corresponding client channel
                    if Channels_info[ch_num][0] == self.cage_ch[cage_channel][1]:
                        print(
                            "  %4d   " % cage_channel,
                            "    %4d    %6d    %6d     %s ( %s )"
                            % Channels_info[ch_num],
                        )
        else:
            print("\n   No active file channels in cage")
        print()


# -----------------------------------------------------

# write data to file
def w_cage(self, fchannel, begin, data, Kerr):

    if is_err(Kerr) >= 0:
        return False

    _length = len(data)

    if _length == 0:
        return True

    if _length > self.maxstrlen:
        set_err_int(
            Kerr,
            Mod_name,
            "w_cage " + self.cage_name,
            1,
            message="Parameter len(data) out of range " + str(_length) + " .",
        )
        return False
    if begin < 0:
        set_err_int(
            Kerr,
            Mod_name,
            "w_cage " + self.cage_name,
            2,
            message="Parameter begin out of range " + str(begin) + ".",
        )
        return False

    if self.cage_ch[fchannel][2][0] == "r":
        set_err_int(
            Kerr,
            Mod_name,
            "w_cage " + self.cage_name,
            3,
            message=" Impossible write "
            + nsop
            + " as modified because of channel %d was open only for read." % nf,
        )
        return False
    _pos = 0
    _nblok = int(begin / self.pagesize)
    _from = int(begin - self.pagesize * _nblok)
    while _length > 0:
        if (_length + _from) > self.pagesize:
            _lenost = self.pagesize - _from
            _length -= _lenost
        else:
            _lenost = int(_length)
            _length = 0

        # print( 'W_cage 1 ==========  self.client_id  fchannel > ',self.client_id, fchannel)

        nsop = self.get_page(fchannel, _nblok, Kerr)
        if type(nsop) is bool:
            if nsop == False:
                return False

        self.masstr[nsop] = (
            self.masstr[nsop][:_from]
            + data[_pos : (_pos + _lenost)]
            + self.masstr[nsop][(_from + _lenost) :]
        )

        self.binout[nsop]["prmod"] = True

        _pos += _lenost
        _nblok += 1
        _from = 0

    return True


# -----------------------------------------------------

# read data from file
def r_cage(self, fchannel, begin, len_data, Kerr):

    if is_err(Kerr) >= 0:
        return False

    data = bytearray()

    if len_data < 0 or len_data > self.maxstrlen:
        set_err_int(
            Kerr,
            Mod_name,
            "r_cage " + self.cage_name,
            1,
            message="Parameter len_data out of range " + str(len_data) + ".",
        )
        return False
    if begin < 0:
        set_err_int(
            Kerr,
            Mod_name,
            "r_cage " + self.cage_name,
            2,
            message="Parameter begin out of range " + str(begin) + ".",
        )
        return False
    if len_data == 0:
        set_warn_int(
            Kerr,
            Mod_name,
            "r_cage " + self.cage_name,
            3,
            message="Parameter len_data = 0 ",
        )
        return data

    _length = len_data
    _pos = 0
    _nblok = int(begin / self.pagesize)
    _from = int(begin - self.pagesize * _nblok)
    while _length > 0:
        if (_length + _from) > self.pagesize:
            _lenost = self.pagesize - _from
            _length -= _lenost
        else:
            _lenost = int(_length)
            _length = 0

        # print( ' fchannel ',fchannel,'  _nblock ',_nblok,' _from ', _from, ' _lenost ',_lenost, Kerr)
        # print( 'r_cage 1 ==========  self.client_id  fchannel > ',self.client_id, fchannel)

        nsop = self.get_page(fchannel, _nblok, Kerr)
        if type(nsop) is bool:
            if nsop == False:
                return False

        # print (' nsop ', nsop, ' data >',data,'<')
        # print (self.masstr[nsop][_from:(_from+_lenost)])

        data = data + self.masstr[nsop][_from : (_from + _lenost)]

        # print (len(data), ' data >',data,'<')

        _pos += _lenost
        _nblok += 1
        _from = 0

    return data
