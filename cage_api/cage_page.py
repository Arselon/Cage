# Cage® class v. 2.7 (Cage file server v.3.0)
# functions for methods:
#   get_p, put_p, mod_p,
#   push_all (push_p), refresh (reload_p)
# © A.S.Aliev, 2019

import pickle
import time
import struct
import threading
import queue

import zmq

from cage_par_cl import *
from cage_err import *

Mod_name = "*" + __name__

# -----------------------------------------------------

# provide nesessary file page in buffer
def get_p(self, fchannel, fpage, Kerr):
    #    min            minimum requests to page (counter)
    #    nmin           page with minimum requests
    #    smstr          offset of the physical page in file

    lock_pickle = threading.Lock()

    if is_err(Kerr) >= 0:
        return False

    push_thread = False

    if fchannel not in self.cage_ch:
        set_err_int(
            Kerr,
            Mod_name,
            "get_p " + self.cage_name,
            1,
            message="Channel number " + str(fchannel) + " is wrong",
        )
        return -1

    # convert cage instance channel number to server channel number
    server = self.cage_ch[fchannel][0]
    if WRITE_THREAD:
        thread_socket= self.clients[server][2]
    else:
        thread_socket= None

    """
    if WRITE_THREAD:
        nf_serv_for_read = self.cage_ch[fchannel+1][1]
    else:
    """
    nf_serv_for_read = self.cage_ch[fchannel][1]

    nusf = fpage

    # print( 'get_p 1 ==========  self.client_id  fchannel > ',self.client_id, fchannel)
    # print("\n ------  before page finding ",Channels[fchannel]["namef"]," fchannel= ",fchannel,"  nusf= ",nusf)

    #  statistics
    self.kobr += 1
    #  print("\n        *** get_p *** nusf= "+ nusf)
    #  find page in buffer

    if (nusf, fchannel) in self.hash2nat:  #   page FOUND in buffer
        # pr("page FOUND in buffer nusf= %d "% nusf)
        nsop = self.hash2nat[(nusf, fchannel)]
        self.binout[nsop]["kobs"] += 1  # statistics

        #  reliability checking
        if self.binout[nsop]["nf"] != fchannel:
            set_err_int(Kerr, Mod_name, "get_p " + self.cage_name, 2)
            return -1
        if self.binout[nsop]["nbls"] != nusf:
            set_err_int(Kerr, Mod_name, "get_p " + self.cage_name, 3)
            return -1

        return nsop  # return nesessary page number in buffer

    else:  #   page NOT FOUND in buffer
        zero_page1 = -1
        min1 = self.binout[0]["kobs"]  # find page with minimum requests
        nmin1 = None
        #
        for page in range(self.numpages):
            if zero_page1 < 0 and (
                self.binout[page]["kobs"] == 0 or self.binout[page]["nf"] == -1
            ):  # file was closed
                zero_page1 = page
                break
            if page == 0:
                nmin1 = 0
            elif self.binout[page]["kobs"] < min1:
                min1 = self.binout[page]["kobs"]
                nmin1 = page

    if WRITE_THREAD and  thread_socket != False:
        # pr(  "*** Get_P   *** 02")
        self.lock_memory.acquire()
        # pr(  "*** Get_P   *** 02-x")
    if zero_page1 == -1 and min1 > 1000:  #  normalize request counters
        for page in range(self.numpages):
            self.binout[page]["kobs"] -= min1 - 1
    if WRITE_THREAD  and  thread_socket != False:
        self.lock_memory.release()

    if WRITE_THREAD  and  thread_socket != False:
    # ------------------------------------ write thread ----------------------------------------------------


        if zero_page1 == -1:
            set_err_int(Kerr, Mod_name, "get_p " + self.cage_name, 4)
            return -1

        zero_page2 = -1
        nmin2 = None
        if zero_page1 in range(self.numpages - 1):
            begin_page = zero_page1 + 1
            min2 = self.binout[begin_page]["kobs"]
            #
            for page in range(begin_page, self.numpages):
                if zero_page2 < 0 and (
                    self.binout[page]["kobs"] == 0 or self.binout[page]["nf"] == -1
                ):  # file was closed
                    zero_page2 = page
                    break
                if page == begin_page:
                    nmin2 = begin_page
                if self.binout[page]["kobs"] < min2:
                    min2 = self.binout[page]["kobs"]
                    nmin2 = page

            min1_2 = min(min1, min2)
            # pr(  "*** Get_P   *** 03")
            self.lock_memory.acquire()
            # pr(  "*** Get_P   *** 03-x")
            if min1_2 > 1000:  #  normalize request counters
                for page in range(begin_page, self.numpages):
                    self.binout[page]["kobs"] -= min1_2 - 1
            self.lock_memory.release()

        if zero_page2 == -1:
            # need to push one page
            if nmin1 != None and nmin2 != None:
                if min1 < min2:
                    page_to_push = nmin1
                else:
                    page_to_push = nmin2
            elif nmin1 != None and nmin2 == None:
                page_to_push = nmin1
            elif nmin2 != None and nmin1 == None:
                page_to_push = nmin2
            else:
                set_err_int(Kerr, Mod_name, "get_p " + self.cage_name, 5)
                return -1

            nf_page_to_push = self.binout[page_to_push]["nf"]  # channel of buffer page
            nbls_page_to_push = self.binout[page_to_push][
                "nbls"
            ]  # physical number in file

            if not self.binout[page_to_push]["prmod"]:
                # clear unmodified page with min calls
                # pr(  "*** Get_P   *** 04")
                self.lock_memory.acquire()
                # pr(  "*** Get_P   *** 04-x")
                del self.hash2nat[
                    (nbls_page_to_push, nf_page_to_push)
                ]  # delete unmodified page from dict.
                self.binout[page_to_push]["nbls"] = -1  # and zero it's descripror
                self.binout[page_to_push]["nf"] = -1
                # self.binout[page_to_push]["prmod"] = False
                self.binout[page_to_push]["kobs"] = 0
                self.binout[page_to_push]["time"] = time.time()
                self.lock_memory.release()
            else:
                # for clear modified page with min calls need to push it
                push_thread = True
                self.Pages_to_write.put(page_to_push)

                #if self.lock_write.locked():
                    #self.lock_write.release()                               ###   RELEASE  lock write

        time_page_to_write_queued= time.time()

        page_update = zero_page1

    # ------------------------------------ write thread  end ----------------------------------------------------

    else:  # not write thread

        if zero_page1 >= 0:
            page_update = zero_page1

        else:
            nf_push = self.binout[nmin1][
                "nf"
            ]  # channel of buffer page to push (upload)
            nbls_push = self.binout[nmin1]["nbls"]  # physical number in file

            # print("\n ------  UPLOAD PAGE File  ",nf_push,"  nusf= ",nbls_push);
            if (nbls_push, nf_push) not in self.hash2nat:  # check pushing page in dict.
                set_err_int(Kerr, Mod_name, "get_p " + self.cage_name, 7)
                return -1
            if self.binout[nmin1]["prmod"]:  # if pushing page was modified
                # then upload it into file
                # print(" *** Upload ", nmin1);

                smstr = self.pagesize * nbls_push
                # print( 'get_p 2 ========== self.client_id, fchannel, Kerr',self.client_id, fchannel, Kerr)
                self.req_id += 1
                nf_serv_push = self.cage_ch[nf_push][1]
                request_1 = (
                    "w",
                    self.client_id,
                    nf_serv_push,
                    (smstr, -1),
                    "",
                    self.req_id,
                )

                lock_pickle.acquire()
                req_1 = pickle.dumps(request_1)
                # time.sleep ( 0.001)
                lock_pickle.release()

                try:
                    self.clients[server][1].send(req_1)
                except zmq.ZMQError as err:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "get_p " + self.cage_name,
                        8,
                        message='ZMQ client socket on server "%s" can NOT accept order with command "%s". \n Code = %s.'
                        % (server, request_1[0], str(err)),
                    )
                    return -1

                event = -1
                for i in range(ATTEMPTS_WAIT_RESPONSE):
                    try:
                        event = self.clients[server][1].poll(timeout=RESPONSE_TIMEOUT)
                    except zmq.ZMQError:
                        event = -1
                        pr(
                            'Get_p ... Waiting ... write: operation "%s" file channel :%d.'
                            % (request_1[0], request_1[2])
                        )
                        # time.sleep(1)
                        continue

                    if event > 0:
                        answer = self.clients[server][1].recv()
                        if answer == b"\xFF" * 4:
                            # resend
                            try:
                                self.clients[server][1].send(req_1)
                            except zmq.ZMQError as err:
                                set_err_int(
                                    Kerr,
                                    Mod_name,
                                    "get_p " + self.cage_name,
                                    9,
                                    message='ZMQ client socket on server "%s" can NOT accept order with command "%s". \n Code = %s.'
                                    % (server, request_1[0], str(err)),
                                )
                                return -1
                            continue
                        elif answer == b"\x0F" * 4:
                            break
                        elif answer[:4] == b"\x00" * 4:  # error in request

                            lock_pickle.acquire()
                            respond = pickle.loads(answer[4:])
                            kerr = pickle.loads(respond[4])
                            lock_pickle.release()

                            serv = kerr[0]
                            cl_id = kerr[1]
                            Kerr_file_proc = kerr[2]
                            Kerr.append(tuple(Kerr_file_proc))
                            set_err_int(
                                Kerr,
                                Mod_name,
                                "get_p " + self.cage_name,
                                10,
                                message='Server "%s" return error in request.' % server,
                            )
                            return -1
                        else:
                            set_err_int(
                                Kerr,
                                Mod_name,
                                "get_p " + self.cage_name,
                                11,
                                message='Server "%s" return unexpected answer = %s.'
                                % (
                                    server,
                                    answer.decode("utf-8", errors="backslashreplace"),
                                ),
                            )
                            return -1

                if event == -1:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "get_p " + self.cage_name,
                        13,
                        message="Cage client_id.: "
                        + str(self.client_id)
                        + ' Timing - not recieved respond from file server "%s" promptly.'
                        % server
                        + '\n Operation "%s" file channel :%d.'
                        % (request_1[0], request_1[2]),
                    )
                    return -1

                try:
                    id = (self.client_id, self.req_id)

                    lock_pickle.acquire()
                    id_byte = pickle.dumps(id)
                    # time.sleep ( 0.001)
                    lock_pickle.release()

                    len_id_byte = struct.pack(">L", len(id_byte))
                    id_block = len_id_byte + id_byte
                    self.clients[server][1].send(
                        b"\x0F" * 4 + id_block + self.masstr[nmin1]
                    )
                except zmq.ZMQError as err:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "get_p " + self.cage_name,
                        14,
                        message='ZMQ client socket on server "%s" can NOT accept message with data to write. \n Code = %s.'
                        % (server, str(err)),
                    )
                    return -1

                if not self.join(request_1, server, Kerr):
                    # error during uploading
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "get_p " + self.cage_name,
                        15,
                        message='Error from JOIN during uploading.'
                        % (server, str(err)),
                    )
                    return -1  # getpage failed

                self.kwyg += 1  # statistics

            '''
            if WRITE_THREAD:
                # pr(  "*** Get_P   *** 05")
                self.lock_memory.acquire()
                # pr(  "*** Get_P   *** 05-x")
            '''

            del self.hash2nat[(nbls_push, nf_push)]  # delete uploaded page from dict.
            self.binout[nmin1]["nbls"] = -1  # and zero it's descripror
            self.binout[nmin1]["nf"] = -1
            self.binout[nmin1]["prmod"] = False
            self.binout[nmin1]["kobs"] = 0
            self.binout[nmin1]["time"] = time.time()

            '''
            if WRITE_THREAD:
                self.lock_memory.release()
            '''

            page_update = nmin1

    # WRITING PAGE FINISHED

    time_page_to_read= time.time()
    # download nesessary physical page from file into free bufffer page

    smstr = self.pagesize * nusf
    # print("\n ------  before checking eof ",Channels[fchannel]["namef"]," fchannel= ",fchannel,"  nusf= ",nusf)
    # print( 'get page 3 ========== self.client_id, fchannel ',self.client_id, fchannel)

    self.req_id += 1
    request_2 = (
        "r",
        self.client_id,
        nf_serv_for_read,
        (smstr, self.pagesize),
        None,
        self.req_id,
    )
    lock_pickle.acquire()
    req_2 = pickle.dumps(request_2)
    # time.sleep ( 0.001)
    lock_pickle.release()

    try:
        self.clients[server][1].send(req_2)
    except zmq.ZMQError as err:
        set_err_int(
            Kerr,
            Mod_name,
            "get_p " + self.cage_name,
            16,
            message='ZMQ client socket on server "%s" can NOT accept order with command "%s" \n Code = %s.'
            % (server, request_2[0], str(err)),
        )
        return -1

    event = -1
    for i in range(ATTEMPTS_WAIT_RESPONSE):
        try:
            event = self.clients[server][1].poll(timeout=RESPONSE_TIMEOUT)
        except zmq.ZMQError:
            event = -1
            pr(
                'Get_p ... Waiting ... read: operation "%s" file channel :%d.'
                % (request_2[0], request_2[2])
            )
            time.sleep(1)
            continue
        if event > 0:
            answer = self.clients[server][1].recv()
            if answer == b"\xFF" * 4:
                # resend
                try:
                    self.clients[server][1].send(req_2)
                except zmq.ZMQError as err:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "get_p " + self.cage_name,
                        17,
                        message='ZMQ client socket on server "%s" can NOT accept order with command "%s" \n Code = %s.'
                        % (server, request_2[0], str(err)),
                    )
                    return -1
                continue
            elif answer == b"\x0F" * 4:
                break
            elif answer == b"\x00" * 4:  # error in request

                lock_pickle.acquire()
                respond = pickle.loads(answer[4:])
                kerr = pickle.loads(respond[4])
                lock_pickle.release()

                serv = kerr[0]
                cl_id = kerr[1]
                Kerr_file_proc = kerr[2]
                Kerr.append(tuple(Kerr_file_proc))
                set_err_int(
                    Kerr,
                    Mod_name,
                    "get_p " + self.cage_name,
                    18,
                    message='Server "%s" return error in request =%s ' \
                        % ( server, str(request_2))
                )
                return -1
            else:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "get_p " + self.cage_name,
                    19,
                    message='Server "%s" return unexpected answer = %s.'
                    % (server, answer.decode("utf-8", errors="backslashreplace")),
                )
                return -1
    if event == -1:
        set_err_int(
            Kerr,
            Mod_name,
            "get_p " + self.cage_name,
            21,
            message="Cage client_id.: "
            + str(self.client_id)
            + ' Timing - not recieved respond from file server "%s" promptly.' % server
            + '\n Operation "%s" file channel :%d.' % (request_2[0], request_2[2]),
        )
        return -1

    try:
        id = (self.client_id, self.req_id)

        lock_pickle.acquire()
        id_byte = pickle.dumps(id)
        lock_pickle.release()

        len_id_byte = struct.pack(">L", len(id_byte))
        id_block = len_id_byte + id_byte
        self.clients[server][1].send(b"\x0F" * 4 + id_block)
    except zmq.ZMQError as err:
        set_err_int(
            Kerr,
            Mod_name,
            "get_p " + self.cage_name,
            22,
            message='ZMQ client socket on server "%s" can NOT accept confirmation on read. Code = %s'
            % (server, str(err)),
        )
        return -1

    event = -1
    for i in range(ATTEMPTS_WAIT_RESPONSE):
        try:
            event = self.clients[server][1].poll(timeout=RESPONSE_TIMEOUT)
        except zmq.ZMQError:
            event = -1
            pr(
                'Get_p ... Waiting ... read: operation "%s" file channel :%d.'
                % (request_2[0], request_2[2])
            )
            time.sleep(1)
            continue
        if event > 0:
            err_or_data = self.clients[server][1].recv()
            err = err_or_data[:4]
            if err == b"\x00" * 4:  # error

                lock_pickle.acquire()
                respond = pickle.loads(err_or_data[4:])
                kerr = pickle.loads(respond[4])
                lock_pickle.release()

                serv = kerr[0]
                cl_id = kerr[1]
                Kerr_file_proc = kerr[2]
                Kerr.append(tuple(Kerr_file_proc))
                set_err_int(
                    Kerr,
                    Mod_name,
                    "get_p " + self.cage_name,
                    24,
                    message='Server "%s" return error for read data. request = %s' \
                        % (server, str(request_2) )
                )
                return -1
            elif err == b"\x0F" * 4:
                self.masstr[page_update] = err_or_data[4:]
                break
            else:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "get_p " + self.cage_name,
                    25,
                    message='Server "%s" return unexpected answer = %s. request = %s '\
                    % (server, err_or_data.decode("utf-8", errors="backslashreplace"), 
                          str(request_2) ),
                )
                return -1
    if event == -1:
        set_err_int(
            Kerr,
            Mod_name,
            "get_p " + self.cage_name,
            26,
            message="Cage client_id.: "
            + str(self.client_id)
            + ' Timing - not recieved respond from file server "%s" promptly.' % server
            + '\n Operation "%s" file channel :%d.' % (request_2[0], request_2[2]),
        )
        #if WRITE_THREAD and self.lock_write.locked():
            #self.lock_write.release()                                                ###   RELEASE  lock write
        return -1

    # print ('     get_p -- after ---  self.masstr[page_update:',  self.masstr[page_update])

    time_page_readed= time.time()

    self.kzag += 1  # statistics
    # print("\n ------  after LOAD PAGE from file ",Channels[fchannel]["namef"]," fchannel= ",fchannel,"  nusf= ",nusf)
    # print("  >> page >>>>", str(  self.masstr[page_update ) )
    # print ("  type( self.masstr[page_update]) - " , type( self.masstr[page_update]) )

    # make page descriptor
    if WRITE_THREAD  and  thread_socket != False:
        # pr(  "*** Get_P   *** 06")
        self.lock_memory.acquire()
        # pr(  "*** Get_P   *** 06-x")

    self.binout[page_update]["nbls"] = nusf
    self.binout[page_update]["nf"] = fchannel
    self.binout[page_update]["prmod"] = False
    self.binout[page_update]["kobs"] = 1
    self.binout[page_update]["time"] = time.time()
    # create element in dict.
    if len(self.hash2nat) <= self.numpages:
        self.hash2nat[(nusf, fchannel)] = page_update

    if WRITE_THREAD  and  thread_socket != False:
        self.lock_memory.release()

    if WRITE_THREAD  and  thread_socket != False:

        page_upload = -1
        while push_thread:
            try:
                page_upload = int(self.Pages_clean.get(False))
            except queue.Empty:
                # pr(   "Get_p   ***  Queue Pages_clean is empty"  )
                #time.sleep(0.001)
                continue
            else:
                # pr(   "Get_p   ***  Queue Pages_clean get %d" % page_upload  )
                self.kwyg += 1
                #self.lock_write.acquire()
                break

        #if self.lock_write.locked():
            #self.lock_write.release()                               ###   RELEASE  lock write

   #pr ('/n/n  GET_PAGE TIMING /n     time_page_to_write_queued: %f /n     time_page_to_read%f /n     time_page_readed%f'%\
            #(time_page_to_write_queued - int(time_page_to_write_queued), \
            #time_page_to_read - int(time_page_to_read), \
            #time_page_readed - int(time_page_readed)  ) )

    return page_update  # getpage success


# -----------------------------------------------------

# upload all modified pages into specified (opened) file
def put_p(self, fchannel, Kerr):

    if is_err(Kerr) >= 0:
        return False

    if fchannel not in self.cage_ch:
        set_err_int(
            Kerr,
            Mod_name,
            "put_p " + self.cage_name,
            1,
            message="Channel number " + str(fchannel) + " is wrong",
        )
        return False

    # if not self.is_open ( fchannel, Kerr) :
    #    set_err_int (Kerr, Mod_name, 'put_p '+self.cage_name, 2 , \
    #        message='For channel number '+str(fchannel)+ \
    #                ' corresponding server channel not opened ')
    #    return False

    server = self.cage_ch[fchannel][0]
    nf_serv = self.cage_ch[fchannel][1]

    # print( 'put_p ========== self.client_id, fchannel ',self.client_id, fchannel)

    for page in range(self.numpages):

        if self.binout[page]["prmod"] and self.binout[page]["nf"] == fchannel:
            smstr = self.pagesize * self.binout[page]["nbls"]

            self.req_id += 1
            request = ("w", self.client_id, nf_serv, (smstr, -1), "", self.req_id)
            req = pickle.dumps(request)
            # time.sleep ( 0.001)
            try:
                self.clients[server][1].send(req)
            except zmq.ZMQError as err:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "put_p " + self.cage_name,
                    2,
                    message='ZMQ client socket on server "%s" can NOT accept order with command "%s" \n Code = %s.'
                    % (server, request[0], str(err)),
                )
                return False
            event = -1
            for i in range(ATTEMPTS_WAIT_RESPONSE):
                try:
                    event = self.clients[server][1].poll(timeout=RESPONSE_TIMEOUT)
                except zmq.ZMQError:
                    event = -1
                    pr(
                        ' ... Waiting ... write: operation "%s" file channel :%d.'
                        % (request[0], request[2])
                    )
                    time.sleep(1)
                    continue
                if event > 0:
                    answer = self.clients[server][1].recv()
                    if answer == b"\xFF" * 4:
                        # resend
                        try:
                            self.clients[server][1].send(req)
                        except zmq.ZMQError as err:
                            set_err_int(
                                Kerr,
                                Mod_name,
                                "put_p " + self.cage_name,
                                3,
                                message='ZMQ client socket on server "%s" can NOT accept order with command "%s". \n Code = %s.'
                                % (server, request[0], str(err)),
                            )
                            return False
                        continue
                    elif answer == b"\x0F" * 4:
                        break
                    elif answer == b"\x00" * 4:  # error in request
                        respond = pickle.loads(answer[4:])
                        kerr = pickle.loads(respond[4])
                        serv = kerr[0]
                        cl_id = kerr[1]
                        Kerr_file_proc = kerr[2]
                        Kerr.append(tuple(Kerr_file_proc))
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "put_p " + self.cage_name,
                            4,
                            message='Server "%s" return error in requestr.' % server,
                        )
                        return False
                    else:
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "put_p " + self.cage_name,
                            5,
                            message='Server "%s" return unexpected answer = %s.'
                            % (
                                server,
                                answer.decode("utf-8", errors="backslashreplace"),
                            ),
                        )
                        return False
            if event == -1:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "put_p " + self.cage_name,
                    6,
                    message="Cage client_id.: "
                    + str(self.client_id)
                    + ' Timing - not recieved respond from file server "%s" promptly.'
                    % server
                    + '\n Operation "%s" file channel :%d.' % (request[0], request[2]),
                )
                return False

            try:
                id = (self.client_id, self.req_id)
                id_byte = pickle.dumps(id)
                len_id_byte = struct.pack(">L", len(id_byte))
                id_block = len_id_byte + id_byte
                self.clients[server][1].send(b"\x0F" * 4 + id_block + self.masstr[page])
            except zmq.ZMQError as err:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "put_p " + self.cage_name,
                    7,
                    message='ZMQ client socket on server "%s" can NOT accept message with data to write. \n Code = %s.'
                    % (server, str(err)),
                )
                return False

            if not self.join(request, server, Kerr):
                return False

            self.kwyg += 1

    return True  # putpage success


# -----------------------------------------------------

# set flag of modification for buffer page
def mod_p(self, nsop, Kerr):

    if is_err(Kerr) >= 0:
        return False

    nf = self.binout[nsop]["nf"]
    if nf < 0:  # internal error - page not active
        set_err_int(
            Kerr,
            Mod_name,
            "mod_p " + self.cage_name,
            1,
            message=" internal error - page " + str(nsop) + " not active.",
        )
        return False

    if self.cage_ch[nf][2][0] == "r":
        set_err_int(
            Kerr,
            Mod_name,
            "mod_p " + self.cage_name,
            2,
            message=" Impossible mark buffer page "
            + str(nsop)
            + " as modified because of channel %d was open only for read." % nf,
        )
        return False

    self.binout[nsop]["prmod"] = True

    return True


# -----------------------------------------------------
# refresh pages of those opened files who was modified outwardly
# (before cage wakeup and after last use in "old" cage  )
def reload_p(self, Kerr):

    for page in range(self.numpages):
        nf = self.binout[page]["nf"]
        time = self.binout[page]["time"]
        if nf >= 0 and time > 0:
            file_stat = self.is_active(nf, Kerr, get_f_status=True)
            if file_stat == False:
                return False
            time_f_mod = file_stat[1].st_mtime
            if time < time_f_mod:
                # download nesessary physical page from file into free bufffer page
                nusf = self.binout[page]["nbls"]
                server = self.cage_ch[nf][0]
                nf_serv = self.cage_ch[nf][1]

                smstr = self.pagesize * nusf
                # print("\n ------  before checking eof ",Channels[fchannel]["namef"]," fchannel= ",fchannel,"  nusf= ",nusf)
                # print( 'update_p ========== self.client_id, fchannel ',self.client_id, fchannel)
                self.req_id += 1
                request = (
                    "r",
                    self.client_id,
                    nf_serv,
                    (smstr, self.pagesize),
                    None,
                    self.req_id,
                )
                req = pickle.dumps(request)
                try:
                    self.clients[server][1].send(req)
                except zmq.ZMQError as err:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "reload_p " + self.cage_name,
                        1,
                        message='ZMQ client socket on server "%s" can NOT accept order with command "%s". \n Code = %s.'
                        % (server, request[0], str(err)),
                    )
                    return False
                event = -1
                for i in range(ATTEMPTS_WAIT_RESPONSE):
                    try:
                        event = self.clients[server][1].poll(timeout=RESPONSE_TIMEOUT)
                    except zmq.ZMQError:
                        event = False
                        pr(
                            ' ... Waiting ... read: operation "%s" file channel :%d.'
                            % (request[0], request[2])
                        )
                        time.sleep(1)
                        continue
                    if event > 0:
                        answer = self.clients[server][1].recv()
                        if answer == b"\xFF" * 4:
                            # resend
                            try:
                                self.clients[server][1].send(req)
                            except zmq.ZMQError as err:
                                set_err_int(
                                    Kerr,
                                    Mod_name,
                                    "reload_p " + self.cage_name,
                                    2,
                                    message='ZMQ client socket on server "%s" can NOT accept order with command "%s". \n Code = %s.'
                                    % (server, request[0], str(err)),
                                )
                                return False
                            continue
                        elif answer == b"\x0F" * 4:
                            break
                        elif answer == b"\x00" * 4:  # error in request
                            respond = pickle.loads(answer[4:])
                            kerr = pickle.loads(respond[4])
                            serv = kerr[0]
                            cl_id = kerr[1]
                            Kerr_file_proc = kerr[2]
                            Kerr.append(tuple(Kerr_file_proc))
                            set_err_int(
                                Kerr,
                                Mod_name,
                                "reload_p " + self.cage_name,
                                3,
                                message='Server "%s" return error in request.' % server,
                            )
                            return False
                        else:
                            set_err_int(
                                Kerr,
                                Mod_name,
                                "reload_p " + self.cage_name,
                                4,
                                message='Server "%s" return unexpected answer = %s.'
                                % (
                                    server,
                                    answer.decode("utf-8", errors="backslashreplace"),
                                ),
                            )
                            return False
                if event == False:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "reload_p " + self.cage_name,
                        5,
                        message="Cage client_id.: "
                        + str(self.client_id)
                        + ' Timing - not recieved respond from file server "%s" promptly.'
                        % server
                        + '\n Operation "%s" file channel :%d.'
                        % (request[0], request[2]),
                    )
                    return False

                try:
                    id = (self.client_id, self.req_id)
                    id_byte = pickle.dumps(id)
                    len_id_byte = struct.pack(">L", len(id_byte))
                    id_block = len_id_byte + id_byte
                    self.clients[server][1].send(b"\x0F" * 4 + id_block)
                except zmq.ZMQError as err:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "reload_p " + self.cage_name,
                        6,
                        message='ZMQ client socket on server "%s" can NOT accept confirmation for read. \n Code = %s.'
                        % (server, str(err)),
                    )
                    return False
                event = -1
                for i in range(ATTEMPTS_WAIT_RESPONSE):
                    try:
                        event = self.clients[server][1].poll(timeout=RESPONSE_TIMEOUT)
                    except zmq.ZMQError:
                        event = False
                        pr(
                            ' ... Waiting ... read: operation "%s" file channel :%d.'
                            % (request[0], request[2])
                        )
                        time.sleep(1)
                        continue
                    if event > 0:
                        err_or_data = self.clients[server][1].recv()
                        err = err_or_data[:4]
                        if err == b"\xFF" * 4:  # error
                            respond = pickle.loads(err_or_data[4:])
                            kerr = pickle.loads(respond[4])
                            serv = kerr[0]
                            cl_id = kerr[1]
                            Kerr_file_proc = kerr[2]
                            Kerr.append(tuple(Kerr_file_proc))
                            set_err_int(
                                Kerr,
                                Mod_name,
                                "reload_p " + self.cage_name,
                                7,
                                message='Server "%s" return error for read data.'
                                % server,
                            )
                            return False
                        elif err == b"\x0F" * 4:
                            self.masstr[page] = err_or_data[4:]
                            break
                        else:
                            set_err_int(
                                Kerr,
                                Mod_name,
                                "reload_p " + self.cage_name,
                                8,
                                message='Server "%s" return unexpected answer = %s.'
                                % (
                                    server,
                                    err_or_data.decode(
                                        "utf-8", errors="backslashreplace"
                                    ),
                                ),
                            )
                            return False
                if event == -1:
                    set_err_int(
                        Kerr,
                        Mod_name,
                        "reload_p " + self.cage_name,
                        9,
                        message="Cage client_id.: "
                        + str(self.client_id)
                        + ' Timing - not recieved respond from file server "%s" promptly.'
                        % server
                        + '\n Operation "%s" file channel :%d.'
                        % (request[0], request[2]),
                    )
                    return False

                # print ('     update_p -- after ---  self.masstr[nmin]:',  self.masstr[nmin])

                self.kzag += 1  # statistics
                self.binout[page]["prmod"] = False
                self.binout[page]["kobs"] = 1
                t = time.time()
                self.binout[page]["time"] = t

    return True


# -----------------------------------------------------
# push all modified pages into files
def push_p(self, Kerr):

    for page in range(self.numpages):
        if self.binout[page]["prmod"]:
            nf = self.binout[page]["nf"]
            nusf = self.binout[page]["nbls"]
            server = self.cage_ch[nf][0]
            nf_serv = self.cage_ch[nf][1]
            smstr = self.pagesize * self.binout[page]["nbls"]

            self.req_id += 1
            request = ("w", self.client_id, nf_serv, (smstr, -1), "", self.req_id)
            req = pickle.dumps(request)
            try:
                self.clients[server][1].send(req)
            except zmq.ZMQError as err:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "push_p " + self.cage_name,
                    1,
                    message='ZMQ client socket on server "%s" can NOT accept order with command "%s". \n Code = %s.'
                    % (server, request[0], str(err)),
                )
                return False
            event = -1
            for i in range(ATTEMPTS_WAIT_RESPONSE):
                try:
                    event = self.clients[server][1].poll(timeout=RESPONSE_TIMEOUT)
                except zmq.ZMQError:
                    event = -1
                    pr(
                        ' ... Waiting ... write: operation "%s" file channel :%d.'
                        % (request[0], request[2])
                    )
                    time.sleep(1)
                    continue
                if event > 0:
                    answer = self.clients[server][1].recv()
                    if answer == b"\xFF" * 4:
                        # resend
                        try:
                            self.clients[server][1].send(req)
                        except zmq.ZMQError as err:
                            set_err_int(
                                Kerr,
                                Mod_name,
                                "push_p " + self.cage_name,
                                2,
                                message='ZMQ client socket on server "%s" can NOT accept order with command "%s". \n Code = %s.'
                                % (server, request[0], str(err)),
                            )
                            return False
                        continue
                    elif answer == b"\x0F" * 4:
                        break
                    elif answer == b"\x00" * 4:  # error in request
                        respond = pickle.loads(answer[4:])
                        kerr = pickle.loads(respond[4])
                        serv = kerr[0]
                        cl_id = kerr[1]
                        Kerr_file_proc = kerr[2]
                        Kerr.append(tuple(Kerr_file_proc))
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "push_p " + self.cage_name,
                            3,
                            message='Server "%s" return error in requestr.' % server,
                        )
                        return False
                    else:
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "push_p " + self.cage_name,
                            4,
                            message='Server "%s" return unexpected answer = %s.'
                            % (
                                server,
                                answer.decode("utf-8", errors="backslashreplace"),
                            ),
                        )
                        return False
            if event == -1:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "push_p " + self.cage_name,
                    5,
                    message="Cage client_id.: "
                    + str(self.client_id)
                    + ' Timing - not recieved respond from file server "%s" promptly.'
                    % server
                    + '\n Operation "%s" file channel :%d.' % (request[0], request[2]),
                )
                return False

            try:
                id = (self.client_id, self.req_id)
                id_byte = pickle.dumps(id)
                len_id_byte = struct.pack(">L", len(id_byte))
                id_block = len_id_byte + id_byte
                self.clients[server][1].send(b"\x0F" * 4 + id_block + self.masstr[page])
            except zmq.ZMQError as err:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "push_p " + self.cage_name,
                    6,
                    message='ZMQ client socket on server "%s" can NOT accept message with data to write. \n Code = %s.'
                    % (server, str(err)),
                )
                return False

            if not self.join(request, server, Kerr):
                return False

            self.kwyg += 1

            # delete element from dict.
            if (self.binout[page]["nbls"], nf) in self.hash2nat:
                del self.hash2nat[(self.binout[page]["nbls"], nf)]
            else:  #  internal error - in dict. no element for pushing page
                set_err_int(
                    Kerr,
                    Mod_name,
                    "push_p " + self.cage_name,
                    7,
                    message="There is no page being pushed out in the dictionary.",
                )
                return False  # putpage failed

            self.binout[page]["nbls"] = -1
            self.binout[page]["nf"] = -1
            self.binout[page]["prmod"] = False
            self.binout[page]["kobs"] = 0
            self.binout[page]["time"] = time.time()

    return True  #
