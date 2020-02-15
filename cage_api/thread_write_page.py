# Cage® v. 2.6 (Cage file server v.3.1)
# © A.S.Aliev, 2019

import pickle
import time
import struct
import threading
import queue

import zmq

from .cage_par_cl import *
from .cage_err import *

Mod_name = "*" + __name__

# ------------------------------------------------------------


# Thread for writing pages


def page_write(
    Kerr,
    pagesize,
    # dict. for keeping of ZeroMQ "client" objects:
    clients,  # server conditional name -> ZMQ object
    # { 'server name' : ( Common socket, Temp_socket) }
    # dict. with index for fast access:
    hash2nat,  # (no. of page in file, channel) -> buffer page
    # dict. for renumerate session's cage channels numbers into
    # session's servers files  channels (unique files "numbers"):
    cage_ch,  # cage channel number -> (server, server internal channel number)
    binout,
    masstr,
    client_id,
    cage_name,
    Pages_to_write,
    Pages_clean,
    lock_write,
    lock_memory,
    req_id_thread,
):

    global Mod_name

    page = None

    #pr(  "10 Page_write   ***  Page_write thread begin"  )

    while True:

        #pr(  "*** Page_write   *** 00 page="+str(page))

        #lock_write.acquire()  # get  permission to upload page
        #pr (" LOCK_WRITE  1 : "+ str (   lock_write ) +'\n'    )

        time_to_write = time.time()

        if page == None:
            #time.sleep(0.001)                        #   !!! ??????
            try:
                #pr(  "10 Page_write   ***  Page_write try get page number"  )
                page = int(Pages_to_write.get(False))

            except queue.Empty:
                #pr(   "11 Page_write   ***  Queue Pages_to_write is EMPTY"  )    
                #pr (" LOCK_WRITE  2 : "+ str (   lock_write ) +'\n'  )
                #lock_write.release()

                time.sleep(0.001)               #   !!! ??????
                continue
            else:
                pass

        else:
            pr("20 CageERR Internal error in thread Page_write.")
            raise CageERR("20 CageERR   Internal error in thread Page_write.")
            lock_write.release()
            sys.exit()

        time.sleep(0.00001)
        #pr(  "12 Page_write   *** Cage  %s :  Page_write got page number # %d  (req_id_thread = %d) "  %  (cage_name, page, req_id_thread) )
        if page == -1:
            #pr( "13 Page_write   *** Cage  %s :  Pages write thread STOPPED normally."  \
                #% cage_name  )
            #lock_write.release()
            sys.exit()

        fchannel = binout[page]["nf"]
        server = cage_ch[fchannel][0]
        nf_serv = cage_ch[fchannel][1]

        if binout[page]["prmod"]:
            #pr(  "*** Page_write   *** 01")
            smstr = pagesize * binout[page]["nbls"]

            req_id_thread += 1
            request = ("w", client_id, nf_serv, (smstr, -1), "", req_id_thread)

            req = pickle.dumps(request)

            # time.sleep(0.01)                                          ###################
            try:
                #pr(  "*** Page_write   *** 02")
                clients[server][2].send(req)
            except zmq.ZMQError as err:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "page_write " + cage_name,
                    2,
                    message='ZMQ client socket on server "%s" can NOT accept order with command "%s" \n Code = %s.'
                    % (server, request[0], str(err)),
                )
                pr("21 CageERR Pages write thread STOPPED due to an error.")
                raise CageERR(
                    "21 CageERR   Pages write thread STOPPED due to an error."
                )
                #lock_write.release()
                sys.exit()

            event = -1
            for i in range(ATTEMPTS_WAIT_RESPONSE):
                try:
                    #pr(  "*** Page_write   *** 03")
                    event = clients[server][2].poll(timeout=RESPONSE_TIMEOUT)
                except zmq.ZMQError:
                    event = -1
                    pr(
                        '14 Page_write   *** Cage  %s :   ... Waiting ...   page_write: operation "%s" file channel :%d.'
                        % cage_name,
                        (request[0], request[2]),
                    )
                    time.sleep(1)
                    continue
                if event > 0:
                    answer = clients[server][2].recv()
                    if answer == b"\xFF" * 4:
                        # resend
                        try:
                            #pr(  "*** Page_write   *** 04")
                            clients[server][2].send(req)
                        except zmq.ZMQError as err:
                            set_err_int(
                                Kerr,
                                Mod_name,
                                "page_write " + cage_name,
                                3,
                                message='ZMQ client socket on server "%s" can NOT accept order with command "%s". \n Code = %s.'
                                % (server, request[0], str(err)),
                            )
                            pr(
                                "22 CageERR Pages write thread  STOPPED due to an error."
                            )
                            raise CageERR(
                                "22 CageERR   Pages write thread STOPPED due to an error."
                            )
                            #lock_write.release()
                            sys.exit()
                        continue
                    elif answer == b"\x0F" * 4:
                        #pr(  "*** Page_write   *** 05")
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
                            "page_write " + cage_name,
                            4,
                            message='Server "%s" return error in request.' % server,
                        )
                        pr("23 CageERR Pages write thread STOPPED due to an error.")
                        raise CageERR(
                            "23 CageERR   Pages write thread STOPPED due to an error."
                        )
                        #lock_write.release()
                        sys.exit()
                    else:
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "page_write " + cage_name,
                            5,
                            message='Server "%s" return unexpected answer = %s.'
                            % (
                                server,
                                answer.decode("utf-8", errors="backslashreplace"),
                            ),
                        )
                        pr("24 CageERR Pages write thread STOPPED due to an error.")
                        raise CageERR(
                            "24 CageERR   Pages write thread STOPPED due to an error."
                        )
                        #lock_write.release()
                        sys.exit()
            if event == -1:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "page_write " + cage_name,
                    6,
                    message="Cage client_id.: "
                    + str(client_id)
                    + ' Timing - not recieved respond from file server "%s" promptly.'
                    % server
                    + '\n Operation "%s" file channel :%d.' % (request[0], request[2]),
                )
                pr("25 CageERR Pages write thread STOPPED due to an error.")
                raise CageERR(
                    "25 CageERR   Pages write thread STOPPED due to an error."
                )
                #lock_write.release()
                sys.exit()

            try:
                #pr(  "*** Page_write   *** 06")
                id = (client_id, req_id_thread)

                id_byte = pickle.dumps(id)

                len_id_byte = struct.pack(">L", len(id_byte))
                id_block = len_id_byte + id_byte
                clients[server][2].send(b"\x0F" * 4 + id_block + masstr[page])
            except zmq.ZMQError as err:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "page_write " + cage_name,
                    7,
                    message='ZMQ client socket on server "%s" can NOT accept message with data to write. \n Code = %s.'
                    % (server, str(err)),
                )
                pr("26 CageERR Pages write thread STOPPED due to an error.")
                raise CageERR(
                    "26 CageERR   Pages write thread STOPPED due to an error."
                )
                #lock_write.release()
                sys.exit()

            write_confirm = False
            for i in range(ATTEMPTS_WAIT_RESPONSE):
                event = -1
                try:
                    event = clients[server][2].poll(timeout=RESPONSE_TIMEOUT)
                except zmq.ZMQError:
                    event = -1
                #pr ('\n JOIN_thread server "%s"   event =%d     request: %s' % (server, event, str(request)  ) )                 ###

                if event > 0:
                    answer = clients[server][2].recv()

                    if answer == b"\xFF" * 4:
                        resend = pickle.dumps(request)
                        clients[server][2].send(resend)
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
                            "join_thread " + cage_name,
                            1,
                            message="Cage client_id.: "
                            + client_id
                            + '\n      Recieved error message from file server "%s".'
                            % server,
                        )

                        #lock_write.release()
                        sys.exit()

                    #pr('\n JOIN_thread request >>> ' + str(request) )                                                             ###

                    try:
                        respond = pickle.loads(answer)
                    except pickle.UnpicklingError:
                        pr("\n JOIN_******* UnpicklingError ")
                        #lock_write.release()
                        sys.exit()

                    #pr('\n JOIN_thread respond <<< ' + str(respond)+'\n' )                                                ###
                    oper = respond[0]
                    # id=         respond[1]
                    # nf_serv=    respond[2]
                    # Pointer=    respond[3]
                    # data=       respond[4]
                    req_id_thread = respond[5]

                    if req_id_thread != request[5]:
                        set_err_int(
                            Kerr,
                            Mod_name,
                            "join_thread " + cage_name,
                            2,
                            message="Respond Id "
                            + str(req_id_thread)
                            + " not equal request Id "
                            + str(request[5]),
                        )
                        #pr( "\n JOIN_thread ERROR 2 --- request >>> " + str(request) )  ###
                        #pr( "\n JOIN_thread ERROR 2 --- respond <<< " + str(respond) )  ###
                        #lock_write.release()
                        sys.exit()

                    if oper == "w":
                        write_confirm = True
                        break

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
                                "join_thread " + cage_name,
                                6,
                                message="Cage client_id.: "
                                + client_id
                                + '\n      Recieved warning message from file server "%s".'
                                % server,
                            )
                        else:
                            set_err_int(
                                Kerr,
                                Mod_name,
                                "join_thread " + cage_name,
                                3,
                                message="Cage client_id.: "
                                + client_id
                                + '\n      Recieved error message from file server "%s".'
                                % server,
                            )
                        #lock_write.release()
                        sys.exit()

                #pr ('\n ... Waiting ... JOIN_thread: operation "%s" file channel :%d.'% (request[0],request[2]) )

            if not write_confirm:
                set_err_int(
                    Kerr,
                    Mod_name,
                    "join_thread " + cage_name,
                    5,
                    message="Cage client_id.: "
                    + str(client_id)
                    + ' Timing - not recieved respond from file server "%s" promptly.'
                    % server
                    + '\n Operation "%s" file channel :%d.' % (request[0], request[2]),
                )
                #lock_write.release()
                sys.exit()

            lock_memory.acquire()
            #pr(  "*** Page_write   *** 07")
            # delete element from dict.
            if (binout[page]["nbls"], fchannel) in hash2nat:
                del hash2nat[(binout[page]["nbls"], fchannel)]
                #pr("15 Page_write  *** Cage  %s :  Elem HASH of  ( block  %d,  channel  %d  ) was deleted." %  \
                            #(cage_name, binout[page]["nbls"], fchannel)   )
            else:  #  internal error - in dict. no element for pushing page
                set_err_int(
                    Kerr,
                    Mod_name,
                    "page_write " + cage_name,
                    8,
                    message="There is no page being pushed out in the dictionary.",
                )
                pr("28 CageERR Pages write thread STOPPED due to an error.")
                raise CageERR(
                    "28 CageERR   Pages write thread STOPPED due to an error."
                )
                #lock_write.release()
                lock_memory.release()
                sys.exit()

        #pr("16 Page_write  *** Cage  %s :   Page  %d  ( block  %d  of  channel  %d  ) from buffer was pushed succesfully." %  \
                #(cage_name, page, binout[page]["nbls"], fchannel)      )

        binout[page]["nbls"] = -1
        binout[page]["nf"] = -1
        binout[page]["prmod"] = False
        binout[page]["kobs"] = 0
        binout[page]["time"] = time.time()

        #pr(  "*** Page_write   *** 08")
        lock_memory.release()

        Pages_clean.put(page)

        page = None

        #lock_write.release()

        #pr ('/n/n  THREAD_WRITE_PAGE TIMING      time_to_write: %f '%  \
            #(time_to_write - int(time_to_write),) )
        #pr(  "*** Page_write   *** 09")
        continue

        # ------------------------------------------------------------
