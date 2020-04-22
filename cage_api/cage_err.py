# Cage system (Cage file server v.3.0)
#   Module with utility classes and static functions for
#   errors processing and debugging
# © A.S.Aliev, 2018

import os
import sys
import datetime
import time
import threading

from cage_par_cl import *

Log_err = "cage_error.log"  # default errlog file

Log_print = "cage_print.txt"  # file for copying/redirecting system
#   outstream(printing)

# -----------------------------------------------------
# open error logfile
try:
    errlog = open(Log_err, "at", encoding="utf-8")
except OSError as err:
    print(
        "File operation for errors log <"
        + str(Log_err)
        + "> raise exception:"
        + str(err)
    )
    sys.exit()

# -----------------------------------------------------

# Class for redirecting system out stream (print function)
class Logger(object):
    def __init__(self, filename=Log_print, term=True):
        self.term = term
        if self.term:
            self.terminal = sys.stdout
        self.log = open(filename, "w", encoding="utf-8")

    def __del__(self):
        self.log.close()
        if self.term:
            del self.terminal
        del self.log

    def write(self, message):
        if self.term:
            self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        pass

# -----------------------------------------------------

# Cage system exception
class CageERR(Exception):
    pass


# -----------------------------------------------------
# check difference among two dict
class DictDiffer(object):
    """
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values
    © fcmax.livejournal.com/10291.html
    """

    def __init__(self, current_dict, past_dict):
        self.current_dict = current_dict
        self.past_dict = past_dict
        self.set_current = set(current_dict.keys())
        self.set_past = set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)

    def added(self):
        return self.set_current - self.intersect

    def removed(self):
        return self.set_past - self.intersect

    def changed(self):
        return set(
            o for o in self.intersect if self.past_dict[o] != self.current_dict[o]
        )

    def unchanged(self):
        return set(
            o for o in self.intersect if self.past_dict[o] == self.current_dict[o]
        )


# -----------------------------------------------------

# debug messages generating (printing in debug state and record copy to errlog)
def pr(
    message="",
    func="",  # name of function or class method
    proc_inf=False,  # show process and treads info
    cage_debug=CAGE_DEBUG,
    wait=0.00001,  # timeout for comfort in parallel environment
    tim=False,
):

    if cage_debug :

        time.sleep(wait)
        dt = datetime.datetime.now().strftime("%d.%m.%y  %H:%M:%S .%f")
        tm = datetime.datetime.now().strftime("%M:%S .%f")
        if func != "":
            print("\n*** DEBUG ***  %s   ***  function: %s" % (dt, func))
        # else:
        # print ('\n*** DEBUG ***    ', dt)
        if proc_inf:
            print(
                "          --- parent process:",
                os.getppid(),
                " --- self process : ",
                os.getpid(),
                " --- thread :",
                threading.get_ident(),
            )
            time.sleep(wait)
            errlog.write(
                "\n"
                + dt
                + "  *** DEBUG *** "
                + str(func)
                + " parent pr.:"
                + str(os.getppid())
                + " , self pr. : "
                + str(os.getpid())
                + " , thread :"
                + str(threading.get_ident())
            )
            errlog.flush()
        mess = bytes(message, "utf-8").decode("utf-8", "ignore")
        if tim == False:
            tm = ""
        if len(mess) > 0:
            print(" %s >>> %s   " % (tm, mess))
            time.sleep(wait)
            errlog.write("\n %s >>> %s " % (tm, mess))
            errlog.flush()


# -----------------------------------------------------

# set Kerr list  empty
def zero_Kerr(Kerr, type=("s", "e"), my_debug=False):

    if my_debug or __debug__:
        nerr = len(Kerr) - 1
        while True:
            if nerr == -1:
                break
            if Kerr[nerr][0] in type:
                del Kerr[nerr]
            nerr -= 1


# -----------------------------------------------------

# set warning code "w" to error list "Kerr",
# write record to errlog and generate debugging message
def set_warn_int(
    Kerr,
    inst,  # name of module or class instance
    func="",  # name of function or class method
    int_err="",  # serial internal Cage system error number in function or method
    message="",
    cage_debug=False,
):

    dt = datetime.datetime.now().strftime("%d.%m.%y  %H:%M:%S. %f")
    #
    if str(inst)[0] == "*":  # error in static function
        n = "module - function > " + str(inst)[1:] + " - " + func
        Kerr.append(("w", str(inst)[1:], "", func, str(int_err), dt))
    else:  # error in class method
        n = (
            "module - class - function > "
            + inst.__class__.__module__
            + " - "
            + inst.__class__.__name__
            + " - "
            + func
        )
        Kerr.append(
            (
                "w",
                inst.__class__.__module__,
                inst.__class__.__name__,
                func,
                str(int_err),
                dt,
            )
        )
    if CAGE_DEBUG:
        errlog.write("\n" + dt + " -!- program warning -!- ")
        errlog.write("\n" + "          Kerr = " + str(Kerr))
        if message != "":
            errlog.write("\n >>> " + str(message))

    if cage_debug and __debug__:

        pr(" *** WARNING  in " + n + " *** \n Kerr= " + str(Kerr))
        if message != "":
            pr(" >>> " + str(message) + "\n")
    errlog.flush()


# -----------------------------------------------------

# set serious program error code "e" to error list "Kerr",
# write record to errlog and generate debugging message
def set_err_int(
    Kerr,
    inst,
    func="",
    int_err="",  # serial internal Cage system error number in function or method
    message="",
    cage_debug=True,
):

    dt = datetime.datetime.now().strftime("%d.%m.%y  %H:%M:%S. %f")
    #
    if str(inst)[0] == "*":  # error in static function
        n = "module - function > " + str(inst)[1:] + " - " + func
        Kerr.append(("e", str(inst)[1:], "", func, str(int_err), dt))
    else:  # error in class method
        n = (
            "module - class - function > "
            + inst.__class__.__module__
            + " - "
            + inst.__class__.__name__
            + " - "
            + func
        )
        Kerr.append(
            (
                "e",
                inst.__class__.__module__,
                inst.__class__.__name__,
                func,
                str(int_err),
                dt,
            )
        )

    errlog.write("\n" + dt + " -*- program error -*- ")
    errlog.write("\n" + "          Kerr = " + str(Kerr))
    if message != "":
        errlog.write("\n >>> " + str(message))

    if cage_debug and __debug__:
        # )
        pr(" *** PROGRAM error in " + n + " *** \n Kerr= " + str(Kerr))
        if message != "":
            pr(" >>> " + str(message) + "\n")
    errlog.flush()


# -----------------------------------------------------

# set system error code "s" to error list "Kerr",
# write record to errlog and generate debugging message
#  - used usually for record OS exceptions
def set_err_syst(
    Kerr,
    inst,
    func="",
    int_err="",  # serial internal Cage system error number in function or method
    OSsubsyst="",
    pathfile="",  # pathname for OS file system errors
    sys_err="",  # os error code generated by exception
    message="",
    cage_debug=True,
):

    dt = datetime.datetime.now().strftime("%d.%m.%y  %H:%M:%S. %f")
    #
    if str(inst)[0] == "*":  # error in static function
        n = "module - function > " + str(inst)[1:] + " - " + func
        Kerr.append(
            (
                "s",
                str(inst)[1:],
                "",
                func,
                str(int_err),
                dt,
                OSsubsyst,
                pathfile,
                sys_err,
            )
        )
    else:  # error in class method
        n = (
            "module - class - function > "
            + inst.__class__.__module__
            + " - "
            + inst.__class__.__name__
            + " - "
            + func
        )
        Kerr.append(
            (
                "s",
                inst.__class__.__module__,
                inst.__class__.__name__,
                func,
                str(int_err),
                dt,
                OSsubsyst,
                pathfile,
                sys_err,
            )
        )

    errlog.write("\n" + dt + " *** system error *** ")
    errlog.write("\n" + "          Kerr = " + str(Kerr))
    if message != "":
        errlog.write("\n >>> " + str(message))

    if cage_debug and __debug__:
        #
        pr(" *** SYSTEM error in " + n + " *** \n Kerr= " + str(Kerr))
        if message != "":
            pr(" >>> " + str(message) + "\n")
    errlog.flush()


# -----------------------------------------------------

# check for presense serious "e" and/or system "s" errors
# in error list "Kerr"
def is_err(Kerr, types="*"):

    ind = len(Kerr) - 1
    while ind >= 0:
        if len(Kerr[ind][4]) > 0:
            if types == "*" and Kerr[ind][0] != "w":
                return ind
            elif types.find(Kerr[ind][0]) >= 0:
                return ind
        ind -= 1
    return -1


# -----------------------------------------------------


def dream(i=1):
    for j in range(int(i * 500000)):
        k = j * i
    return k
