__author__ = 'mh719'

from subprocess import Popen, PIPE

def execCmd(cmd):
    try:
        p = Popen(cmd,stdout=PIPE,stderr=PIPE,close_fds=True, shell=True)
        stdo,stde = p.communicate()
        p.wait()
        ret = p.returncode
        return (ret,stdo,stde)
    except OSError as e:
        print("Error", e.errno)
        print("Error",e)
        raise e