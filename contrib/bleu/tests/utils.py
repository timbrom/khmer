import subprocess
import os.path
import tempfile

thisdir = os.path.dirname(__file__)
thisdir = os.path.abspath(thisdir)

bleupath = os.path.join(thisdir, '../bleu')
bleupath = os.path.abspath(bleupath)

def run_bleu(inputfile, ksize, memory):
    """
    returns string containing output sequences
    """

    inputfile = os.path.join(thisdir, inputfile)
    assert os.path.exists(inputfile), "%s doesn't exist" % inputfile

    dir = tempfile.mkdtemp('.bleu')
    tempname = os.path.join(dir, "output.fa")
    cmd = [bleupath, inputfile, str(ksize), str(memory), tempname]
    print "COMMAND:",cmd

    p = subprocess.Popen(cmd, shell=False,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE)

    (out, err) = p.communicate()
    print '---'
    print "OUT:", out
    print '---'
    print "ERR:", err
    print '---'

    assert p.returncode == 0

    return open(tempname).read()

def get_partition(name):
    partition = int(name.rsplit('\t', 1)[1])
    return partition
