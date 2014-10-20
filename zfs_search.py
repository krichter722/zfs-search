#!/usr/bin/python

# 
# inspired by http://stackoverflow.com/questions/3217334/python-searching-reading-binary-data (CC-by-SA)
# 
# ends of ZFS pools which lie before a start which is found by this script are ignored and there's no way to search such.
# 
# asked https://groups.google.com/a/zfsonlinux.org/forum/#!topic/zfs-discuss/mmvKIywFnAk for input regarding realisation and better implementation/solution
# 

import sys
import logging
from bitstring import ConstBitStream
import plac
import os
import tempfile
import subprocess as sp

# binaries
dd = "dd"
zpool = "zpool"
truncate = "truncate"

# buffer size in bytes
buffer_size_default=4096
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
zfs_start_bytes = "0x"+"0"*32688+"117a0cb17ada1002" # guessed from <tt>head -c 64K /path/to/sparse_file_vdev | hexdump -C</tt> and dd if=/dev/sdxY bs=512 count=1024 | hexdump -C</tt>
    #"0x"+"00"*808+"117a0cb17ada1002"
zfs_end_bytes = "0x"+"00"*2048+"117a0cb17ada1002"
    #+ "2a819478644bce513fc5f9fbd32e703c65c90d685e1d5d27d4543f5ec92afb98" #
# the number of bytes following an occurance of zfs_end_bytes without an identifiable pattern
zfs_end_tail_bytes_count = 16*16*4

@plac.annotations(
search_file_path="file to search a ZFS pool start and end in",
buffer_size="buffer size for dd in bytes, no suffixes accepted",
keep_test_files=("Whether generated files for test with ''zpool import'' ought to be prevented from deletion (because they normally don't serve further purposes)", "flag"),
)
def zfs_search(search_file_path, buffer_size=buffer_size_default, keep_test_files=False):
    #if search_file_path != None:
    #    search_bytes = open(search_file_path, "r")
    #else:
    #    search_bytes = sys.stdin
    #    logger.info("no input file specified, reading from stdin")
    
    # check privileges first (this is polite)
    if os.getuid() != 0:
        raise RuntimeError("Privileges are necessary to invoke zpool import tests, exiting")
    
    if search_file_path is None:
        raise ValueError("search_file_path mustn't be None")
    if not os.path.exists(search_file_path):
        raise ValueError("search_file_path '%s' doesn't exist" % (search_file_path,))
    #if not os.path.isfile(search_file_path): # return false for device files
    #    raise ValueError("search_file_path '%s' isn't a file, but has to be" % (search_file_path,))
    search_bytes = open(search_file_path, "r")
    
    # Can initialise from files, bytes, etc.
    s = ConstBitStream(search_bytes) # if argument name is omitted, auto is used
    starts = sorted(list(s.findall(zfs_start_bytes, bytealigned=True))) # ConstBitStream.find returns the bit position (rather than byte position)
    ends = sorted(list(s.findall(zfs_end_bytes, bytealigned=True)))
    
    # create test files in a temporary directory because there might thousands and deletion and overwriting shouldn't be necessary to be checked for no advantage
    temp_dir = tempfile.mkdtemp()
    logger.info("using temporary directory '%s'" % (temp_dir,))
    test_file_name_prefix = tempfile.mkstemp(dir=temp_dir)[1]
    for start in starts:
        start = start/8
        for end in ends:
            end = end/8
            if end < start:
                logger.info("skipping occurance of end at '%s' before start at '%s'" % (str(end),str(start)))
                continue
            end = end + zfs_end_tail_bytes_count
            logger.info("creating file from byte %s to byte %s" % (str(start), str(end)))
            test_file_name = "%s-%s-%s" % (test_file_name_prefix, str(start), str(end))
            # use dd for copying in order to avoid separate handling of block devices and files (might be easy though, simply not researched at the moment); dd doesn't fail if invalid value for skip is specified which makes debugging extremely annoying, asked http://unix.stackexchange.com/questions/163026/how-to-make-dd-fail-if-it-invalid-skip-value-is-specified for input; dd prints normal output to stderr which is neither useful nor intuitive
            # copy a head and tail with bs=1 and the middle part with buffer_size for performance reasons and dd compatibility
            sp.check_call([truncate, "-s", str(end-start), test_file_name])
            count_head = -1
            if end-start > buffer_size:            
                count_head = buffer_size-((end-start) % buffer_size)
            else:
                count_head = end-start
            if count_head > 0:
                sp.check_call([dd, "if=%s" % (search_file_path,), "of=%s" % (test_file_name,), "bs=1", "count=%s" % (str(count_head),), "skip=%s" % (str(start),), ], stdout=sp.PIPE, stderr=sp.PIPE)
            # copy with buffer_size for performance
            count = 0
            if (end-start-count_head) > buffer_size:
                count = (end-start-count_head) / buffer_size
                sp.check_call([dd, "if=%s" % (search_file_path,), "of=%s" % (test_file_name,), "bs=%s" % (str(buffer_size)), "count=%s" %(str(count),), "skip=%s" % (str((start+count_head)/buffer_size),), "seek=%s" % (str(count_head),), ], stdout=sp.PIPE, stderr=sp.PIPE)
            # copy rest
            count_tail = (end-start-count_head) % buffer_size
            if count_tail > 0:
                sp.check_call([dd, "if=%s" % (search_file_path,), "of=%s" % (test_file_name,), "bs=1", "count=%s" % (str(count_tail),), "skip=%s" % (str(start+count_head+count*buffer_size),), "seek=%s" % (str(count_head+count*buffer_size),), ], stdout=sp.PIPE, stderr=sp.PIPE)
            
            # test zpool import
            zpool_import_process = sp.Popen([zpool, "import", "-d", os.path.realpath(os.path.join(test_file_name, "..")), "-D", ]) # there's no difference in returncode between no results and successful listing of possible import -> parse output
            zpool_import_process.wait()
            zpool_import_output = zpool_import_process.communicate()[1] # no pool available message is written onto stderr
            if (zpool_import_output is None or zpool_import_output.strip() != "no pools available to import") and zpool_import_process.returncode == 0:
                logger.info("interval %s to %s possibly contains a valid zpool. The output of the ''zpool import'' test command is:\n%s\n\nThe file which succeeded is '%s'. Skipping further search" % (str(start), str(end), zpool_import_output, test_file_name))
                return
            else:
                if not keep_test_files:
                    os.remove(test_file_name)
    
    return
    
    #solution if zfs_start_bytes and zfs_end_bytes would differ
    
    # Search to Start of Frame 0 code on byte boundary
    found = s.find(zfs_start_bytes, bytealigned=True)
    found_pos = found[0]
    if found:
        print("Found start code at byte offset %d." % found_pos)
        s0f0, length, bitdepth, height, width = s.readlist('hex:16, uint:16, uint:8, 2*uint:16')
        print("Width %d, Height %d" % (width, height))
    
    # search the end
    s2 = ConstBitStream(search_bytes) # assume that stream can be processed further (implies that ends which lie before start are ignored which shouldn't be a problem) (if search_bytes is a file, it is reset anyway (therefore specifying start below is necessary))
    found = s2.find(
        zfs_end_bytes, 
        start=found_pos+1, # skip occurance if zfs_start_bytes and zfs_end_bytes are identical
        bytealigned=True)
    if found:
        print("Found end code at byte offset %d." % found[0])
        s0f0, length, bitdepth, height, width = s2.readlist('hex:16, uint:16, uint:8, 2*uint:16')
        print("Width %d, Height %d" % (width, height))
# internal implementation notes:
# - providing possibility to read from stdin makes only sense if the search 
# result doesn't have to be tested

if __name__ == "__main__":
    plac.call(zfs_search)

