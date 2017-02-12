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
import re

# binaries
dd = "dd"
zpool = "zpool"
truncate = "truncate"
losetup="losetup"

# buffer size in bytes
buffer_size_default=16*1024*1024
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

label_size = 262144 # bytes = 256 K = 1 label (40000 in hexa)
zfs_start_bytes = "0x"+"0"*32688+"117a0cb17ada1002" # guessed from <tt>head -c 64K /path/to/sparse_file_vdev | hexdump -C</tt> and dd if=/dev/sdxY bs=512 count=1024 | hexdump -C</tt>
    #"0x"+"00"*808+"117a0cb17ada1002"
#zfs_end_bytes = "0x"+"00"*800+"117a0cb17ada1002"
zfs_end_bytes = zfs_start_bytes
    #"0x"+"117a0cb17ada1002"
    #+ "2a819478644bce513fc5f9fbd32e703c65c90d685e1d5d27d4543f5ec92afb98" #
# the number of bytes following an occurance of zfs_end_bytes without an identifiable pattern
zfs_end_tail_bytes_count = 262144-16344-8
    #40 #16*16*4

# plac doesn't support nargs of argparse, therefore this callable has to be 
# implemented
# parses <tt>int,int,int</tt> without surrounding <tt>[]</tt> (if the format 
# has to be documented, it doesn't make sense to add extra characters -> KISS
def plac_int_list(arg):
    return arg.split(",")

@plac.annotations(
    search_file_path="file to search a ZFS pool start and end in",
    pool_name=("Needs to be specified because otherwise the import under another name isn't possible"),
    target_pool_name=("The name under which the pool ought to be imported. This is important if a pool under the name of the found pool already exists and therefore blocks the import under the same name.", "option"),
    buffer_size=("buffer size for dd in bytes, no suffixes accepted", "option", None, int),
    print_starts_ends_only=("print found start positions and end positions only and exit (allows usage of information with external tools or with the <tt>starts</tt> and <tt>ends</tt> argument)", "flag"),
    starts=("a selection of start points (e.g. a manually selected subset of start points found with <tt>print_starts_ends_only</tt> option). <tt>None</tt> indicates usage of all found start points.", "option", None, plac_int_list),
    ends=("a selection of end points (see <tt>starts</tt> for the same description). <tt>None</tt> indicates the usage of all found end points.", "option", None, plac_int_list),
    tmp_dir=("the temporary directory images are created in (this might require a lot of space depending on the size of the image which is checked)", "option"),
    loop_device=("the loop device to be used for mounting a test subset found by the script, defaults to the next free loop device determined with <tt>losetup -f</tt> if omitted. If the loop device file is in use, the script fails with the return code and error message of <tt>losetup</tt>", "option"),
)
def zfs_search(search_file_path, pool_name, target_pool_name="kdiwlcik", buffer_size=buffer_size_default, print_starts_ends_only=False, starts=None, ends=None, tmp_dir="/tmp", loop_device=sp.check_output([losetup, "-f"]).strip(), ):
    #if search_file_path != None:
    #    search_bytes = open(search_file_path, "r")
    #else:
    #    search_bytes = sys.stdin
    #    logger.info("no input file specified, reading from stdin")
    
    #raise RuntimeError("as long issue https://github.com/zfsonlinux/zfs/issues/2830 isn't fixed in the zpool command it doesn't make sense to use the script because copying files takes > 1 month regarding the number of combination, data volume and decreasing performance of a driver becoming fuller and fuller - on the other hand if it is fixed or a workaround found the script will be very efficient")
    
    # check privileges first (this is polite)
    if os.getuid() != 0:
        raise RuntimeError("Privileges are necessary to invoke zpool import tests, exiting")
    # validate loop_device option (assume that losetup -f always returns a 
    # valid and usable result)
    if re.match("/dev/loop[0-9]+", loop_device) is None:
        raise ValueError("loop_device '%s' is not a valid loop device specification" % (loop_device,))
        # @TODO: more checks for loop_device adequacy are necessary
    logger.info("using loop device %s" % (loop_device,))

    if print_starts_ends_only and (starts != None or ends != None):
        raise ValueError("starts or ends specified together with print-starts-ends-only which doesn't make sense (you don't want simply the list you specified on command line to be printed)")

    if search_file_path is None:
        raise ValueError("search_file_path mustn't be None")
    if not os.path.exists(search_file_path):
        raise ValueError("search_file_path '%s' doesn't exist" % (search_file_path,))
    #if not os.path.isfile(search_file_path): # return false for device files
    #    raise ValueError("search_file_path '%s' isn't a file, but has to be" % (search_file_path,))
    search_bytes = open(search_file_path, "r")

    # Can initialise from files, bytes, etc.
    if starts is None or ends is None:
        s = ConstBitStream(search_bytes) # if argument name is omitted, auto is used
    if starts is None:
        bitstring_result = list(s.findall(zfs_start_bytes, bytealigned=True))
        bytealigned_bitstring_result = [x/8 for x in bitstring_result]
        starts = sorted(bytealigned_bitstring_result) # ConstBitStream.find returns the bit position (rather than byte position)
        logger.info("found starts '%s'" % (str(starts)))
    else:
        starts = [int(x) for x in starts]
        logger.info("using starts\n%s\nspecified on command line" % (str(starts)))
    if ends is None:
        bitstring_result = list(s.findall(zfs_end_bytes, bytealigned=True))
        bytealigned_bitstring_result = [x/8 for x in bitstring_result]
        ends = sorted(bytealigned_bitstring_result, reverse=True) # reverse causes testing from largest possible to smallest possible (this eventually causes I/O overhead, but possibility to find a pool in the largest set is much higher)
        logger.info("found ends '%s'" % (str(ends)))
    else:
        ends = [int(x) for x in ends]
        logger.info("using ends\n%s\nspecified on command line" % (str(ends)))

    if print_starts_ends_only:
        logger.info("found start points\n%s\n and end points\n%s\nExiting" % (str(starts), str(ends)))
        return
    logger.info("discarding start positions which don't have a corresponding start position after %s bytes" % (label_size, ))
    valid_starts = []
    for start in starts:
        for start0 in starts:
	    if start0 == start + label_size:
	        valid_starts.append(start)
	        # do nothing with start0 because it might be valid for some strange reason, but if it is, it is more valid than start because there're always 2 ZFS pool labels
    logger.info("The remaining valid starts are '%s'" % (str(valid_starts)))
    starts = valid_starts
    
    logger.info("discarding end positions which don't have a corresponding end position after %s bytes" % (label_size, ))
    valid_ends = []
    for end in ends:
        for end0 in ends:
	    if end0 == end+ label_size:
	        valid_ends.append(end)
    logger.info("The remaining valid ends are '%s'" % (str(valid_ends)))
    ends = valid_ends

    logger.info("trying all %s combinations of %s start and %s end points from largest to smallest eventual result" % (str(len(starts)*len(ends)), str(len(starts)), str(len(ends)), ))
    for start in starts:
        for end in ends:
            if end < start:
                logger.info("skipping occurance of end at '%s' before start at '%s'" % (str(end),str(start)))
                continue
            end = end + zfs_end_tail_bytes_count
            
            sp.check_call([losetup, "-o", str(start), "--sizelimit", str(end-start), loop_device, search_file_path])
            logger.info("mounting subset of search file %s from byte %s to byte %s under %s" % (search_file_path, str(start), str(end), loop_device))
            
            # test zpool import
            zpool_import_process = sp.Popen([zpool, "import", "-D", ], stdout=sp.PIPE, stderr=sp.PIPE) # there's no difference in returncode between no results and successful listing of possible import -> parse output
            zpool_import_process.wait()
            zpool_import_output_tuple = zpool_import_process.communicate()
            zpool_import_output = zpool_import_output_tuple[1] # no pool available message is written onto stderr; subprocess.Popen.communicate can be invoked only once, second invokation causes error because performing I/O on closed file
            zpool_import_output_stdout = zpool_import_output_tuple[0]
            logger.debug("The output of the ''zpool import'' test command is:\n%s\n\n\n%s\n\n" % (zpool_import_output, zpool_import_output_stdout, ))
            if zpool_import_process.returncode == 0 and (zpool_import_output is None or (zpool_import_output.strip() != "no pools available to import" and not "state: UNAVAIL" in zpool_import_output)) and (zpool_import_output_stdout is None or (not "state: UNAVAIL" in zpool_import_output and not "state: UNAVAIL" in zpool_import_output_stdout)): # zpool import returncodes are useless (finding a pool with status UNAVAIL returns 0) 
                logger.info("interval %s to %s possibly contains a valid zpool. The output of the ''zpool import'' test command is:\n%s\n\n\n%s\n\nSkipping further search" % (str(start), str(end), zpool_import_output, zpool_import_output_stdout))
                logger.info("the loop device '%s' still provides the pool. Move the data from it to a reliable pool as soon as possible and detach the loop device yourself." % (loop_device,))
                return
            sp.check_call([losetup, "-d", loop_device]) # only detach loop device if import wasn't successful because otherwise the newly rescued pooled is displayed as faulted because the underlying loop device is no longer available

    return
# internal implementation notes:
# - providing possibility to read from stdin makes only sense if the search 
# result doesn't have to be tested

if __name__ == "__main__":
    plac.call(zfs_search)

