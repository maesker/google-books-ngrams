#!/usr/bin/python2.7.9
# -*- coding: iso-8859-15 -*-

import time
import string
import argparse
import os
import sys
import shutil
import gzip
import glob
import re
import io
import collections
import multiprocessing

__author__ = "Markus Mäsker"
__email__ = "maesker@gmx.net"
__version__ = "0.2"
__status__ = "Development"

# http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-1gram-20120701-a.gz
# https://storage.googleapis.com/books/ngrams/books/datasetsv2.html

SERVER = "http://storage.googleapis.com/books/ngrams/books"
WORK_base = os.path.join(os.getcwd(), "google-ngrams")
DL = os.path.join(os.getcwd(), 'storage.googleapis.com/books/ngrams/books')
DL_ARCHIVE = os.path.join(os.getcwd(), "archive")

VALID_LANGS = ['eng', 'chi-sim', 'fre', 'ger', 'heb', 'ita', 'rus', 'spa']
SPECIAL_TERMS = ['pos', 'punctuation', 'other']
STATS_FILE_LIMIT_BYTES = 100
CORES = multiprocessing.cpu_count()


def setup():
    """create directores"""
    for i in [DL, DL_ARCHIVE]:
        if not os.path.isdir(i):
            os.makedirs(i)


def download(file):
    """download file"""
    if not os.path.isfile(os.path.join(DL_ARCHIVE, file)):
        os.system("""wget -r --no-parent %s/%s """ % (SERVER, file))
        shutil.move(os.path.join(DL, file), os.path.join(DL_ARCHIVE, file))


def get_ngram_keys(ngrams=2):
    """generate the list of suffixes for google ngram link generation"""
    dst = SPECIAL_TERMS
    if ngrams == 1:
        for i in string.lowercase + string.digits:
            dst.append(i)
    else:
        src = ['_']
        src.extend(string.lowercase)
        for i in string.digits:
            dst.append("%s" % (i))
        for i in string.lowercase:
            for j in src:
                dst.append("%s%s" % (i, j))
    return dst


def processfile(filename, inputdir, outputdir, linepattern):
    mapping = collections.defaultdict(int)
    lines = 0
    int_ref = int
    matchref = linepattern.match
    outputfile_abspath = "%s/summed_%s" % (outputdir, filename)
    outputfile_stats_abspath = "%s_stats.txt" % outputfile_abspath
    inputfile = os.path.join(inputdir, filename)
    outputfile = gzip.GzipFile(outputfile_abspath, 'w', 9)

    gz = gzip.GzipFile(inputfile, 'rb')
    gzio = io.BufferedReader(gz, buffer_size=1024 * 1024 * 32)
    for line in gzio:
        lines += 1
        m = matchref(line)
        if m:
            mapping[m.group('str')] += int_ref(m.group("occs"))

    outputlines = 0
    bufout = io.BufferedWriter(outputfile, buffer_size=1024 * 1024 * 32)
    for k, v in mapping.iteritems():
        bufout.write("%s;%i\n" % (k, v))
        outputlines += 1
    bufout.flush()
    bufout.close()
    outputfile.close()
    filestats = os.stat(inputfile)
    outfile_stats = os.stat(outputfile_abspath)
    with open(outputfile_stats_abspath, "w") as fp_stats:
        fp_stats.write("\nv2;%s;%d;%d;%d;%d" % (os.path.basename(
            filename), lines, outputlines, filestats.st_size,
                                                outfile_stats.st_size))
    os.remove(inputfile)  # rm tmp/... file


def process(filename, outputdir, pattern):
    limit = 100 * 1000 * 1000
    bufsize = 1024 * 1024 * 128
    line_iter = 0    # splitting
    inputfile = os.path.join(DL_ARCHIVE, filename)
    if not os.path.isfile(inputfile):
        print "File ", inputfile, " not found"
        return -1
    outputdir_3 = os.path.join(outputdir, "merged")
    outputdir_2 = os.path.join(outputdir, "proc1")
    tmp_output_dir = os.path.join(outputdir, "tmp")
    for i in [tmp_output_dir, outputdir_2, outputdir_3]:
        if not os.path.isdir(i):
            os.makedirs(i)

    # SPLIT
    gz = gzip.GzipFile(inputfile, 'rb')
    gzio = io.BufferedReader(gz, buffer_size=bufsize)
    outputfile_suffix = 1
    outputfile = gzip.GzipFile(
        os.path.join(tmp_output_dir, "%s_%s.gz" %
                     (filename, outputfile_suffix)), 'w', 1)
    bufout = io.BufferedWriter(outputfile, buffer_size=bufsize)
    for line in gzio:
        line_iter += 1
        bufout.write(line)
        if not (line_iter % limit):
            line_iter = 0
            bufout.flush()
            bufout.close()
            outputfile.close()
            outputfile_suffix += 1
            outputfile = gzip.GzipFile(
                os.path.join(tmp_output_dir, "%s_%s.gz" %
                             (filename, outputfile_suffix)), 'w', 1)
            bufout = io.BufferedWriter(outputfile, buffer_size=bufsize)
    bufout.flush()
    bufout.close()
    outputfile.close()

    # SPLIT done.
    # process tmp files
    cnt = CORES  # multiprocessing.cpu_count()
    pool = []
    filelist = os.listdir(tmp_output_dir)
    while len(filelist) > 0:
        if len(pool) < cnt:
            f = filelist.pop(0)
            p = multiprocessing.Process(target=processfile,
                                        args=(f, tmp_output_dir,
                                              outputdir_2, pattern,))
            pool.append(p)
            p.start()
        else:
            while True:
                p = pool.pop(0)
                if not p.is_alive():
                    p.join()
                    break
                else:
                    pool.append(p)
                time.sleep(1)
    while len(pool) > 0:
        p = pool.pop(0)
        p.join()
    # PROCESSING done.

    # MERGE
    errors_valueerror = 0
    errors_other = 0
    # mapping might grow to considerable size. If your workstation runs out of
    # memory, try to replace this with shelve.
    # An alternative would be to only create a set of the keys to test for
    # duplicates and handle those in a seperate stage
    mapping = {}
    int_ref = int
    for fl in glob.glob("%s/summed_%s*.gz" % (outputdir_2, filename)):
        try:
            gz = gzip.GzipFile(fl, 'rb')
            gzio = io.BufferedReader(gz, buffer_size=1024 * 1024 * 32)
            for line in gzio:
                try:
                    k, v = line.rsplit(';', 1)
                    if k in mapping:
                        mapping[k] += int_ref(v)
                    else:
                        mapping[k] = int_ref(v)
                except ValueError, e:
                    errors_valueerror += 1
                    print line
            gzio.close()
            gz.close()
            os.remove(fl)
        except:
            errors_other += 1
    if errors_other + errors_valueerror:
        print "%i value errors occured." % errors_valueerror
        print "%i other errors occured." % errors_other
        exit(0)

    outputfile_abspath = "%s/summed_%s" % (outputdir_3, filename)
    outputfile = gzip.GzipFile(outputfile_abspath, 'w', 3)
    for k, v in mapping.iteritems():
        outputfile.write("%s;%i\n" % (k, v))
    outputfile.close()

    # only move if write was complete
    shutil.move(outputfile_abspath, os.path.dirname(outputdir_3))


def worker_process(outputdir, filename, config):
    complete = 0
    outputfile_abspath = "%s/summed_%s" % (outputdir, filename)
    outputfile_stats_abspath = "%s_stats.txt" % outputfile_abspath
    if os.path.isfile(outputfile_stats_abspath):
        filestats = os.stat(outputfile_stats_abspath)
        if filestats.st_size >= STATS_FILE_LIMIT_BYTES:
            complete = 1
    if not complete:
        download(filename)
        if config['merge']:
            process(filename, outputdir, config['line_pattern'])


def master_process(outputdir, filelist, config):
    """Master download process """
    if not os.path.isdir(DL):
        raise Exception("Not a directory %s", DL)
    if not os.path.isdir(outputdir):
        os.makedirs(outputdir)

    pool = []
    while len(filelist) > 0:
        if len(pool) < CORES:
            f = filelist.pop(0)
            p = multiprocessing.Process(
                target=worker_process, args=(outputdir, f, config, ))
            pool.append(p)
            p.start()
        else:
            while True:
                p = pool.pop(0)
                if not p.is_alive():
                    p.join()
                    break
                else:
                    pool.append(p)
                time.sleep(1)
    while len(pool) > 0:
        p = pool.pop(0)
        p.join()


def process_1gram(config):
    """ Process 1grams """
    config['line_pattern'] = re.compile(
        "(?P<str>\S+)\s+(?P<year>[0-9]+)\s+(?P<occs>[0-9]+)\s+(?P<books>[0-9]+)")
    for language in config['languages']:
        files = []
        outputdir = os.path.join(WORK_base, "1grams", language)
        for i in get_ngram_keys(1):
            files.append(
                "googlebooks-%s-all-1gram-20120701-%s.gz" % (language, i))
        master_process(outputdir, files, config)


def process_2gram(config):
    config['line_pattern'] = re.compile(
        "(?P<str>\S+\s\S+)\s+(?P<year>[0-9]+)\s+(?P<occs>[0-9]+)\s+(?P<books>[0-9]+)")
    for language in config['languages']:
        outputdir = os.path.join(WORK_base, "2grams", language)
        files = []
        for i in get_ngram_keys():
            files.append(
                "googlebooks-%s-all-2gram-20120701-%s.gz" % (language, i))
        master_process(outputdir, files, config)


def process_3gram(config):
    config['line_pattern'] = re.compile(
        "(?P<str>\S+\s\S+\s\S+)\s+(?P<year>[0-9]+)\s+(?P<occs>[0-9]+)\s+(?P<books>[0-9]+)")
    for language in config['languages']:
        outputdir = os.path.join(WORK_base, "3grams", language)
        files = []
        for i in get_ngram_keys():
            files.append(
                "googlebooks-%s-all-3gram-20120701-%s.gz" % (language, i))
        master_process(outputdir, files, config)


def process_4gram(config):
    config['line_pattern'] = re.compile(
        "(?P<str>\S+\s\S+\s\S+\s\S+)\s+(?P<year>[0-9]+)\s+(?P<occs>[0-9]+)\s+(?P<books>[0-9]+)")
    for language in config['languages']:
        outputdir = os.path.join(WORK_base, "4grams", language)
        files = []
        for i in get_ngram_keys():
            files.append(
                "googlebooks-%s-all-4gram-20120701-%s.gz" % (language, i))
        master_process(outputdir, files, config)


def process_5gram(config):
    config['line_pattern'] = re.compile(
        "(?P<str>\S+\s\S+\s\S+\s\S+\s\S+)\s+(?P<year>[0-9]+)\s+(?P<occs>[0-9]+)\s+(?P<books>[0-9]+)")
    for language in config['languages']:
        outputdir = os.path.join(WORK_base, "5grams", language)
        files = []
        for i in get_ngram_keys():
            files.append(
                "googlebooks-%s-all-5gram-20120701-%s.gz" % (language, i))
        master_process(outputdir, files, config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Google ngrams processing')
    parser.add_argument(
        '-n', '--ngrams', required=True,
        help='Ngram to process. [1, 2, 3, 4, 5, all]')

    parser.add_argument(
        '-l', '--language', default='eng',
        help="The language to download, ['eng', 'chi-sim', 'fre', 'ger', 'heb', 'ita', 'rus', 'spa']")

    parser.add_argument(
        '-m', '--merge', action='store_true',
        help="Merge the ngrams individual years and books, to a single entry"
    )

    args = parser.parse_args()

    setup()

    langs = VALID_LANGS
    if args.language in VALID_LANGS:
        langs = [args.language]

    config = {
        'languages': langs,
        'merge': args.merge
    }
    if args.ngrams == '1':
        process_1gram(config)
    elif args.ngrams == '2':
        process_2gram(config)
    elif args.ngrams == '3':
        process_3gram(config)
    elif args.ngrams == '4':
        process_4gram(config)
    elif args.ngrams == '5':
        process_5gram(config)

    print "Done."
    sys.exit(1)
