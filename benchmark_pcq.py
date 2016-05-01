import urllib2
import gzip
import re
import os, tempfile, shutil, sys
import hashlib
import math
import csv, StringIO
from rdflib import *
from rdflib.compare import to_isomorphic
import collections
from multiprocessing import Manager, Process, Pool, Queue, Event, JoinableQueue, cpu_count
from Queue import Empty
import requests

GRAPH_RE = re.compile("<([^<^>^ ^\"]+)> *\.$")


def download(url):
    """Helper to download large files
        the only arg is a url
       this file will go to a temp directory
       the file will also be downloaded
       in chunks and print out how much remains
    """

    baseFile = '_'.join(url.split('/')[-4:]) #os.path.basename(url)

    #move the file to a more uniq path
    os.umask(0002)
    temp_path = "/data/"
    f = os.path.join(temp_path,baseFile)
    file_exists = os.path.isfile(f)
    
    if not file_exists:
        sys.stderr.write("Downloading %s from the web\n"%url)
        try:
            req = urllib2.urlopen(url)
            total_size = int(req.info().getheader('Content-Length').strip())
            downloaded = 0
            CHUNK = 256 * 10240
            with open(f+".download", 'wb') as fp:
                while True:
                    chunk = req.read(CHUNK)
                    downloaded += len(chunk)
                    #print math.floor( (downloaded / total_size) * 100 )
                    if not chunk: break
                    fp.write(chunk)
            shutil.move(f+".download", f)
        except urllib2.HTTPError, e:
            sys.stderr.write("HTTP Error: %s %s\n"%(e.code , url))
            return False
        except urllib2.URLError, e:
            sys.stderr.write("URL Error: %s %s\n"%(e.reason , url))
            return False
        sys.stderr.write("Downloaded %s from the web\n"%url)

    sys.stderr.write("File ready: %s\n"%url)
    return f

stat_cols = [
    'id',
    'tree_depth',
    'color_count',
    'individuations',
    'prunings',
    'initial_color_count',
    'adjacent_nodes',
    'initial_coloring_runtime',
    'triple_count',
    'graph_digest',
    'to_hash_runtime',
    'canonicalize_triples_runtime',
    'lines',
    'ontology',
    'error',
    ]

def digest_graph(uri, turtle):
    stats = collections.defaultdict(str)
    stats["id"] = uri
    #stats['lines'] = len(nquads.split('\n'))
    #sys.stderr.flush()
    try:
        g = ConjunctiveGraph()
        g.parse(data=turtle, format="turtle")
        #sys.stderr.write("Processing %s (%d)...\n"%(uri, len(g)))
        sys.stderr.write('.')
        sys.stderr.flush()
        stats['ontology'] = g.value(predicate=RDF.type, object=OWL.Class) is not None
        ig = to_isomorphic(g)
        graph_digest = ig.graph_digest(stats)
        #sys.stderr.write("Processed %s with %s triples in %s sec.\n"%(uri, len(g), stats['to_hash_runtime']))
    except Exception as e:
        sys.stderr.write("ERROR: %s %s\n"%(uri, e))
        sys.stderr.flush()
        stats['error'] = str(e)
        #print nquads
    return [unicode(stats[c]).encode("ascii","ignore") for c in stat_cols]

def segment_graphs(url):
    if ".nq" not in url:
        return
    local_file = download(url)
    store_dir = "/mnt"+local_file+".triplestore"
    store_exists = os.path.isdir(store_dir)
    graph = ConjunctiveGraph('Sleepycat')
    # first time create the store:
    graph.open(store_dir, create = True)
    if not store_exists:
        statements = gzip.GzipFile(fileobj=open(local_file, 'rb'))
        for line in statements:
            try:
                graph.parse(data=line, format="nquads")
            except Exception as e:
                sys.stderr.write("ERROR: %s %s\n"%(line, e))
                sys.stderr.flush()

        print "Loaded", store_dir
    return graph.contexts(), store_dir

submitted = Event()
processed = Event()

def work(id, jobs, result):
    while True:
        try:
            turtle, uri = jobs.get(timeout=10)
            #graph = ConjunctiveGraph('Sleepycat', identifier=uri)
            # first time create the store:
            #graph.open(path)
            #print uri
            stats_line = digest_graph(uri, turtle)
            result.put(stats_line)
            jobs.task_done()
        except Empty:
            if submitted.is_set():
                processed.set()
                break

def read(jobs, file_list):
    for url in file_list:
        contexts, path = segment_graphs(url.strip())
        print "Submitting contexts for", path
        for context in contexts:
            try:
                jobs.put((context.serialize(format="turtle"), context.identifier), True)
            except Exception as e:
                sys.stderr.write("ERROR: %s %s\n"%(context.identifier, e))
                sys.stderr.flush()
        
    submitted.set()

def process_one(uri):
    stats = collections.defaultdict(str)
    stats["id"] = uri
    stats['debug'] = True
    try:
        g = ConjunctiveGraph()
        g.parse(data=requests.get(uri,headers={"Accept":"application/rdf+xml"}).text,format="xml")
        #print g.serialize(format="turtle")
        stats['lines'] = len(g)
        sys.stderr.write("Processing %s (%d)...\n"%(uri, stats['lines']))
        sys.stderr.flush()
        stats['ontology'] = g.value(predicate=RDF.type, object=OWL.Class) is not None
        ig = to_isomorphic(g)
        graph_digest = ig.graph_digest(stats)
        sys.stderr.write("Processed %s with %s triples in %s sec.\n"%(uri, len(g), stats['to_hash_runtime']))
    except Exception as e:
        sys.stderr.write("ERROR: %s %s\n"%(uri, e))
        stats['error'] = str(e)
    sys.stderr.flush()
    print '\n'.join(['%s:\t%s'%(key, str(value)) for key, value in stats.items()])
    return [str(stats[c]) for c in stat_cols]

            
def main(file_list, outputFile):
    jobs = JoinableQueue(10000)
    result = JoinableQueue()

    loader_processes = 3
    NUMBER_OF_PROCESSES = cpu_count() - loader_processes
    files = [ [] for x in range(loader_processes)]
    for i, f in enumerate(open(file_list)):
        files[i%loader_processes].append(f)

    for i in range(loader_processes):
        Process(target=read, args=(jobs, files[i])).start()

    for i in xrange(NUMBER_OF_PROCESSES):
        p = Process(target=work, args=(i, jobs, result))
        p.daemon = True
        p.start()

    o = csv.writer(open(outputFile, 'w'), delimiter=',')
    o.writerow(stat_cols)
    while not submitted.is_set() and not processed.is_set():
        row = result.get()
        o.writerow(row)
        result.task_done()
        
if __name__ == '__main__':
    if len(sys.argv) == 2:
        process_one(sys.argv[1])
    else:
        main(sys.argv[1],sys.argv[2])
