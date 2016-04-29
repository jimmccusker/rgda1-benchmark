#!/usr/bin/env python

from mrjob.job import MRJob, MRStep, RawValueProtocol
from mrjob.protocol import PickleProtocol
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
import boto.s3
from threading import Thread, Condition

GRAPH_RE = re.compile("<([^<^>^ ^\"]+)> *\.$")

from boto.s3.connection import S3Connection
conn = S3Connection()

def download(url, bucket_id, key_prefix):
    """Helper to download large files
        the only arg is a url
       this file will go to a temp directory
       the file will also be downloaded
       in chunks and print out how much remains
    """

    baseFile = '_'.join(url.split('/')[-4:]) #os.path.basename(url)

    #move the file to a more uniq path
    os.umask(0002)
    temp_path = "/tmp/"
    file = os.path.join(temp_path,baseFile)
    bucket = conn.get_bucket(bucket_id)
    key = bucket.get_key(key_prefix + baseFile, validate=False)
    s3_exists = key.exists()
    file_exists = os.path.isfile(file)
    
    if not file_exists and s3_exists:
        sys.stderr.write("Downloading %s from S3\n"%url)
        key.get_contents_to_filename(file)
        sys.stderr.write("Downloaded %s from S3\n"%url)
    elif not file_exists and not s3_exists:
        sys.stderr.write("Downloading %s from the web\n"%url)
        try:
            req = urllib2.urlopen(url)
            total_size = int(req.info().getheader('Content-Length').strip())
            downloaded = 0
            CHUNK = 256 * 10240
            with open(file, 'wb') as fp:
                while True:
                    chunk = req.read(CHUNK)
                    downloaded += len(chunk)
                    #print math.floor( (downloaded / total_size) * 100 )
                    if not chunk: break
                    fp.write(chunk)
        except urllib2.HTTPError, e:
            sys.stderr.write("HTTP Error: %s %s\n"%(e.code , url))
            return False
        except urllib2.URLError, e:
            sys.stderr.write("URL Error: %s %s\n"%(e.reason , url))
            return False
        sys.stderr.write("Downloaded %s from the web\n"%url)

    if not s3_exists:
        sys.stderr.write("Uploading %s to S3\n"%url)
        key.set_contents_from_filename(file)

    sys.stderr.write("File ready: %s\n"%url)
    return file

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
    
class BTCBenchmark(MRJob):

    OUTPUT_PROTOCOL = RawValueProtocol
    INTERNAL_PROTOCOL = PickleProtocol

    JOBCONF = {
        "mapred.task.timeout": 800000,
    }
    
    def steps(self):
        return [
            MRStep(mapper_pre_filter='grep "data.nq"', mapper=self.process,reducer='cat'),#self.merge_results),
            #MRStep(reducer=self.digest_graph),
            #MRStep(reducer=self.merge_results)
        ]

    def combine_graphs(self, graph_uri, nquads):
        yield graph_uri, '\n'.join(nquads)

    def bundle_segment_graphs(self, _, line):
        graphs = collections.defaultdict(list)
        i = 0
        for graph, nquads in self.segment_graphs(_,line):
            i += 1
            graphs[graph].append(nquads)
            if len(graphs) % 1000 == 0:
                yield i, graphs.items()
                graphs = collections.defaultdict(list)
        yield i, graphs.items()

    def _digest(self, uri):
        m = hashlib.md5()
        m.update(uri)
        return m.hexdigest()[-3:]

    def process(self,_, f):
        local_file = download(f, "btc-2014", "archives/")
        graph = ConjunctiveGraph('Sleepycat')
        # first time create the store:
        graph.open(local_file+".triplestore", create = True)
        for line in gzip.GzipFile(fileobj=open(local_file, 'rb')):
            try:
                graph.parse(data=line, format="nquads")
            except Exception as e:
                sys.stderr.write("ERROR: %s %s\n"%(line, e))

        results = {}
        for context in graph.contexts():
            try:
                uri = context.identifier
                g = ConjunctiveGraph()
                g += context
                sys.stderr.write("Processing %s with %s triples...\n"%(uri, len(g)))
                sys.stderr.flush()
    
                stats = collections.defaultdict(str)
                stats["id"] = uri
                ig = to_isomorphic(g)
                graph_digest = ig.graph_digest(stats)
            
                sys.stderr.write("Processed %s with %s triples in %s sec.\n"%(uri, len(g), stats['to_hash_runtime']))
            except Exception as e:
                sys.stderr.write("ERROR: %s %s\n"%(uri, e))
                stats['error'] = str(e)
            sys.stderr.flush()
            stats_line = [unicode(stats[c]).encode('ascii', 'ignore') for c in stat_cols]
            results[uri] = stats_line
        sys.stderr.write("Digested %s into %s graphs.\n"%(f, len(results)))
        results_string = '\n'.join([','.join(row) for row in results.values()])
        yield "benchmark", results_string
                
    def segment_graphs(self, _, line):
        if ".nq" not in line:
            return
        local_file = download(line, "btc-2014", "archives/")
        statements = gzip.GzipFile(fileobj=open(local_file, 'rb'))
        lines = [(GRAPH_RE.search(statement).group(1), statement) for statement in statements]
        lines = sorted(lines, key=lambda x: x[0])
        current_graph = None
        current_graph_statements = []
        #b = []
        #i = 0
        #j = 0
        #for statement in statements:
        #    graph = GRAPH_RE.search(statement).group(1)
        #    b.append((str(graph), statement))
        #    i = (i + 1) % 100000
        #    if i == 0:
        #        j += 1
        #        sys.stderr.write("processing batch %s\n"% j)
        #        b = sorted(b, key=lambda x: x[0])
        #        for graph, statement in b:
        #            if current_graph != graph:
        #                last_graph = current_graph
        #                last_graph_statements = '\n'.join(current_graph_statements)
        #                current_graph = graph
        #                current_graph_statements = []
        #                if last_graph is not None:
        #                    yield self._digest(last_graph), last_graph_statements
        #            current_graph_statements.append(statement)
        #        yield self._digest(current_graph), '\n'.join(current_graph_statements)
        #        b = []

        #b = sorted(b, key=lambda x: x[0])
        for graph, statement in lines:
            if current_graph != graph:
                last_graph = current_graph
                last_graph_statements = '\n'.join(current_graph_statements)
                current_graph = graph
                current_graph_statements = []
                if last_graph is not None:
                    yield last_graph, last_graph_statements
            current_graph_statements.append(statement)
        yield current_graph, '\n'.join(current_graph_statements)

    def digest_graphs(self, _, line):
        if ".nq" not in line:
            return
        #sys.stderr.write("Downloading %s\n"%line)
        #local_file = download(line, "btc-2014", "archives/")
        #sys.stderr.write("Downloaded %s\n"%line)
        #f = gzip.GzipFile(fileobj=open(local_file, 'rb'))
        #allGraphs = ConjunctiveGraph(store='Sleepycat')
        #allGraphs.open("local_store", create=True)
        #for line in f:
        #    try:
        #        allGraphs.parse(data=line, format="nquads")
        #    except:
        #        sys.stderr.write( "BAD LINE: %s"% line)
        #    #    pass
        #sys.stderr.write("Parsed %s"%line)
        #yield 'benchmark', ','.join(stat_cols)
        #results = StringIO.StringIO()
        #resultsWriter = csv.writer(results)
        results = {}
        graphs = collections.defaultdict(str)
        i = 0
        #for g in allGraphs.contexts():
        #    i += 1
        for uri, lines in self.segment_graphs(_, line):
            #if uri is None:
            #    continue
            i += 1    
            graphs[uri] += '\n'+lines
            stats = collections.defaultdict(str)
            stats["id"] = uri
            try:
                g = ConjunctiveGraph()
                g.parse(data=graphs[uri], format="nquads")
                sys.stderr.flush()
                if len(g) == 0:
                    sys.stderr.write("%s (%d)" % (graphs[uri], len(g)))
                stats['lines'] = len(graphs[uri].split('\n'))
                ig = to_isomorphic(g)
                graph_digest = ig.graph_digest(stats)
                #stats['graph_digest'] = graph_digest
            except Exception as e:
                sys.stderr.write("ERROR: %s %s\n"%(stats['id'], e))
                stats['error'] = str(e)
            sys.stderr.write("Processed %s with %s triples in %s sec.\n"%(uri, len(g), stats['to_hash_runtime']))
            stats_line = [str(stats[c]) for c in stat_cols]
            results[uri] = stats_line
            #resultsWriter.writerow(stats_line)
        try:
            shutil.rmtree(store_dir)
        except:
            pass
        sys.stderr.write("Digested %s into %s graphs.\n"%(line, len(graphs)))
        results_string = StringIO.StringIO()
        results_writer = csv.writer(results_string)
        for result in results.values():
            results_writer.writerow(result)
        yield "benchmark", results_string.getvalue()
        
    def bundle_digest_graph(self, i, graphs):
        result = []
        for graph, nquads in graphs:
            for r, row in self.digest_graph(graph, '\n'.join(nquads)):
                result.append(row)
        yield "benchmark", '\n'.join(result)

    def digest_multigraph(self, _, nquads):
        try:
            nquads = '\n'.join(list(nquads))
            g = ConjunctiveGraph()
            g.parse(data=nquads, format="nquads")
        except:
            return

        result = None
        
        for graph in g.contexts():
            sys.stderr.write("Processing %s...\n"%graph.identifier)
            stats = collections.defaultdict(str)
            stats["id"] = graph.identifier
            try:
                ig = to_isomorphic(graph)
                graph_digest = ig.graph_digest(stats)
                #stats['graph_digest'] = graph_digest
            except Exception as e:
                sys.stderr.write("ERROR: %s %s\n"%(stats['id'], e))
                stats['error'] = str(e)
            #if stats['to_hash_runtime'] > 0.1:
            sys.stderr.write("Processed %s with %s triples in %s sec.\n"%(graph.identifier, len(graph), stats['to_hash_runtime']))
        
            stats_line = [str(stats[c]) for c in stat_cols]

            result_string = StringIO.StringIO()
            result_writer = csv.writer(result_string)
            result_writer.writerow(stats_line)
            if result is None:
                result = result_string.getvalue()
            else:
                result = result + "\n" + result_string.getvalue()
        yield "benchmark", result


    def digest_graph(self, uri, nquads):
        nquads = '\n'.join(list(nquads))
        sys.stderr.write("Processing %s (%d)...\n"%(uri, len(nquads)))
        stats = collections.defaultdict(str)
        stats["id"] = uri
        try:
            g = ConjunctiveGraph()
            g.parse(data=nquads, format="nquads")
            stats['ontology'] = g.value(predicate=RDF.type, object=OWL.Class) is not None
            sys.stderr.flush()
            stats['lines'] = len(nquads.split('\n'))
            ig = to_isomorphic(g)
            graph_digest = ig.graph_digest(stats)
            #stats['graph_digest'] = graph_digest
        except Exception as e:
            sys.stderr.write("ERROR: %s %s\n"%(stats['id'], e))
            stats['error'] = str(e)
        #if stats['to_hash_runtime'] > 0.1:
        sys.stderr.write("Processed %s with %s triples in %s sec.\n"%(uri, len(g), stats['to_hash_runtime']))
        
        stats_line = [str(stats[c]) for c in stat_cols]

        result_string = StringIO.StringIO()
        result_writer = csv.writer(result_string)
        result_writer.writerow(stats_line)
        yield "benchmark", result_string.getvalue()

    def merge_results(self, _, lines):
        yield 'result_table', '\n'.join(lines)


if __name__ == '__main__':
    BTCBenchmark.run()
