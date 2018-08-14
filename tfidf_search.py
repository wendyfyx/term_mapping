import numpy as np
import pandas as pd
import string
import math
import os
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
from whoosh.fields import Schema, TEXT, ID, STORED
from whoosh.analysis import StemmingAnalyzer
from whoosh.qparser import QueryParser
from whoosh import index
from whoosh import scoring
from whoosh import qparser
from whoosh.index import open_dir

source_filter_list = ["ICD9CM","ICD10CM"]
umlsPath = "/Users/Fyxstkala/Desktop/research/term_mapping"
outPath = "/Users/Fyxstkala/Desktop/research/term_mapping"
mrrank = umlsPath + "/MRRANK.RRF"
mrsty = umlsPath + "/MRSTY.RRF"
mrconso = umlsPath + "/MRCONSO.RRF"
indexDir = outPath + "/whoosh_index"

## TO DO
# 1. COULD STORE SYNONYMS, GIVE PREFERRED TERM MORE WEIGHT (score_boost)
# 2. PROCESS UNIQUE TERM, GET HIGHEST RANK TERM, PREFERRED TERM
# 3. COMPARE WITH ORIGINAL MAPPING, COMPUTE ACCURACY

class Search_Result():
    def __init__(self, cui, term, source, score):
        self.cui = cui
        self.term = term
        self.source = source
        self.score = score

    def print(self):
        print(self.cui, self.term, self.score)

# import all terms in source filter list
def index_umls(mrconso):

    # create schema
    schema = Schema(cui = STORED,
                    term = TEXT(stored=True),
                    source = STORED)

    # create index directory
    if not os.path.exists(indexDir):
        os.mkdir(indexDir)
    ix = index.create_in(indexDir, schema)

    # create writer
    writer = ix.writer()

    with open(mrconso) as f:
        for line in f:
            splitStr = line.strip().split("|")
            cui = splitStr[0]
            lang = splitStr[1]
            source = splitStr[11]
            term = splitStr[14]
            if lang == "ENG" and source in source_filter_list:
                writer.add_document(cui = cui, term = term, source = source)            
        writer.commit()


def whoosh_search(query_input, n = 5):
    ix = index.open_dir(indexDir)
    result_list = []

    with ix.searcher(weighting = scoring.TF_IDF()) as s:
        parser = QueryParser("term", schema = ix.schema,group=qparser.OrGroup)
        query = parser.parse(query_input)
        results = s.search(query, limit = n)
        for i in results:
            obj = Search_Result(i["cui"],i["term"],i["source"],i.score)
            result_list.append(obj)
    return result_list

def whoosh_batch_search(query_input_list):
    result_list = []
    for q in query_input_list:
        results = whoosh_search(q)
        result_list.append((q, results))
    return result_list
        

def whoosh_synonym_search(query_input, n = 5):
    return

def whoosh_norm_search(query_input, n = 5):
    return

def whoosh_norm_synonym_search(query_input, n = 5):
    return

def get_synonyms(query):
    return

def main():
    #index_umls(mrconso)
    query = "Auditory nerve disorder"
    whoosh_search(query)
    #search(document, ["Auditory nerve disorder"])


    #ix = open_dir('whoosh_index')
    

if __name__ == "__main__":
    main()