import numpy as np
import pandas as pd
import string
import math
import os
import time
import progressbar as pb
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
from whoosh.fields import Schema, TEXT, ID, STORED, NUMERIC
from whoosh.analysis import StemmingAnalyzer
from whoosh.qparser import QueryParser
from whoosh import index
from whoosh import scoring
from whoosh import qparser
from whoosh.index import open_dir
import process_umls

source_filter_list = ["ICD9CM","ICD10CM"]
rootPath = "/Users/Fyxstkala/Desktop/GitHub/term_mapping"
mrrank = rootPath + "/umls/MRRANK.RRF"
mrsty = rootPath + "/umls/MRSTY.RRF"
mrconso = rootPath + "/umls/MRCONSO.RRF"
indexDir = rootPath + "/whoosh_index"

## TO DO
# 1. COULD STORE SYNONYMS, GIVE PREFERRED TERM MORE WEIGHT (score_boost)
# 2. PROCESS UNIQUE TERM, GET HIGHEST RANK TERM, PREFERRED TERM
# 3. SORT RESULTS 

class Search_Result():
    def __init__(self, cui, term, source, score):
        self.cui = cui
        self.term = term
        self.source = source
        self.score = score

    def write(self):
        print(self.cui, self.term, self.score)

# import all terms in source filter list
def index_umls(mrconso):

    # create schema
    schema = Schema(cui = STORED,
                    term = TEXT(stored=True),
                    source = STORED,
                    len_word = NUMERIC(sortable=True)) # used to sort search result

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
            len_word = len(term.split(" "))
            if lang == "ENG" and source in source_filter_list:
                writer.add_document(cui = cui, term = term, source = source, 
                                    len_word = len_word)            
        writer.commit()

# search single query
def whoosh_search(query_input, n = 5):
    ix = index.open_dir(indexDir)
    result_list = []

    with ix.searcher(weighting = scoring.TF_IDF()) as s:
        parser = QueryParser("term", schema = ix.schema,group=qparser.OrGroup)
        query = parser.parse(query_input)
        results = s.search(query, limit = None)
        for i in results[:n]:
            obj = Search_Result(i["cui"],i["term"], i["source"],i.score)
            result_list.append(obj)
    return result_list

# search batch query
def whoosh_batch_search(query_input_list):
    batch_results = {}
    widgets = [pb.Percentage(), pb.Bar()]
    progress = pb.ProgressBar(widgets = widgets, max_value=len(query_input_list)).start()
    var = 0
    for q in query_input_list:
        batch_results[q] = whoosh_search(q)
        progress.update(var + 1)
        var += 1
    return batch_results

# get synonyms or lexial variants from UMLS Lexical Tools
def get_synonyms(query):
    return

# normalize term using UMLS Lexical Tools
def get_norm(query):
    return
        
def whoosh_synonym_search(query_input, n = 5):
    return

def whoosh_norm_search(query_input, n = 5):
    return

def whoosh_norm_synonym_search(query_input, n = 5):
    return

def write_to_file(batch_results):
    f = open("whoosh_result.txt", "w", encoding='utf-8')
    for query, results in batch_results.items():
        f.write(query+"|")
        res = [i.term for i in results]
        f.write("|".join(res))
        f.write("\n")
    f.close()

def evaluate_results(umls_mapping, encoder_mapping):
    # batch result list --> data frame
    df1 = pd.DataFrame(columns=["term", "umls_map"])
    df2 = pd.DataFrame(columns=["term", "encoder_map"])
    i = 0
    for key, value in umls_mapping.items():
        df1.loc[i] = [key, value]
        i+=1
    i = 0
    for key, value in encoder_mapping.items():
        df2.loc[i] = [key, [i.cui for i in value]]
        i+=1

    merged = pd.merge(df1, df2, how = "inner", on = ["term"])
    incorrect = []
    count = 0
    for index, row in merged.iterrows():
        l1 = [i for i in row["umls_map"]]
        l2 = [i for i in row["encoder_map"]]
        common = list(set(l1).intersection(l2))
        if len(common)>0:
            count += 1
        else: # false query terms
            incorrect.append(row["term"])
    print("MAPPED: ", count)
    print("TOTAL: ", merged.shape[0])
    print("PERCENTAGE CORRECT: ", float(count)/merged.shape[0])
    return incorrect

def main():
    #index_umls(mrconso)
    
    icd9 = process_umls.get_from_source(mrconso,"ICD9CM")
    icd10 = process_umls.get_from_source(mrconso,"ICD10CM")
    mdr = process_umls.get_from_source(mrconso, "MDR")
    combined = process_umls.combined_mapping(icd9, icd10, mdr, "PT")
    pt_list = combined.keys()
    print(len(pt_list))
    
    start = time.time()
    results = whoosh_batch_search(pt_list)
    end = time.time()
    print(end-start)
    write_to_file(results)

    incorrect = evaluate_results(combined, results)
        

if __name__ == "__main__":
    main()