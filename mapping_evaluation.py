import numpy as np
import pandas as pd
import os
import re, math
from collections import Counter
from collections import defaultdict
import statistics

# https://stackoverflow.com/questions/15173225/calculate-cosine-similarity-given-2-sentence-strings
WORD = re.compile(r'\w+')

def retrieve_meddra(filename, level):
    with open(filename) as file:
        concepts = np.asarray([line.strip().split('\t') for line in file])
    
    # fill invalid_reason column if empty
    for row in concepts:
        if len(row) == 9 and len(row[8])==8:
            row.append("NA")

    concepts = np.asarray(concepts)
    ret = np.asarray([row for row in concepts if row[3] == "MedDRA" and row[4] == level])
    df = pd.DataFrame([[entry[0],entry[1],entry[3]] for entry in ret], 
        columns = ["id", "concept",'vocab'])

    return df

def get_cosine(vec1, vec2):
     intersection = set(vec1.keys()) & set(vec2.keys())
     numerator = sum([vec1[x] * vec2[x] for x in intersection])

     sum1 = sum([vec1[x]**2 for x in vec1.keys()])
     sum2 = sum([vec2[x]**2 for x in vec2.keys()])
     denominator = math.sqrt(sum1) * math.sqrt(sum2)

     if not denominator:
        return 0.0
     else:
        return "{0:.4f}".format(float(numerator) / denominator)

def text_to_vector(text):
     words = WORD.findall(text)
     return Counter(words)

# similarity between two terms
def term_similarity(text1, text2):
    vector1 = text_to_vector(text1)
    vector2 = text_to_vector(text2)
    return get_cosine(vector1, vector2)

def retrieve_umls_type(filename):
    with open(filename) as f:
        concepts = pd.DataFrame([line.strip().split('|') for line in f],
                                columns = ["CUI","TUI","STN","STY","ATUI","CVF","NULL"])
        return concepts

def get_semantic_files(filename,concepts):
    with open(filename) as f:
        for _ in range(5):
            next(f)
        for line in f:
            ls = re.split(r'\s{2,}', line.strip())
            print(concepts.loc[concepts["CUI"] == ls[0], "STY"])

def get_umls_file(filename):
    with open(filename) as umls_file:
        for j in range(5):
            next(umls_file)
        umls = pd.DataFrame([re.split(r'\s{2,}', line.strip()) for line in umls_file], 
                                columns = ["cui","mdr","icd"])
    return umls

def get_ohdsi_file(filename):
    with open(filename) as ohdsi_file:
        for i in range(6):
            next(ohdsi_file)
        ohdsi = pd.DataFrame([[re.split(r'\s{2,}', line.strip())[3],
                            re.split(r'\s{2,}', line.strip())[1], 
                            re.split(r'\s{2,}', line.strip())[4]] for line in ohdsi_file],
                                columns=["icd_id","mdr","icd"])
    return ohdsi

def initialize_dict(u1, u2):
    umls1 = get_umls_file(u1)
    umls2 = get_umls_file(u2)
    #ohdsi1 = get_ohdsi_file(o1)
    #ohdsi2 = get_ohdsi_file(o2)

    umls = pd.concat([umls1,umls2]).drop_duplicates()
    #ohdsi = pd.concat([ohdsi1,ohdsi2]).drop_duplicates()

    map_dict = defaultdict(list)
    for index, row in umls.iterrows():
        map_dict[row["mdr"]].append((row["icd"],row["cui"]))
    #for index, row in ohdsi.iterrows():
    #    map_dict[row["mdr"]].append((row["icd"],row["icd_id"]))
    
    return map_dict

# get all mapped terms
def get_all(file1, file2):
    '''
    with open(file1) as umls_file:
        for j in range(5):
            next(umls_file)
        umls_mdr = pd.DataFrame([re.split(r'\s{2,}', line.strip())[:2] for line in umls_file], 
                                columns = ["id","mdr"])
    '''

    with open(file1) as ohdsi_file1:
        for i in range(6):
            next(ohdsi_file1)
        ohdsi_mdr1 = pd.DataFrame([[re.split(r'\s{2,}', line.strip())[0],re.split(r'\s{2,}', line.strip())[1]] for line in ohdsi_file1],
                                columns=["id","mdr"])

    with open(file2) as ohdsi_file:
        for i in range(6):
            next(ohdsi_file)
        ohdsi_mdr = pd.DataFrame([[re.split(r'\s{2,}', line.strip())[0],re.split(r'\s{2,}', line.strip())[1]] for line in ohdsi_file],
                                columns=["id","mdr"])
    
    # mapped UMLS and OHDSI together based on string match
    all_terms = pd.concat([ohdsi_mdr1,ohdsi_mdr])
    all_terms_dropped = all_terms.drop_duplicates(subset = ["mdr"] , keep = "first")
    #overlap = pd.merge(ohdsi_mdr1,ohdsi_mdr, how = "inner", on = ["mdr"]).drop_duplicates()

    '''
    len_overlap = len(set(overlap["mdr"].tolist()))
    len_all = len(set(all_terms_dropped["id"].tolist())) # dropping duplicates
    
    print("UMLS MDR: ", len(set(umls_mdr["id"].tolist())))
    print("OHDSI MDR: ", len(set(ohdsi_mdr["id"].tolist())))
    print("term count: ", len_all)
    print("overlap count: ", len_overlap)

    print(overlap.to_string(index=False))
    '''

    return all_terms_dropped["mdr"]

# list most words and count with most common on top
def most_common_word(word_df):
    print(len(set(word_df.tolist())))
    string = " ".join(word_df.tolist())
    words = re.findall(r'\w+', string)
    cap_words = [word.upper() for word in words]
    word_counts = Counter(cap_words)
    for key, value in word_counts.most_common():
        print(key, value)

# evaluate encoder vs. actual mapping
def evaluation_queries(map_f, query_f):
    count = 0

    with open(map_f) as f1:
        map_result = pd.DataFrame([line.strip().split("$",1) for line in f1],columns = ["term","mapped"])
    with open(query_f) as f2:
        query_result = pd.DataFrame([line.strip().split("$",1) for line in f2],columns = ["term","mapped"])
    result_ls = []
    merged = pd.merge(map_result,query_result, how = "inner", on="term")

    for index, row in merged.iterrows():
        l1 = row["mapped_x"]
        l2 = row["mapped_y"]
        ls = list(set(l1).intersection(l2)) # intersecting CUIs
        if len(ls)>0:
            count += 1
        else: # false query terms
            result_ls.append(row["term"])
    print("Mapped: ", count)
    print("Not mapped: ", len(result_ls))
    print("Mapped evaluation: ", float(count/len(map_result)))

    for i in result_ls:
        print(i)

def main():

    pd.set_option('display.max_colwidth', -1)
    '''
    orig_file = "/Users/Fyxstkala/Desktop/research/term_mapping/ohdsi_false_queries_orig.txt"
    query_file = "/Applications/apache-tomcat-9.0.6/webapps/mapping_encoder/ohdsi_false_queries.txt"

    with open(orig_file) as f1:
        orig_df = pd.DataFrame([line.strip().split("\t") for line in f1],
                                columns = ["mdr","icd_mapped"])
    with open(query_file) as f2:
        query_df = pd.DataFrame([line.strip().split("\t") for line in f2],
                                columns = ["mdr","icd_query"])
    combined_df = pd.merge(orig_df, query_df, how = "inner", on = ["mdr"])
    combined_df.to_csv("ohdsi_false_queries_merged.txt",index=False,sep = "[")
    
    '''
    '''
    # ----Get original Mapping----
    
    map_dict = initialize_dict("result/official_mapping/icd9_mdr_pt_umls.txt",
                                "result/official_mapping/icd10_mdr_pt_umls.txt")
    
    with open("/Users/Fyxstkala/Desktop/research/term_mapping/result/official_mapping/umls_merged_pt.txt") as f:
        for line in f:
            #term = line.strip().split("\t")[1]
            #soc = line.strip().split("\t")[0]
            term = line.strip()
            result = map_dict.get(term)
            if result:
                ids = list(set([i[1] for i in result]))
                print("%s$%s" % (term, "$".join(ids)))
                    #print(line.strip() + "\t"+ i[1] + "\t" + i[0])
            else:
                print(line.strip()+ "$None")
    '''
    evaluation_queries("/Users/Fyxstkala/Desktop/research/term_mapping/orig_test.txt",
                "/Applications/apache-tomcat-9.0.6/webapps/mapping_encoder/top5_queries_test.txt")
    



if __name__ == "__main__":
    main()