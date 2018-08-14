from numpy import genfromtxt
from collections import Counter
import numpy as np
import pandas as pd

# retrieve data from UMLS
def retrieve_umls_vocab_by_name(filename, vocab):
    with open(filename) as f:
        concepts = np.asarray([line.strip().split('|') for line in f 
                            if (vocab in line)])
    return concepts

# retrived pandas dataframe from dataset by term type, only extract CUI and STR
def get_dataframe(data, term_type):
    df = pd.DataFrame([[entry[0],entry[14]] for entry in data 
                        if (entry[1] == "ENG" and entry[12] == term_type)],
                        columns = ["CUI", "STR"])
    return df

def get_icd(icd):
    df = pd.DataFrame([[entry[0],entry[14]] for entry in icd 
                        if (entry[1] == "ENG")],
                        columns = ["CUI", "ICD"])
    return df

# find corresponding pt term
def get_pt(filename, term):
    with open(filename) as f:
        pt_dict = {line.strip().split(",")[1]:line.strip().split(",")[0] for line in f}
        if term in pt_dict.keys():
            return pt_dict[term]
        else:
            return term


# given term and extracted MDR data frame, find CUI in mdr data
def get_cui(mdr, term):
    return mdr.loc[mdr["STR"] == term, "CUI"].values[0]

# print CUI and terms matches
def mapping(mdr, icd):
    matches = pd.merge(mdr, icd, how='inner', on=["CUI"])
    len_match = len(set(matches["STR"].tolist()))
    #print("MDR: ", len(set(mdr["STR"].tolist())))
    #print("ICD: ", len(set(icd["STR"].tolist())))
    #print("Matches: ", len_match)
    #print("Percentage of MedDRA: ", float(len_match)/len(set(mdr["STR"].tolist())))
    return matches

def not_mapped(mdr, icd, id):
    matches = pd.merge(mdr, icd, how='inner', on=[id])
    matched_id = matches["CUI"]
    mdr_cui = mdr["CUI"]
    #icd_cui = icd["CUI"]

    exclude_mdr = pd.concat([mdr_cui,matched_id]).drop_duplicates(keep=False)
    #exclude_icd = pd.concat([icd_cui,matched_id]).drop_duplicates(keep=False)

    exclude_mdr = pd.merge(mdr, pd.DataFrame(exclude_mdr), how="inner",on = [id])
    #exclude_icd = pd.merge(icd, pd.DataFrame(exclude_icd), how="inner",on = [id])

    return exclude_mdr


def main():

    pd.set_option('display.max_colwidth', -1)

    # ----- UMLS data -----
    
    icd10cm_umls = retrieve_umls_vocab_by_name("MRCONSO.RRF", "ICD10CM")
    icd9cm_umls = retrieve_umls_vocab_by_name("MRCONSO.RRF", "ICD9CM")
    meddra_umls = retrieve_umls_vocab_by_name("MRCONSO.RRF","MDR")

    mdr_pt = get_dataframe(meddra_umls, "PT")
    mdr_llt = get_dataframe(meddra_umls, "LLT")
    icd9 = get_icd(icd9cm_umls)
    icd10 = get_icd(icd10cm_umls)

    print(mdr_pt.shape)
    print(mdr_llt.shape)
    print(icd9.shape)
    print(icd10.shape)
    '''
    match9_pt = mapping(mdr_pt,icd9)["STR"].drop_duplicates(keep="first")
    match10_pt = mapping(mdr_pt,icd10)["STR"].drop_duplicates(keep="first")
    match9_llt = mapping(mdr_llt, icd9)["STR"].drop_duplicates(keep="first")
    match10_llt = mapping(mdr_llt, icd10)["STR"].drop_duplicates(keep="first")
    print(match9_pt.shape)
    print(match10_pt.shape)
    print(match9_llt.shape)
    print(match10_llt.shape)
    merged_mdr = pd.concat([match9_pt,match10_pt,match9_llt,match10_llt])
    merged_mdr = merged_mdr.drop_duplicates(keep = "first")
    print(merged_mdr.shape)

    print("overall...")
    '''
    '''
    print("MDR - ICD9 unique STR: ", len(set(match9["STR_x"])))
    print("MDR - ICD10 unique STR: ", len(set(match10["STR_x"])))
    print("MDR - ICD9 unique CUI: ", len(set(match9["CUI"])))
    print("MDR - ICD10 unique CUI: ", len(set(match10["CUI"])))
    
    #print("Merged unique CUI: ", len(set(merged_mdr["CUI"])))
    print("Merged unique STR: ", len(set(merged_mdr)))
    merged_mdr.to_csv("umls_combined.txt",index=False,sep = "\t")
    '''
    
    mdr = get_df(meddra_umls)
    icd9 = get_df(icd9cm_umls)
    icd10 = get_df(icd10cm_umls)
    mapping(mdr, icd9, "CUI","STR")
    mapping(mdr, icd10, "CUI","STR")

    
    exclude_mdr = not_mapped(mdr_select, icd_select, "CUI")
    pd.set_option('display.max_colwidth', -1)
    print("MDR: ", exclude_mdr.shape[0])
    print(exclude_mdr.to_string(index=False))
    

if __name__ == "__main__":
    main()