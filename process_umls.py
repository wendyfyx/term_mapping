from collections import Counter
import numpy as np
import pandas as pd
from collections import defaultdict

umlsPath = "/Users/Fyxstkala/Desktop/GitHub/term_mapping/umls"
mrrank = umlsPath + "/MRRANK.RRF"
mrsty = umlsPath + "/MRSTY.RRF"
mrconso = umlsPath + "/MRCONSO.RRF"


# retrived pandas dataframe from dataset
def get_from_source(filename, source):
    df = pd.read_csv(filename, sep="|", header=None, usecols=[0,1,11,12,14])
    df.columns = ["CUI","LAT","SAB","TTY","STR"]
    df = df.loc[df["LAT"] == "ENG"]
    df = df.loc[df["SAB"] == source]
    return df

# retrieve meddra terms by term type
def get_meddra(df, term_type):
    return df.loc[df["TTY"] == term_type]

# mapping between MedDRA and ICD
def mapping(mdr, icd):
    matches = pd.merge(mdr, icd, how='inner', on=["CUI"])
    len_match = len(set(matches["STR_x"].tolist()))
    len_mdr = len(set(mdr["STR"].tolist()))
    print("MDR: ", len_mdr)
    print("Matches: ", len_match)
    print("Percentage of MedDRA: ", float(len_match)/len_mdr)
    print("---")
    matches = matches.rename(columns={'STR_x': 'MDR', 'STR_y': 'ICD'})
    return matches[["CUI","MDR","ICD"]]

# combined mapping from ICD9 and ICD10
def combined_mapping(icd9, icd10, mdr, term_type):
    mdr = get_meddra(mdr, term_type)
    mapping_icd9 = mapping(mdr, icd9)
    mapping_icd10 = mapping(mdr, icd10)
    combined = pd.concat([mapping_icd9, mapping_icd10]).drop_duplicates()
    mapping_dict = defaultdict(list)
    for index,row in combined.iterrows():
        mapping_dict[row["MDR"]].append(row["CUI"])
    #print("Combined:", combined.shape[0])
    return mapping_dict


def main():

    pd.set_option('display.max_colwidth', -1)
    '''
    # ----- UMLS data -----
    
    icd9 = get_from_source(mrconso,"ICD9CM")
    icd10 = get_from_source(mrconso,"ICD10CM")
    mdr = get_from_source(mrconso, "MDR")

    pt_combined = combined_mapping(icd9, icd10, mdr, "PT")
    llt_combined = combined_mapping(icd9, icd10, mdr, "LLT")

    '''

if __name__ == "__main__":
    main()