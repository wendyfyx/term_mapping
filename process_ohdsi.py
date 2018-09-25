from numpy import genfromtxt
import numpy as np
import pandas as pd
from collections import Counter

# retrieve data downlaoded from OHDSI vocabularies (concept file)
def retrieve_ohdsi_vocab(filename, vocab, id_name):
    with open(filename) as file:
        concepts = np.asarray([line.strip().split('\t') for line in file])
    
    # fill invalid_reason column if empty
    for row in concepts:
        if len(row) == 9 and len(row[8])==8:
            row.append("NA")

    concepts = np.asarray(concepts)
    ret = np.asarray([row for row in concepts if row[3] == vocab])
    df = pd.DataFrame([[entry[0],entry[1],entry[3]] for entry in ret], 
        columns = [id_name, "name",'vocab'])

    return df


def retrieve_meddra(filename, id_name, level):
    with open(filename) as file:
        concepts = np.asarray([line.strip().split('\t') for line in file])
    
    # fill invalid_reason column if empty
    for row in concepts:
        if len(row) == 9 and len(row[8])==8:
            row.append("NA")

    concepts = np.asarray(concepts)
    ret = np.asarray([row for row in concepts if row[3] == "MedDRA" and row[4] == level])
    df = pd.DataFrame([[entry[0],entry[1],entry[3]] for entry in ret], 
        columns = [id_name, "name",'vocab'])

    return df

# get all maps-to relationships
def get_concept_relationships(file, relation):
    with open(file) as f:
        next(f)
        f = np.asarray([line.strip().split("\t") for line in f])
        df = pd.DataFrame([entry[:3] for entry in f if (entry[2] == relation)], 
            columns = ['id1','id2',"relation"])
        #df = pd.DataFrame([entry[:3] for entry in f], columns = ['id1','id2',"relation"])
    return df

def mapping_with_snomed(vocab, name, snomed, relation):
    df_relation = get_concept_relationships("ohdsi/CONCEPT_RELATIONSHIP.csv", relation)
    id_match = pd.merge(vocab, df_relation, how ='inner', on=['id1'])
    df = pd.merge(id_match, snomed, how ='inner', on=["id2"])
    
    df = df[["id1","name_x","id2"]]
    df = df.rename(index=str, columns={"id1": name+"_id", "name_x": name, "id2": "snomed_id"})
    return df

def ohdsi_mapping():
    
    icd9 = retrieve_ohdsi_vocab("ohdsi/CONCEPT.csv", "ICD9CM", "id1")
    icd10 = retrieve_ohdsi_vocab("ohdsi/CONCEPT.csv", "ICD10CM", "id1")
    mdr_llt = retrieve_meddra("ohdsi/CONCEPT.csv","id1","LLT")
    mdr_pt = retrieve_meddra("ohdsi/CONCEPT.csv","id1","PT")
    snomed = retrieve_ohdsi_vocab("ohdsi/CONCEPT.csv", "SNOMED","id2")

    '''
    df_relation = get_concept_relationships("ohdsi/CONCEPT_RELATIONSHIP.csv", "MedDRA - ICD9CM")
    mdr_relation = pd.merge(mdr, df_relation, how="inner", on=["id1"])
    mdr_icd = pd.merge(mdr_relation, icd9, how="inner", on=["id2"])
    '''

    mdr_pt_snomed = mapping_with_snomed(mdr_pt, "mdr", snomed, "MedDRA - SNOMED eq")
    mdr_llt_snomed = mapping_with_snomed(mdr_llt, "mdr", snomed, "MedDRA - SNOMED eq")
    icd9_snomed = mapping_with_snomed(icd9, "icd", snomed, "Maps to")
    icd10_snomed = mapping_with_snomed(icd10, "icd", snomed, "Maps to")

    mdr_icd9_pt = pd.merge(mdr_pt_snomed, icd9_snomed, how="inner", on=["snomed_id"])
    mdr_icd9_llt = pd.merge(mdr_llt_snomed, icd9_snomed, how="inner", on=["snomed_id"])
    mdr_icd10_pt = pd.merge(mdr_pt_snomed, icd10_snomed, how="inner", on=["snomed_id"])
    mdr_icd10_llt = pd.merge(mdr_llt_snomed, icd10_snomed, how="inner", on=["snomed_id"])

    merged = pd.concat([mdr_icd9_pt[["mdr","icd"]],mdr_icd9_llt[["mdr","icd"]],
                            mdr_icd10_pt[["mdr","icd"]],mdr_icd10_llt[["mdr","icd"]]])
    merged = merged.drop_duplicates(keep = "first")
    merged.columns = ["MDR","ICD"]
    print("Merged: ", len(set(merged["MDR"])))

    return merged

    #merged_mdr.to_csv("ohdsi_combined.txt",index=False,sep = "\t")

def main():
    return 

if __name__ == "__main__":
    main()