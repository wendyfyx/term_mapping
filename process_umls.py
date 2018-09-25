import pandas as pd
from collections import defaultdict
from Authentication import *
import requests
import json
import time
import progressbar as pb

umlsPath = "/Users/Fyxstkala/Desktop/GitHub/term_mapping/umls"
mrrank = umlsPath + "/MRRANK.RRF"
mrsty = umlsPath + "/MRSTY.RRF"
mrconso = umlsPath + "/MRCONSO.RRF"
base_url = "https://uts-ws.nlm.nih.gov/rest"

def retrieve_from_api_by_source(endpoint,apikey):

    AuthClient = Authentication(apikey)
    tgt = AuthClient.gettgt()
    query = {'ticket':AuthClient.getst(tgt), "language":"ENG",
            "pageNumber":1, "pageSize":1000}
    r = requests.get(endpoint, params = query)
    r.encoding = "utf-8"
    try:
        items  = json.loads(r.text)
        return items
    except ValueError:
        print("Decode failed...")

def retrive_from_api(version, source, apikey):

    endpoint = "/content/"+version+"/source/"+source
    source_data = retrieve_from_api_by_source(base_url+endpoint,apikey)
    term_dict = defaultdict(list)

    # PROGRESS BAR
    widgets = [pb.Percentage(), pb.Bar()]
    progress = pb.ProgressBar(widgets = widgets, 
                max_value=len(source_data["result"])).start()
    var = 0
    # PROGRESS BAR

    for item in source_data["result"]:
        identifier = item["ui"]
        descend_endpoint = endpoint + "/" + identifier + "/descendants"
        descend_data = retrieve_from_api_by_source(base_url+descend_endpoint,apikey)
        
        # PROGRESS BAR
        progress.update(var + 1)
        var += 1
        # PROGRESS BAR
        
        for i in descend_data["result"]:
            concept_data = retrieve_from_api_by_source(i["concepts"],apikey)
            for k in concept_data["result"]["results"]:
                if k["rootSource"] == "MDR":
                    term_dict[k["name"]].append(k["ui"])
    print("term dict created...")
    print(len(term_dict))
    return term_dict


# retrived pandas dataframe from MRCONSO
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
    combined = pd.concat([mapping_icd9, mapping_icd10]).drop_duplicates(keep = "first")
    return combined
    '''
    mapping_dict = defaultdict(list)
    for index,row in combined.iterrows():
        mapping_dict[row["MDR"]].append(row["CUI"])
    #print("Combined:", combined.shape[0])
    return pd.DataFrame(mapping_dict, columns = ["MDR", "CUI"])
    '''

def umls_mapping():
    
    icd9 = get_from_source(mrconso,"ICD9CM")
    icd10 = get_from_source(mrconso,"ICD10CM")
    mdr = get_from_source(mrconso, "MDR")

    pt_combined = combined_mapping(icd9, icd10, mdr, "PT")
    llt_combined = combined_mapping(icd9, icd10, mdr, "LLT")
    return pt_combined[["MDR", "ICD"]], llt_combined[["MDR", "ICD"]]


def main():

    pd.set_option('display.max_colwidth', -1)

if __name__ == "__main__":
    main()