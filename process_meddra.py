import pandas as pd
import random
from collections import Counter
from collections import defaultdict
from process_ohdsi import ohdsi_mapping
from process_umls import umls_mapping

meddra_file_path = "MedDRA/meddra_21_0_english/MedAscii"
mapping_file_path = "offic_mapping_results"
pt_file = meddra_file_path + "/pt.asc"
soc_file = meddra_file_path + "/soc.asc"
llt_file = meddra_file_path + "/llt.asc"
umls_combined_file = mapping_file_path + "/umls_combined.txt"
ohdsi_combined_file = mapping_file_path + "/ohdsi_combined.txt"

def get_random(df, soc_code, n):
    pt_list = df.loc[df['soc_code'] == soc_code]["pt_name"]
    number = min(n,len(pt_list))
    random_ls = random.sample(set(pt_list),number)
    print(number)
    return random_ls

# find corresponding pt term
def initialize_pt_llt_dict(pt_df, llt_df):
    ret = pd.merge(pt_df,llt_df, how = "inner", on = "pt_code")
    ret = ret[["pt_name","llt_name"]]
    return ret

def initialize_pt_soc_dict(pt_df, soc_df):
    ret = pd.merge(pt_df,soc_df, how = "inner", on = "soc_code")
    ret = ret[["pt_name","soc_name"]]
    return ret

def convert_pt_from_llt(llt_df, pt_df, llt_pt_df):
    # convert mapped llt to pt
    pt_from_llt = pd.merge(llt_df, llt_pt_df, how = "inner", on = ["llt_name"])
    # concatenate with current pt
    combined = pd.concat([pt_from_llt[["pt_name"]],pt_df[["pt_name"]]]).drop_duplicates(keep = "first")
    print("combined pt: ",combined.shape)
    combined.to_csv("ALLCOMBINED.csv", index = False, sep = "\t")
    return combined

# count soc from a list of 
def soc_count(word_df, pt_soc_df):
    soc_from_pt = pd.merge(word_df,pt_soc_df, how = "inner", on = ['pt_name'])
    print(soc_from_pt.shape)
    # get count df
    count = Counter(soc_from_pt["soc_name"].tolist()).most_common()
    count = pd.DataFrame(count, columns = ["soc_name","count"])

    count.to_csv("soc_count.txt",sep = "\t")

    # get soc name
    #count = pd.merge(count,soc_df, how = "inner", on = ["soc_code"])

def main():
    
    pd.set_option('display.max_colwidth', -1)
    
    with open(pt_file) as f:
        pt_df = pd.DataFrame([line.strip().split("$")[:4] for line in f],
                            columns = ["pt_code","pt_name","null","soc_code"])
        #ptlist = [line.strip().split("$")[1] for line in f]
    with open(llt_file) as f:
        llt_df = pd.DataFrame([line.strip().split("$")[:3] for line in f],
                            columns = ["llt_code","llt_name","pt_code"])
        #lltlist = [line.strip().split("$")[1] for line in f]
    with open(soc_file) as f:
        soc_df = pd.DataFrame([line.strip().split("$")[:2] for line in f], 
                            columns = ["soc_code","soc_name"])

    llt_pt_df = initialize_pt_llt_dict(pt_df, llt_df)
    pt_soc_df = initialize_pt_soc_dict(pt_df, soc_df)

    mapped_ohdsi = ohdsi_mapping()
    mapped_pt_umls, mapped_llt_umls = umls_mapping()
    mapped_all = pd.concat([mapped_ohdsi,mapped_pt_umls,mapped_llt_umls]).drop_duplicates()

    llt_filtered = pd.merge(mapped_all,llt_df, left_on="MDR", right_on="llt_name",how = "inner")[["MDR","ICD"]]
    print(llt_filtered.shape)
    pt_filtered = pd.merge(mapped_all, pt_df, left_on="MDR", right_on="pt_name", how = "inner")[["MDR","ICD"]]
    print(pt_filtered.shape)

    llt_to_pt_filtered = pd.merge(llt_filtered, llt_pt_df, left_on="MDR", right_on="llt_name")[["pt_name","ICD"]]
    llt_to_pt_filtered.columns = ["MDR","ICD"]
    combined_all = pd.concat([pt_filtered,llt_to_pt_filtered]).drop_duplicates()
    print(len(set(combined_all["MDR"])))
    print(combined_all[:3])
    
    mapping_dict = defaultdict(list)
    for index,row in combined_all.iterrows():
        mapping_dict[row["MDR"]].append(row["ICD"])

    with open("combined_mapping_dict.txt","a") as writer:
        for k, v in mapping_dict.items():
            vals = "||".join(v)
            writer.write("%s%s\n" % (k, vals))


if __name__ == "__main__":
    main()