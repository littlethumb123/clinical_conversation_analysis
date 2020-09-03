import pandas as pd
import json
import os
import re
import numpy as np
import tqdm



def getMeta(string):
    '''
	Processing meta output; 
	arg: output string
	return cui and its perferred name
    
    '''
    import re 
    cui = ""
    pfname = ""
    cui_reg = r"(?<=cui=\')(.*)(?=\', semtypes)"
    pfname_reg = r"(?<=preferred_name=\')(.*)(?=\', cui)"
    cui_find = re.findall(cui_reg, string)
    pfname_find = re.findall(pfname_reg, string)
    
    if cui_find and pfname_find:
        cui = cui_find[0]
        pfname = pfname_find[0]
         
    return (cui, pfname)
	
	
def meta_clean(df):
	
	'''
	Preprocess meta excel files, remove nan columns and rows
	'''
	
    # Drop the columns with all missing values
    df.dropna(how = "all", axis = 1, inplace = True)

    # Drop the rows with all missing values
    df.dropna(subset = df.columns[1:], how = "all", axis = 0, inplace = True)

    # Split role:diag_seq
    new = df["Unnamed: 0"].str.split(":", n=1, expand = True)
    df["subj_no"] = new[0]
    df["diag_seq"] = new[1]

    # Drop "Unnamed: 0" column
    df.drop(columns = ["Unnamed: 0"], inplace = True)
    
    # Drop any utterance with no role information
    df.dropna(subset = ["subj_no"], axis = 0, inplace = True)

    return df

def meta_term_summary(df):
    '''
    	Get frequency of metamap concept of each participants
	
   	arg: cleaned dataframe
	return: 
	1. subj_seq_meta_dict: including sequence information for role split
	2. subj_meta_dict: frquency of each concept in a visit
    
    '''
    # map subject-seq-meta concept
    subj_seq_meta_dict = defaultdict(list)

    # map subject-meta concept
    subj_meta_dict = defaultdict(dict)

    for subj_no, data in tqdm(df.groupby("subj_no")):

        # record all meta concept of each subject
        all_meta = []
        seq_meta_dict = dict() # seq-meta labels dictionary
        for i, row in data.iterrows():
            seq = row["diag_seq"]
            
            # record meta concept of each position for each subject (to distinguish physician and patient)
            meta_list = []

            for col in range(row.count()-2):    
                # number of non-nan values in each row; do not count "subj_no" and "diag_seq"
                
                cui_term = getMeta(row[col])
                meta_list.append(cui_term[0])
                all_meta.append(cui_term[0])
            seq_meta_dict[seq] = dict(Counter(meta_list))

        subj_seq_meta_dict[subj_no] = seq_meta_dict
        subj_meta_dict[subj_no] = dict(Counter(all_meta)) 
    
    return subj_seq_meta_dict, subj_meta_dict	
	

# sort the records by frequency
def sort_nestdict(data):
    '''
	Sort by meta concept count for each subject

	arg: nested dictionary from json files
	return: dict, name and type frequency 

    
    '''
    res_name = {}
    res_type = {}
    if isinstance(data, dict):
        for subj, meta_v in data.items():
            temp_dict_name = {}
            temp_dict_type = {}
            for k, v in sorted(meta_v.items(), key= lambda item: item[1], reverse=True):
                if k:
                    # get name for each cui
                    temp_dict_name[df_cui_df.loc[k, "name"]] = v
                    
                    # get type for each cui, there will be repetitive type so add to existing keys
                    meta_type = df_cui_df.loc[k, "semantic type"]
                    if meta_type not in temp_dict_type:
                        temp_dict_type[meta_type] = v
                    else:
                        temp_dict_type[meta_type] += v
                    
            res_name[subj] = temp_dict_name
            res_type[subj] = temp_dict_type
    return res_name, res_type








# Main function 
c_path = "yourpath/meta_map_group/control"
td_path = "yourpath/meta_map_group/group_td"
ts_path = "yourpath/meta_map_group/group_ts"
cuis_path = "yourpath/all_cuis_dictionary.xlsx"

def main():

	# Import the global dictionary
	# columns: ['name', 'semantic type', 'definitions']
	df_cui_df = pd.read_excel(cuis_path) 


	# Import and clean excel file batch
	def import_file()

		for path in [c_path, ts_path, td_path]:

			files = [path + "\\" + f for f in os.listdir(path) if f[-4:] == 'xlsx']
			meta_df = pd.DataFrame()
			for f in c_files:
			    meta_df = c_meta_df.append(pd.read_excel(f, 'Sheet1'))
			
			meta_df_clean = meta_clean(meta_df)

			yield meta_df_clean

	c_meta_df_clean, ts_meta_df_clean, td_meta_df_clean = import_file()

	

	
if __name__ == "__main__":
    main()
	



