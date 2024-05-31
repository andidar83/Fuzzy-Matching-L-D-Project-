# -*- coding: utf-8 -*-
"""
Created on Tue May 14 16:25:42 2024

@author: NXP
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Apr 24 11:32:49 2024

@author: NXP
"""
import gspread as gs
import gspread_dataframe as gd
import pandas as pd
from rapidfuzz import process, utils
import requests
import time
import json




##############Functions#######################################################################
credentials = {

}

gc= gs.service_account_from_dict(credentials)

def export_to_sheets(file_name, sheet_name):
    ws = gc.open(file_name).worksheet(sheet_name)
    data = ws.get_all_values()
    headers = data.pop(0)
    return pd.DataFrame(data, columns=headers)

def poll_job(s, redash_url, job):
    # TODO: add timeout
    while job['status'] not in (3,4):
        response = s.get('{}/api/jobs/{}'.format(redash_url, job['id']))
        job = response.json()['job']
        time.sleep(1)

    if job['status'] == 3:
        return job['query_result_id']
    
    return None


def get_fresh_query_result(redash_url, query_id, api_key, params):
    s = requests.Session()
    s.headers.update({'Authorization': 'Key {}'.format(api_key)})

    payload = dict(max_age=0, parameters=params)

    response = s.post('{}/api/queries/{}/results'.format(redash_url, query_id), data=json.dumps(payload))

    if response.status_code != 200:
        return 'Refresh failed'
        raise Exception('Refresh failed.')

    result_id = poll_job(s, redash_url, response.json()['job'])

    if result_id:
        response = s.get('{}/api/queries/{}/results/{}.json'.format(redash_url, query_id, result_id))
        if response.status_code != 200:
            raise Exception('Failed getting results.')
    else:
        raise Exception('Query execution failed.')

    return response.json()['query_result']['data']['rows']

#########################Pull Emplyee Database###########################################3
print("pulling employee database...")
employee_db = []
while True:
    try:
        params2 = {}
        api_key2 = 'Poqwen8BjW4zjLHOzpmSlXDnm7TSrV5DzevDv6eD'
        result2 = get_fresh_query_result('https://redash-id.ninjavan.co/',2268, api_key2, params2)
        df2 = pd.DataFrame(result2)
        df2["Nama_Pegawai"] = df2["Nama_Pegawai"].str.strip().str.lower()
        employee_db.append(df2)
        break
    except:
        print('>> Pulling failed, retrying...')

employee_db = employee_db[0]
employee_db['name_reg'] = employee_db['Nama_Pegawai']+employee_db['Region']
employee_db['name_reg'] = employee_db['name_reg'].str.strip().str.lower()
employee_db['NIK'] = employee_db['NIK'].astype('str').str.lower().str.strip()
employee_db['SYSTEM_ID'] = employee_db['SYSTEM_ID'].astype('str').str.lower().str.strip()
employee_db['Old_NIK'] =  employee_db['Old_NIK'].astype('str').str.lower().str.strip()
print("EMPLOYEE_dATA")
print(employee_db.info())


###########################################for fuzzy nik purposes bcs there are 3 goddamn nik column needs to be fuzzied##############################
employee_db_system_id = employee_db[employee_db['SYSTEM_ID'] != "0" ]
employee_db_old_nik = employee_db[employee_db['Old_NIK'] != "0" ]
employee_db_system_id['NIK_comp'] = employee_db_system_id['SYSTEM_ID']
employee_db_old_nik['NIK_comp'] = employee_db_old_nik['Old_NIK']
employee_db['NIK_comp'] = employee_db['NIK']
fuzzy_nik_table = pd.concat([employee_db,employee_db_system_id,employee_db_old_nik])


###########################pull cl data all region###############################################
def fuzzy_clemployee_data(sheetname,region):
    print("processing ",region)
    cl_data = export_to_sheets(sheetname,region)
    cl_data = cl_data.iloc[:,[0,21,24,8,3,23,5,6,10,11,13,18,19]]
    cl_data = cl_data[cl_data.iloc[:, 5].str.lower().isin(["ya", "yes","fraud issue"])]
    
    if len(cl_data)==0:
        return pd.DataFrame()
    else :
        cl_data = cl_data[~((cl_data.iloc[:, 1]=="") & (cl_data.iloc[:, 2]==""))]
        cl_data = cl_data[~((cl_data.iloc[:, 1].isna()) & (cl_data.iloc[:, 2].isna()))]
        
        
        
        
        cl_data = cl_data.rename(columns={cl_data.columns[0]: 'Region',
                                                            cl_data.columns[1]: 'NIK',
                                                            cl_data.columns[2]: 'Recovery - Whom to Deduct?',
                                                            cl_data.columns[3]: 'SUM of Claim Amount',
                                                            cl_data.columns[4]: 'TRID',
                                                            cl_data.columns[5]: 'Deduct?',
                                                            cl_data.columns[6]: 'Resolution Date',
                                                            cl_data.columns[7]: 'Ticket Type',
                                                            cl_data.columns[8]: 'Last Seen Date',
                                                            cl_data.columns[9]: 'Last Seen Hub',
                                                            cl_data.columns[10]: 'Last Seen Driver',
                                                            cl_data.columns[11]: 'Notes',
                                                            cl_data.columns[12]: 'Bukti'
                                                            })
        
        
        
        cl_data["Recovery - Whom to Deduct?"] = cl_data["Recovery - Whom to Deduct?"].str.strip().str.lower()
        cl_data["NIK"] = cl_data["NIK"].astype('str').str.lower().str.strip()
        print("CL_DATA")
        print(cl_data.info())
    
        
        #############split name containing - ###############################################
        clean_name = cl_data[~cl_data["Recovery - Whom to Deduct?"].str.contains("-")]
        dirty_name = cl_data[cl_data["Recovery - Whom to Deduct?"].str.contains("-")]
        
        
        
        def contains_three_letter_word(text):
            words = text.replace('-', ' ').split()
            for word in words:
                if len(word) == 3:
                    return True
            return False
        
        def remove_hub_name(text):
            words = [word.strip() for word in text.split("-")]
            for word in words:
                if len(word) > 3:  # Checking if the word is more than 3 letters
                    if not any(hub in word.lower() for hub in ["rider", "capt", "sph", "d1"]):
                        return word.strip()  # Returning the word if it contains any of the specified words
            return None  # Returning None if no valid word found
        
        clean_name2 = dirty_name[dirty_name["Recovery - Whom to Deduct?"].apply(lambda x: not contains_three_letter_word(x))]
        dirty_name = dirty_name[~dirty_name["Recovery - Whom to Deduct?"].apply(lambda x: not contains_three_letter_word(x))]
    
    
        
        
        ###########for qc#################################################
        # dirty_name[] = dirty_name["Recovery - Whom to Deduct?"].apply(remove_hub_name)
        # clean_name2.to_csv("clean_name2.csv")
        # dirty_name.to_csv("dirty_name.csv")
        ####################################################################
        dirty_name["Recovery - Whom to Deduct?"] = dirty_name["Recovery - Whom to Deduct?"].apply(remove_hub_name)
        
    
        
        ###################Clean_CL_Data#########################################
        cl_data = pd.concat([clean_name,clean_name2,dirty_name])
        cl_data['name_reg'] = cl_data['Recovery - Whom to Deduct?'] + cl_data['Region']
        cl_data['name_reg'] = cl_data['name_reg'].str.strip().str.lower()
        print("CL_DATA")
        print(cl_data.info())
        # cl_data.to_csv("clean_names_final.csv")
        cl_data_withnik = cl_data[cl_data['NIK'].notnull()]
        cl_data_withnik = cl_data_withnik[cl_data_withnik['NIK'] != '']
        cl_data_nonik = cl_data[(cl_data['NIK'].isnull()) | (cl_data['NIK'] == '')]
        
        ##################Fuzzy NIK with Database Employee##############################################     
                
        # Create empty lists to store matched names and IDs
        matched_names = []
        matched_ids = []
        matched_regs = []
        matched_jobposs = []
        threshold = 99
        # Iterate through the names in dataframe A and find the best match in dataframe B using fuzzy matching
        for nik_a in cl_data_withnik['NIK']:
            #best_match = process.extractOne(nik_a, fuzzy_nik_table['NIK_comp'])
            best_match = process.extractOne(nik_a, fuzzy_nik_table['NIK_comp'], processor=None)
            print(best_match[1])
            # best_match is a tuple containing the best matching name and its similarity score
            # You can decide a threshold similarity score to consider a match
            
            if best_match[1] > threshold:
                # Find the corresponding ID in dataframe B
                print("found")
                matched_id = fuzzy_nik_table[fuzzy_nik_table['NIK_comp'] == best_match[0]]['NIK'].iloc[0]
                print(nik_a,"|",best_match[0],"|",best_match[1],"|",matched_id)
                matched_name = fuzzy_nik_table[fuzzy_nik_table['NIK_comp'] == best_match[0]]['Nama_Pegawai'].iloc[0]
                matched_reg = fuzzy_nik_table[fuzzy_nik_table['NIK_comp'] == best_match[0]]['Region'].iloc[0]
                matched_jobpos = fuzzy_nik_table[fuzzy_nik_table['NIK_comp'] == best_match[0]]['Jabatan'].iloc[0]
                matched_names.append(matched_name)
                matched_ids.append(matched_id)
                matched_regs.append(matched_reg)
                matched_jobposs.append(matched_jobpos)
            else:
                print("not found")
                # If no suitable match is found, append None to both lists
                print(nik_a,"|",best_match[0],"|",best_match[1])
                matched_names.append(None)
                matched_ids.append(None)
                matched_regs.append(None)
                matched_jobposs.append(None)
        
        # Add the matched names and IDs as new columns to dataframe A
        cl_data_withnik['matched_name'] = matched_names
        cl_data_withnik['NIK_db'] = matched_ids
        cl_data_withnik['Reg_db'] = matched_regs
        cl_data_withnik['Job_pos_db'] = matched_jobposs
        
        ##################splith data which not found on nik fuzzy to be concated to fuzzy name process###################
        cl_data_fuzzy_nik = cl_data_withnik[~((cl_data_withnik['NIK_db'].isnull()) | (cl_data_withnik['NIK_db'] == ''))]
        cl_data_fuzzy_nik_notfound = cl_data_withnik[(cl_data_withnik['NIK_db'].isnull()) | (cl_data_withnik['NIK_db'] == '')]
        
        cl_data_nonik_nofuzzynik = pd.concat([cl_data_nonik,cl_data_fuzzy_nik_notfound])
        ##############################################################################################################
        ######################Fuzzy Name################################################################################
        matched_names2 = []
        matched_ids2 = []
        matched_regs2 = []
        matched_jobposs2 = []
        threshold2 = 95
        # Iterate through the names in dataframe A and find the best match in dataframe B using fuzzy matching
        cl_data_nonik_nofuzzynik['name_reg'] = cl_data_nonik_nofuzzynik['name_reg'].fillna('-')
        for name_a in cl_data_nonik_nofuzzynik['name_reg']:
            # best_match2 = process.extractOne(name_a, employee_db['name_reg'])
            best_match2 = process.extractOne(name_a, employee_db['name_reg'], processor=None)
            # best_match is a tuple containing the best matching name and its similarity score
            # You can decide a threshold similarity score to consider a match
      
            print(name_a,"|",best_match2[0],"|",best_match2[1])
            if best_match2[1] > threshold2:
                # Find the corresponding ID in dataframe B
                matched_id2 = employee_db[employee_db['name_reg'] == best_match2[0]]['NIK'].iloc[0]
                matched_name2 = employee_db[employee_db['name_reg'] == best_match2[0]]['Nama_Pegawai'].iloc[0]
                matched_reg2 = employee_db[employee_db['name_reg'] == best_match2[0]]['Region'].iloc[0]
                matched_jobpos2 = employee_db[employee_db['name_reg'] == best_match2[0]]['Jabatan'].iloc[0]
                matched_names2.append(matched_name2)
                matched_ids2.append(matched_id2)
                matched_regs2.append(matched_reg2)
                matched_jobposs2.append(matched_jobpos2)
            else:
                # If no suitable match is found, append None to both lists
                matched_names2.append(None)
                matched_ids2.append(None)
                matched_regs2.append(None)
                matched_jobposs2.append(None)
        
        # Add the matched names and IDs as new columns to dataframe A
        cl_data_nonik_nofuzzynik['matched_name'] = matched_names2
        cl_data_nonik_nofuzzynik['NIK_db'] = matched_ids2
        cl_data_nonik_nofuzzynik['Reg_db'] = matched_regs2
        cl_data_nonik_nofuzzynik['Job_pos_db'] = matched_jobposs2
        
        print("KETEMU DI FUZZY NIK")
        print(cl_data_fuzzy_nik.info())
        print("GAKETEMU DI FUZZY NIK")
        print(cl_data_nonik_nofuzzynik.info())
        
        final_output = pd.concat([cl_data_fuzzy_nik,cl_data_nonik_nofuzzynik])
        print("FINAL OUTPUT")
        print(final_output.info())
        
        print(region," done")
        return final_output

list_region  = ["Greater Jakarta","Jawa Timur","Sumatera","Sulawesi","Kalimantan","Indonesia Timur","Jawa Barat","Central Java","First Mile","Land Haul","SORT","Air Freight"]
fuzzy_result = []


for i in list_region:
    result = fuzzy_clemployee_data("Copy of Penalty Scheme - Maret 2024",i)
    fuzzy_result.append(result)
    #time.sleep(1)

fuzzy_result = pd.concat(fuzzy_result)
fuzzy_result.to_csv("clean_names_final7.csv")

print("csv generated")




time.sleep(60)
#######################append to gsheets###############################################
print(fuzzy_result.info())
gsheets_db = fuzzy_result[["TRID","SUM of Claim Amount","NIK","Recovery - Whom to Deduct?","matched_name","NIK_db","Reg_db","Job_pos_db","Resolution Date","Ticket Type","Last Seen Date","Last Seen Hub","Last Seen Driver","Notes","Bukti"]]
# gsheets_db.to_csv("inject_dbv3.csv")
# gsheets_db = gsheets_db.astype(str)
def export_to_sheets2(file_name,sheet_name,df,mode):
    ws = gc.open(file_name).worksheet(sheet_name)
    if(mode=='w'):
        ws.clear()
        gd.set_with_dataframe(worksheet=ws,dataframe=df,include_index=False,include_column_header=True,resize=True)
        return True
    elif(mode=='a'):
        #ws.add_rows(4)
        old = gd.get_as_dataframe(worksheet=ws)
        old = old.dropna(how='all')
        updated = pd.concat([old,df])
        print(old.info())
        print(df.info())
        print(updated.info())
        ws.clear()
        gd.set_with_dataframe(worksheet=ws,dataframe=updated,include_index=False,include_column_header=True,resize=True)
        return True
    else:
        return gd.get_as_dataframe(worksheet=ws)

export_to_sheets2("gsheets for OSA","Sheet1",gsheets_db,mode='w')
print("all region done...")
    
    


