# -*- coding: utf-8 -*-
"""
Created on Mon Apr  1 10:26:26 2024

@author: NXP
"""
import requests
import time
import json
import gspread
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaFileUpload
import io
from googleapiclient.errors import HttpError
import gspread_dataframe as gd
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from io import BytesIO
import os
import gspread as gs
import numpy as np
########send mail lib###################
import ssl
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib

def send_mail(sender,recipient,app_password):
    recipients = [recipient] 
    emaillist = [elem.strip().split(',') for elem in recipients]
    msg = MIMEMultipart()
    msg['Subject'] = "L&D Ingest Employee Data Error"
    msg['From'] = 'L&D Project'
    
    
    html = """\
    <html>
      <head></head>
      <body>
        <p>Ingesting latest employee data from gdrive has been error >3 times, likely there are column change in one of the 3 latest file in g-drive<p>
      </body>
    </html>
    """
    
    part1 = MIMEText(html, 'html')
    msg.attach(part1)
    
    
    
    context = ssl.create_default_context()
    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.ehlo() 
        server.starttls(context=context)
        server.ehlo() 
        server.login(sender, app_password)
        server.sendmail(sender, [recipient], msg.as_string()) 
    
    print("Status : Email Sent") 

credentials ={

  }
gc= gs.service_account_from_dict(credentials)

def retrieve_from_sheets(file_name,sheet_name):
    ws = gc.open(file_name).worksheet(sheet_name)
    return gd.get_as_dataframe(worksheet=ws,value_render_option='FORMATTED_VALUE')

def export_to_sheets(file_name,sheet_name,df,mode='r'):
    ws = gc.open(file_name).worksheet(sheet_name)
    if(mode=='w'):
        ws.clear()
        gd.set_with_dataframe(worksheet=ws,dataframe=df,include_index=False,include_column_header=True,resize=True)
        return True
    elif(mode=='a'):
        #ws.add_rows(4)
        old = gd.get_as_dataframe(worksheet=ws)
        updated = pd.concat([old,df])
        ws.clear()
        gd.set_with_dataframe(worksheet=ws,dataframe=updated,include_index=False,include_column_header=True,resize=True)
        return True
    else:
        return gd.get_as_dataframe(worksheet=ws)
    
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

def list_files(service, folder_id):
    results = service.files().list(
        pageSize=1000,
        fields="nextPageToken, files(id, name, mimeType, size, modifiedTime)",
        q=f"'{folder_id}' in parents"
    ).execute()
    items = results.get('files', [])
    compiled_employee = []
    return items

def read_excel_drive(service, folder_id):
    results = service.files().list(
        pageSize=1000,
        fields="nextPageToken, files(id, name, mimeType, size, modifiedTime)",
        q=f"'{folder_id}' in parents"
    ).execute()
    items = results.get('files', []) 
    # if len(items)<1:
    #     recipients = ['andi.darmawan@ninjavan.co'] 
    #     emaillist = [elem.strip().split(',') for elem in recipients]
    #     msg = MIMEMultipart()
    #     msg['Subject'] = "L&D Deduction"
    #     msg['From'] = 'andi.darmawan@ninjavan.co'
    
    
    #     html = """\
    #     <html>
    #       <head></head>
    #       <body>
    #         HR has not uploaded all employee data for this month
    #       </body>
    #     </html>
    #     """
    
    #     part1 = MIMEText(html, 'html')
    #     msg.attach(part1)



    # context = ssl.create_default_context()
    # with smtplib.SMTP('smtp.gmail.com', 587) as server:
    #     server.ehlo() 
    #     server.starttls(context=context)
    #     server.ehlo() 
    #     server.login('andi.darmawan@ninjavan.co', 'lxtkaqzlhjasscel')
    #     server.sendmail('andi.darmawan@ninjavan.co', ['haritsa.wardani@ninjavan.co','rafli.fauzan@ninjavan.co'], msg.as_string()) 

    # print("Status : Email Sent")  
     
    compiled_employee = []
    for file in items:
          name = file['name'][:-5].lower()
          print(f"Reading Google Sheets file: {name}")
          print('Num of char in file name = ',len(file['name']))
          if file['mimeType'] == 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet':
              if name.startswith("report"):
                  print("starts with report")
                  file_id = file['id']
                  request = service.files().get_media(fileId=file_id)
                  downloaded_file = BytesIO(request.execute())
          
                  # Read the Excel file into a pandas DataFrame
                  excel_data = pd.read_excel(downloaded_file)
                  #use (User/Employee ID) to lookup to dict to get region, namain "Region"
                  ##if (Employment Details Payroll ID) kosong maka isi dengan (User/Employee ID)
                  ##tambahin kolom namanya (SYSTEM ID (DRIVER ID)) isinya 0 aja
                  ##ambil 7 karakter dari belakang di kolom (Location  Description) namain "Station Name"
                  ## Rename (Employment Details Payroll ID) ke "NIK"
                  ## Rename (Full Name (as per official document) ke "Nama Pegawai"
                  ## Rename (Job Title) ke "Jabatan"
                  selected_columns = excel_data.iloc[:, [4, 5,8, 10,17,18,19]]
                  # Define a dictionary mapping old column names to new column names
                  selected_columns = selected_columns.rename(columns={selected_columns.columns[1]: 'Employment Details Payroll ID',
                                                                        selected_columns.columns[3]: 'Nama Pegawai',
                                                                        selected_columns.columns[2]: 'Jabatan',
                                                                        selected_columns.columns[4]: 'Station Name',
                                                                        selected_columns.columns[0]: 'NIK',
                                                                        selected_columns.columns[5]: 'Department',
                                                                        selected_columns.columns[6]: 'Cost Center'
                                                                        })
        
                  
                  selected_columns['SYSTEM ID'] = 0
      
                  selected_columns['Station Name'] = selected_columns['Station Name'].str.split(':').str[-1]
                  #selected_columns['NIK'] = selected_columns.apply(lambda row: row['User/Employee ID'] if pd.isnull(row['NIK']) or row['NIK'] == '' else row['NIK'], axis=1)
                  selected_columns = selected_columns[['NIK','Nama Pegawai','Jabatan','Station Name','SYSTEM ID','Department','Cost Center']]
                  #read dictionary Hub-Region, dapet dari redash sort_prod_gl.hubs, tapi ada yang perlu di adjust
                  #east,west,central java ubah ke indo,Sumatra 1,3,4 = nothern sumatera,Sumatra 2,5,6 = southern sumatera
                  while True:
                      try:
                          params2 = {}
                          api_key2 = 'Poqwen8BjW4zjLHOzpmSlXDnm7TSrV5DzevDv6eD'
                          result2 = get_fresh_query_result('https://redash-id.ninjavan.co/',2289, api_key2, params2)
                          hub_region = pd.DataFrame(result2)
                          break
                      except:
                          print('>> Pulling failed, retrying...')
                  selected_columns= pd.merge(selected_columns,hub_region[['Station Name','Region']], left_on =['Station Name'],right_on=['Station Name'],how ='left')
                  selected_columns['Old_NIK'] = 0
                  selected_columns['created_at'] = datetime.now()
                  selected_columns['source_file'] = "SAP_File"
                  selected_columns = selected_columns[['NIK','Nama Pegawai','Jabatan','Station Name','Region','SYSTEM ID','Old_NIK','Department','Cost Center','created_at','source_file']]
                  print(selected_columns.info())
                  #compiled_employee.append(selected_columns)
                  export_to_sheets("HRIS_Ninja_Employee","SAP_Data",selected_columns,mode='w')
                  print("SAP data has been read")
                
                  
              elif name.endswith("incentive"):
                  print("ends with incentive")
            
                  file_id2 = file['id']
                  
                  request2 = service.files().get_media(fileId=file_id2)
                  downloaded_file2 = BytesIO(request2.execute())
          
                  # Read the Excel file into a pandas DataFrame
                  excel_data2 = pd.read_excel(downloaded_file2,header=5)
                  #rename ((EMPLOYEE ID) NIK) ke "NIK"
                  #Rename (Nama Pegawai) ke "Nama Pegawai"
                  #Rename (Posisi) ke "Jabatan"
                  #Rename (KODE HUB)ke "Station Name"
                  #Rename (Regional) ke "Region"
                  selected_columns2 = excel_data2.iloc[:, [1, 2,3, 4,8,13,5,6]]
                  selected_columns2 = selected_columns2.rename(columns={selected_columns2.columns[0]: 'NIK',
                                                                      selected_columns2.columns[1]: 'SYSTEM ID',
                                                                      selected_columns2.columns[3]: 'Jabatan',
                                                                      selected_columns2.columns[2]: 'Nama Pegawai',
                                                                      selected_columns2.columns[4]: 'Station Name',
                                                                      selected_columns2.columns[5]: 'Region',
                                                                      selected_columns2.columns[6]: 'Department',
                                                                      selected_columns2.columns[7]: 'Cost Center'})                  
                  
                  selected_columns2['Old_NIK'] = 0
                  selected_columns2['created_at'] = datetime.now()
                  selected_columns2['source_file'] = "Payment_Incentive_File"
                  selected_columns2 = selected_columns2[['NIK','Nama Pegawai','Jabatan','Station Name','Region','SYSTEM ID','Old_NIK','Department','Cost Center','created_at','source_file']]
                  print(selected_columns2.info())
                  #compiled_employee.append(selected_columns2)
                  export_to_sheets("HRIS_Ninja_Employee","Payment_Incentive",selected_columns2,mode='w')
                  print("incentive data has been read")
              else :
                  print("ops employee data")
                  file_id3 = file['id']
                  request3 = service.files().get_media(fileId=file_id3)
                  downloaded_file3 = BytesIO(request3.execute())
          
                  # Read the Excel file into a pandas DataFrame
                  excel_data3 = pd.read_excel(downloaded_file3)
                  selected_columns3 = excel_data3.iloc[:, [5,6,15,17,4,3,16,19]]
                  selected_columns3['Station Name'] = "-"
                  selected_columns3 = selected_columns3.rename(columns={selected_columns3.columns[0]: 'NIK',
                                                                      selected_columns3.columns[1]: 'Nama Pegawai',
                                                                      selected_columns3.columns[2]: 'Jabatan',
                                                                      selected_columns3.columns[3]: 'Region',
                                                                      selected_columns3.columns[4]: 'SYSTEM ID',
                                                                      selected_columns3.columns[5]: 'Old_NIK',
                                                                      selected_columns3.columns[6]: 'Department',
                                                                      selected_columns3.columns[7]: 'Cost Center'})
                  selected_columns3['Jabatan'] = selected_columns3['Jabatan'].str.replace(r"\(.*\)", "", regex=True)
                  selected_columns3['created_at'] = datetime.now()
                  selected_columns3['source_file'] = "Ops_File"
                  selected_columns3 = selected_columns3[['NIK','Nama Pegawai','Jabatan','Station Name','Region','SYSTEM ID','Old_NIK','Department','Cost Center','created_at','source_file']]
                  print(selected_columns3.info())
                  #compiled_employee.append(selected_columns3)
                  export_to_sheets("HRIS_Ninja_Employee","Ops",selected_columns3,mode='w')
                  print("ops data has been read")
            
######################Tarik data dari ke-3 sheet yang menyimpan data sap,incentive, dan ops. lalu concat dan dump ke sheet/db untuk all employee data##############
####################Ini dilakukan instead of langsung concat ke-3 nya dan dump ke sheet all employe data, agar jika HR belum upload ke -3 file di tgl5, maka tabel all employee hanya akan mengupdate berdasarkan file yang sudah diupload saja, the other are using last month data##############                  
    SAP_Data = retrieve_from_sheets("HRIS_Ninja_Employee","SAP_Data")
    Payment_Incentive = retrieve_from_sheets("HRIS_Ninja_Employee","Payment_Incentive")
    Ops = retrieve_from_sheets("HRIS_Ninja_Employee","Ops")
    
    compiled_employee1 = pd.concat([SAP_Data,Payment_Incentive,Ops], ignore_index=True)
    compiled_employee1['Region'] = compiled_employee1['Region'].str.strip()
    
    # Define conditions and choices
    conditions = [
        (compiled_employee1['Region'] == "West Java"),
        (compiled_employee1['Region'] == "East Java"),
        (compiled_employee1['Region'] == "Central Java"),
        (compiled_employee1['Region'] == "Sumatera 1"),
        (compiled_employee1['Region'] == "Sumatera 2"),
        (compiled_employee1['Region'].isin(["Sumatera 3", "Sumatera 4","Sumatera 7","Sumatera 8","Sumatera 9"])),
        (compiled_employee1['Region'].isin(["Sumatera 5", "Sumatera 6"]))
    ]
    
    choices = [
        'Jawa Barat',
        'Jawa Timur',
        'Jawa Tengah',
        'Northern Sumatera',
        'Southern Sumatera',
        'Northern Sumatera',
        'Southern Sumatera'
    ]
    
    # Apply conditions using np.select()
    compiled_employee1['Region'] = np.select(conditions, choices, default=compiled_employee1['Region'])
    compiled_employee1['Region'].fillna('-', inplace=True)
    compiled_employee1['Station Name'].fillna('-', inplace=True)
    
    export_to_sheets("HRIS_Ninja_Employee","Employee_TableV1",compiled_employee1,mode='w')
    
    employee_db = compiled_employee1
    employee_db_system_id = employee_db[employee_db['SYSTEM ID'] != "0" ]
    employee_db_old_nik = employee_db[employee_db['Old_NIK'] != "0" ]
    employee_db_system_id['NIK_comp'] = employee_db_system_id['SYSTEM ID']
    employee_db_old_nik['NIK_comp'] = employee_db_old_nik['Old_NIK']
    employee_db['NIK_comp'] = employee_db['NIK']
    fuzzy_nik_table = pd.concat([employee_db,employee_db_system_id,employee_db_old_nik])
    fuzzy_nik_table = fuzzy_nik_table[["NIK","Nama Pegawai"]]
    export_to_sheets("HRIS_Ninja_Employee","NIK Autocomplete",fuzzy_nik_table,mode='w')
    
    return compiled_employee1

def get_month_from_folder_name(folder_name):
    parts = folder_name.split()
    if len(parts) >= 3:
        return parts[-2]
    else:
        return None

def read_files_in_folder(service, folder_id):
    items = list_files(service, folder_id)
    #current_month = datetime.datetime.now().strftime('%B')
    # if datetime.datetime.now() > 5:
    #     current_month = (datetime.datetime.now()).strftime('%B')
    # else:
    #     current_month =  (datetime.datetime.now() - relativedelta(months=1)).strftime('%B')
    current_month = (datetime.now()).strftime('%B')
    current_date = (datetime.now()).strftime('%d')
    print(current_month)
    for item in items:
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            folder_name = item['name']
            folder_month = get_month_from_folder_name(folder_name)
            if folder_month and folder_month.lower() == current_month.lower():
                # If it's a folder for the current month, recursively call the function to read files inside it
                
                return read_excel_drive(service, item['id'])




# Define your main folder ID
scope = ['https://www.googleapis.com/auth/drive']
service_account_json_key = 'serv_acc.json'
credentials = service_account.Credentials.from_service_account_file(
                              filename=service_account_json_key, 
                              scopes=scope)
upload_folder_id = '1tnRYUzEgC-MQJaVrBsx6EpW2fcmxyXxu'
service = build('drive', 'v3', credentials=credentials)
#employee_table = read_files_in_folder(service, upload_folder_id)
# Call the function to read files in the main folder and its subfolders
counter = 1
while True:
    try:
        employee_table = read_files_in_folder(service, upload_folder_id)
        break
    except:
        print("failed_retrying")
        counter = counter + 1
        if counter == 3:
            send_mail('andi.darmawan@ninjavan.co','andi.darmawan@ninjavan.co','lctoacfoivynruln')
            
        
print(employee_table.info())
print("Done")
#employee_table.to_excel("employee_table_finel.xlsx")

