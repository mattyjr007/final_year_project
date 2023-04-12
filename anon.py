import os
import pandas as pd
import numpy as np
import pydicom
import time
import sys
import streamlit as st
import anonypy
import shutil
import boto3
import threading
from boto3.s3.transfer import TransferConfig
import mysql.connector
from datetime import timedelta,datetime
import uuid

class ProgressPercentage(object):

    def __init__(self, filename,upP):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self.upP = upP
        

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = int((self._seen_so_far / self._size) * 100)
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            #print(percentage)
            #st.session_state.percentage = percentage
            self.upP.progress(percentage)
           
            sys.stdout.flush()


class kanon:

    def __init__(self):
        self.zipname = ""
        self.dicomzipdir =""


    def k_anonymize(self,extS,anS,anP,status):
        folder_path = 'dicom'

        extS.empty()
        anS.empty()
        anP.empty()
        status.empty()
        # check if folder exist
        if not os.path.exists(folder_path):
            status.error("please upload a dicom zip file")
            return

        # Initialize an empty list to store the sensitive attributes
        sensitive_attributes = []

        # Loop through all the DICOM files in the folder
        start = time.time()
        for path, subdirs, files in os.walk(folder_path):
            extS.info("Please wait tags extraction in progress..")

            for name in files:
                filename = os.path.join(path, name)
                if filename.endswith('.dcm'):
                    # Load the DICOM file using pydicom
                    dicom_file = pydicom.dcmread(filename)
                    #file_path = os.path.join(folder_path, filename)
                    # Extract the sensitive attributes from the DICOM file and store them in a dictionary
                    attributes_dict = {
                    'file_path': filename,
                    'PatientName': np.NaN,
                    'PatientID': np.NaN,
                    'PatientBirthDate': np.NaN,
                    'PatientSex': np.NaN,
                    'PatientAge': np.NaN,
                    'PatientAddress': np.NaN,
                    'ReferringPhysicianName': np.NaN, 
                    'ReferringPhysicianAddress': np.NaN, 
                    'PerformingPhysicianName': np.NaN, 
                    'InstitutionName': np.NaN,
                    'InstitutionAddress': np.NaN,
                    'PatientWeight': np.NaN,
                    'StudyTime': np.NaN,
                    'Occupation': np.NaN,
                    'ReferringPhysicianTelephoneNumbers':np.NaN,
                    'PatientTelephoneNumbers':np.NaN,
                    'PersonTelephoneNumbers':np.NaN,
                    #'Modality': np.NaN,
                
                    }
                    attr = ['PatientName', 'PatientID', 'PatientBirthDate','PatientSex','PatientAge',
                            'PatientAddress','ReferringPhysicianName','ReferringPhysicianAddress',
                        'StudyTime','PerformingPhysicianName','InstitutionName','InstitutionAddress','PatientWeight',
                            'Occupation','ReferringPhysicianTelephoneNumbers','PatientTelephoneNumbers','PersonTelephoneNumbers']
                    
                    for tag in attr:
                        if tag in dicom_file:
                            attributes_dict[tag] = str(dicom_file.data_element(tag).value)
                    

                # Append the dictionary of attributes to the list of sensitive attributes
                    sensitive_attributes.append(attributes_dict)
                
            

        # Convert the list of sensitive attributes to a pandas dataframe
        sensitive_attributes_df = pd.DataFrame(sensitive_attributes)

        # status for extraction
        stop = time.time() - start

        if len(sensitive_attributes_df)< 1:
            extS.warning("No dicom image found.")
        else:
            if stop < 60:
                msg = "Tags extraction complete. total time:  " + str(np.round(stop,2)) + " secs"
            else:
                msg = "Tags extraction complete. total time:  " + str(np.round(stop/60,2)) + " mins"
            extS.success(msg)    


        metadf  = sensitive_attributes_df[['file_path','PatientID']].copy()

        modfy_df = sensitive_attributes_df.copy().drop('file_path',axis=1).drop_duplicates(subset=['PatientID'])

        generalize_cols = ['PatientName','PatientBirthDate','PatientAddress',
                        'ReferringPhysicianName','ReferringPhysicianAddress',
                        'StudyTime','PerformingPhysicianName','InstitutionName','InstitutionAddress',
                        'Occupation','ReferringPhysicianTelephoneNumbers','PatientTelephoneNumbers','PersonTelephoneNumbers']

        for col in generalize_cols:
            modfy_df[col] = '****'

        modfy_df['PatientAge'] = modfy_df['PatientAge'].astype('str').str.extractall('(\d+)').unstack().fillna('').sum(axis=1).astype(int)


        modfy_df.PatientAge.fillna(0, inplace=True)
        modfy_df.PatientSex.replace('','U', inplace=True)
        modfy_df.PatientWeight.fillna(0,inplace=True)
        modfy_df.fillna('U', inplace=True)

        modfy_df.PatientAge = modfy_df.PatientAge.astype('int64')
        modfy_df.PatientWeight = modfy_df.PatientWeight.astype('float')
        modfy_df.PatientWeight = modfy_df.PatientWeight.astype('int64')


        # feature_columns = ['PatientName', 'PatientBirthDate', 'PatientSex','PatientAge', 'PatientAddress',
        #                 'ReferringPhysicianName','ReferringPhysicianAddress',
        #                 'StudyTime','PerformingPhysicianName','InstitutionName','InstitutionAddress','PatientWeight',
        #                 'Occupation','ReferringPhysicianTelephoneNumbers','PatientTelephoneNumbers','PersonTelephoneNumbers']

        attr.remove("PatientID")
        feature_columns = attr
        sensitive_column = "PatientID"

        # feature_columns2 = ['PatientName', 'PatientBirthDate', 'PatientSex', 'PatientAddress',
        #                 'ReferringPhysicianName','ReferringPhysicianAddress',
        #                 'StudyTime','PerformingPhysicianName','InstitutionName','InstitutionAddress',
        #                     'Occupation','ReferringPhysicianTelephoneNumbers','PatientTelephoneNumbers','PersonTelephoneNumbers']

        feature_columns2 = modfy_df.select_dtypes(include=['object']).columns.to_list()
        feature_columns2.remove("PatientID")
                                    
        for name in feature_columns2:
                modfy_df[name] = modfy_df[name].astype("category")


        p = anonypy.Preserver(modfy_df, feature_columns, sensitive_column)
        rows = p.anonymize_k_anonymity(k=5) # k should be less than length of data/2

        dfn = pd.DataFrame(rows)

        dfn['indx'] = range(1,len(dfn)+1)

        dfn.drop('count',axis=1, inplace=True)
        final_df = pd.merge(metadf,dfn, how='inner',on='PatientID')

        final_df.PatientID = "****"
        final_df.PatientID = final_df.PatientID + final_df.indx.astype(str)
        final_df.drop('indx',axis=1, inplace=True)


        # Anonymization begins
        k = 0
        max = len(final_df) - 1
        anP.progress(0.0)

        anS.info("Please wait anonymization in progress..")

        #start time
        start = time.time()
        for i in range(len(final_df)):

            filepath = final_df.loc[i,'file_path']
            
            # Load the DICOM file using pydicom
            dicom_file = pydicom.dcmread(filepath)
            
            attr = ['PatientName', 'PatientID', 'PatientBirthDate','PatientSex','PatientAge','PatientAddress','PatientWeight']
            attr2 = ['ReferringPhysicianName','ReferringPhysicianAddress','StudyTime','PerformingPhysicianName','InstitutionName',
                    'InstitutionAddress','Occupation','ReferringPhysicianTelephoneNumbers','PatientTelephoneNumbers','PersonTelephoneNumbers']
            
            for tag in attr2:
                if tag in dicom_file:
                    dicom_file.data_element(tag).value = None
                
                
            for tag in attr:
                if tag in dicom_file:
                    #dicom_file.data_element(tag).value = str(final_df.loc[i,tag])
                    VR = pydicom.dataelem.DataElement(tag,'LO',final_df.loc[i,tag])
                    dicom_file.add(VR)
            dicom_file.save_as(filepath)

            anP.progress(k/max)

            k +=1

        stop = time.time() - start
        msg = "anonymization complete. total time: " + str(np.round(stop,2)) + "secs"
        anS.success(msg)
        # convert to zip
        self.zipname = 'dico'+ str(time.time())
        self.dicomzipdir = 'dicomzip/'+self.zipname
        shutil.make_archive(self.dicomzipdir, 'zip', 'dicom')
        # delete folder
        shutil.rmtree('dicom')

        return True


    def uploadS3(self,upS,upP):
    
        upS.empty()
        upP.empty()

        # Creating the low level functional client
        client = boto3.client(
                's3',
                aws_access_key_id = os.getenv('aws_access_key_id'),
                aws_secret_access_key = os.getenv('aws_secret_access_key'),
                region_name = os.getenv('region')
            )

        s3db = 'dicomdatabase'

        zipnamezip = self.zipname +".zip"
        thezipdirname = self.dicomzipdir + ".zip"
        upS.info("Uploading to database in progress...")

        try:  
            upP.progress(0)
            client.upload_file(thezipdirname,s3db,zipnamezip,Callback=ProgressPercentage(thezipdirname,upP),Config = TransferConfig(use_threads=False) )
        except: 
            upS.error("There was an issue uploading this file to the database..")
            return
        shutil.rmtree('dicomzip')
        upS.success("uploading successful")

        return True



    def uploadSQL(self,name,email,descrip,sqS):
            
            self.name = name
            self.email = email
            self.description = descrip
            self.role = "user"
            self.accesskey = ""
            zipnameQ = self.zipname + ".zip"

            # Connect to server
            db =  mysql.connector.connect(
                                            host=st.secrets["DB_HOST"],
                                            port=3306,
                                            user= st.secrets["user"],
                                            password= st.secrets["pass"],
                                            database = st.secrets["database"])

            # Get a cursor
            cur = db.cursor()

            query = ("SELECT COUNT(*) FROM Dicometa "
                    "WHERE Email = %s AND Role =  %s")
            admin = 'admin'
            cur.execute(query, (self.email,admin))

            if cur.fetchone()[0] > 0:
                self.role = "admin"
                self.accesskey = str(uuid.uuid1())

            add_file = ("INSERT INTO Dicometa "
               "(`File ID`, `Authors Name`, `Study Description`, Email, Role, Accesskey) "
               "VALUES (%s, %s, %s, %s, %s,  %s)")
            
            file_details = (zipnameQ, self.name, self.description, self.email,self.role,self.accesskey)

            try:
                cur.execute(add_file,file_details)
                db.commit()

                if self.role == 'admin':
                    sqS.info("please keep your accesskey safe. `{}`".format(self.accesskey))
            except:
                sqS.error("An error occured uploading your file to database")    




