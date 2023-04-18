import streamlit as st
import zipfile
import shutil
from PIL import Image
import time
from anon import kanon as ka
import numpy as np
import os
import imageio
import matplotlib.pyplot as plt
import pandas as pd
import pydicom

st.set_page_config(page_title="Anonymizer App", page_icon="🏠")

# this function save the single dicom image into a folder
def save_uploadedfile(uploadedfile):
     try:
         os.mkdir("tempDir")
     except:
         print("directory tempDir exists")
         shutil.rmtree('tempDir')
         os.mkdir("tempDir")

     with open(os.path.join("tempDir",uploadedfile.name),"wb") as f:
         f.write(uploadedfile.getbuffer())
     return st.success("Uploading File:{} successful".format(uploadedfile.name))

# single file anonymizer function
def single_anonymize(pathh):
    filepath = pathh
    
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
            VR = pydicom.dataelem.DataElement(tag,'LO','****')
            dicom_file.add(VR)
    dicom_file.save_as(filepath)


header = st.container()
extractzip = st.container()

headerImg = Image.open('imgs/0o9r95kf.png')

with header:
    st.title("MatZap Dicom Anonymizer")
    st.image(headerImg)
    st.write('')
    st.write("Hey!, Welcome to Matzap dicom imaging k-anonymizer web application. this website is a product of the project \
           `Anonymization of medical images using k-anonymity`.\
             To anonymize your file please create a zip of your dicom dataset and upload below.")

    st.warning("Please note that anonymizing a single file does not apply K-anonymity technique.")

runupload = False

with extractzip:
    #st.markdown("<h6>Upload dicom zip file or single dicom file</h6>", unsafe_allow_html=True)
    
    file_uploaded = st.file_uploader("Upload dicom zip file or single dicom file", type=["dcm","zip"], accept_multiple_files=False)

    upload_sucess = st.empty()

    if 'ran' not in st.session_state:
        if file_uploaded is not None:

             if file_uploaded.type == "application/zip":
            
                info = st.info("Please wait extraction in progress")
                with zipfile.ZipFile(file_uploaded,"r") as z:
                    z.extractall("dicom")
                    info.empty()
                    upload_sucess.success("extraction complete...")
                    #shutil.make_archive('dico', 'zip', 'dicom')
                st.session_state.ran = True  
             else:
                 
                  # save single dcm file
                 save_uploadedfile(file_uploaded)

                 st.markdown("<h4 style='text-align: center;'>Viewing Unanonymized Dicom File</h4>", unsafe_allow_html=True)
                
                 im = imageio.imread('tempDir/{0}'.format(file_uploaded.name))  
                   
                 fig, ax = plt.subplots(dpi=200)
                 ax.axis('off')
                 ax.imshow(im)
                 fig.set_size_inches(12, 12)
                    #fig.savefig('tempDir/image.png',transparent=True)
                    #myimg =Image.open('tempDir/image.png')
                    #new_image = myimg.resize((400, 500))
                 a,b = st.columns(2)
                    #a.dataframe(im.meta)
                 dicodf = pd.DataFrame.from_dict(im.meta, orient='index', columns=['Attributes'])
                # dicodf = pd.DataFrame(im.meta).iloc[0,:].T
                    #dicodf = pd.DataFrame(dicodf)
                    #dicodf.columns = ['Attributes']
                 a.dataframe(dicodf,height=400)
                    #b.image(new_image)
                 b.pyplot(fig)
                 
    else:
        upload_sucess.success("extraction complete...")        
        #st.success("extraction complete...")

def k_anon(extS,anS,anP,status,upS,upP,sqS):    

    if name.strip() == "":
        status.warning("Please enter an author name")
        return
    elif email.strip() == "":
        status.write("Please enter your email address.")
        return
     
    #initialize the k-anonymize function
    anonymizer = ka() 
    # checks for uploaded file
    if file_uploaded is None:
        status.error("please upload a dicom zip file or single dicom file")
        return
    else:
        # check if it is single file then anonymize and end function
        if file_uploaded.type != "application/zip":
            sing_path = 'tempDir/{0}'.format(file_uploaded.name)
            try:
                #anonymize the single file
                tempInfo= st.empty()
                tempInfo.info("please wait anonymizing in progress...")
                single_anonymize(sing_path)
                tempInfo.success("anonymization complete")

                st.markdown("<h4 style='text-align: center;'>Viewing Anonymized Dicom File</h4>", unsafe_allow_html=True)
                
                imA = imageio.imread(sing_path)  
                   
                fig2, ax2 = plt.subplots(dpi=200)
                ax2.axis('off')
                ax2.imshow(imA)
                fig2.set_size_inches(12, 12)
                # create columns
                c,d = st.columns(2)
                # dicom dataframe
                dicodfA = pd.DataFrame.from_dict(imA.meta, orient='index', columns=['Attributes'])
                # dicodf = pd.DataFrame(im.meta).iloc[0,:].T
                    #dicodf = pd.DataFrame(dicodf)
                    #dicodf.columns = ['Attributes']
                c.dataframe(dicodfA,height=400)
                    #b.image(new_image)
                d.pyplot(fig2)
                # for displaying any error messagw
                sStatus = st.empty()
                msg2 = st.empty()

                anonymizer.single_upload(sing_path,name,email,studydesc,sStatus,msg2)

            except:
                st.error("an error occured while anonymizing file")

            return #end code after anonymizing single file


   
    # hanlde zip anonymization
    result = anonymizer.k_anonymize(extS,anS,anP,status)
    
    if result:
        s3result = anonymizer.uploadS3(upS,upP)

        if s3result:
            anonymizer.uploadSQL(name, email,studydesc,sqS)
    else:
        status.error("an error occured in anonymizing file.")

    #info.info("haba")
    #time.sleep(4)
    

formcontainer = st.container()


with formcontainer:
    myform = st.form("my form")
    name = myform.text_input("Your Name")
    email = myform.text_input("Email")
    studydesc = myform.text_area("Study Description")
    
    

    anonymize = myform.form_submit_button("anonymize file")#,on_click=lambda: k_anon(err))

    info = st.info("")
    info.empty()
    success = st.success("")
    success.empty()
    #err = st.error("")

    extS = st.empty()
    anS = st.empty()
    anP = st.empty()
    status = st.empty()
    upS = st.empty()
    upP = st.empty()
    sqS = st.empty()

    if anonymize:
        k_anon(extS,anS,anP,status,upS,upP,sqS)



             
        
       




