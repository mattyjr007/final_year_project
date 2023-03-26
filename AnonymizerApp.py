import streamlit as st
import zipfile
import shutil
from PIL import Image
import time
from anon import kanon as ka
import numpy as np

st.set_page_config(page_title="Anonymizer App", page_icon="🏠")

header = st.container()
extractzip = st.container()

headerImg = Image.open('imgs/cyber.png')

with header:
    st.title("MatZap Dicom Anonymizer")
    st.image(headerImg)
    st.write('')
    st.write("Hey!, Welcome to Matzap dicom imaging k-anonymizer web application. this website is a product of the project \
           `Anonymization of medical images using k-anonymity`.\
             To anonymize your file please create a zip of your dicom dataset and upload below.")

runupload = False

with extractzip:
    
    file_uploaded = st.file_uploader("Upload dicom zip", type=["zip"], accept_multiple_files=False)

    upload_sucess = st.empty()

    if 'ran' not in st.session_state:
        if file_uploaded is not None:
            info = st.info("Please wait extraction in progress")
            with zipfile.ZipFile(file_uploaded,"r") as z:
                z.extractall("dicom")
                info.empty()
                upload_sucess.success("extraction complete...")
                #shutil.make_archive('dico', 'zip', 'dicom')
            st.session_state.ran = True    
    else:
        upload_sucess.success("extraction complete...")        
        #st.success("extraction complete...")

def k_anon(extS,anS,anP,status,upS,upP,sqS):    

    if file_uploaded is None:
        status.error("please upload a dicom zip file")
        return
    elif name.strip() == "":
        status.warning("Please enter an author name")
        return
    elif email.strip() == "":
        status.write("Please enter your email address.")
        return


    anonymizer = ka()
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



             
        
       




