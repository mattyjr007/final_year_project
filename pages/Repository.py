import streamlit as st
import pandas as pd 
import mysql.connector
import numpy as np
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode
import webbrowser
import logging
import boto3
from botocore.exceptions import ClientError
import os

st.set_page_config(page_title="Repository", page_icon="📄",layout="wide")

@st.cache_resource(ttl=2000)
def connect_s3():
    s3_client = boto3.client(
                's3',
                aws_access_key_id = os.getenv("aws_access_key_id"),
                aws_secret_access_key = os.getenv("aws_secret_access_key"),
                region_name = os.getenv('region')
            )
    return s3_client


def create_presigned_url(bucket_name, object_name, expiration=3600):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    s3_client = connect_s3()
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
    except ClientError as e:
        logging.error(e)
        return None

    # The response contains the presigned URL
    return response

@st.cache_resource
def db_connect():
     mydb =   mysql.connector.connect(
                                            host=st.secrets["DB_HOST"],
                                            port=3306,
                                            user= st.secrets["user"],
                                            password= st.secrets["pass"],
                                            database = st.secrets["database"] )
     return mydb

@st.cache_resource(ttl=100)
def get_data():

        try:
            mydb =  mydb =   mysql.connector.connect(
                                            host=st.secrets["DB_HOST"],
                                            port=3306,
                                            user= st.secrets["user"],
                                            password= st.secrets["pass"],
                                            database = st.secrets["database"] )
   
            
            query = "Select `File ID`, `Authors Name`,`Study Description`,`Email`, `Date` from Dicometa;"
            data = pd.read_sql(query,mydb)
            query2 = "Select `File ID`,`Email`,`Role`,`Accesskey` from Dicometa;"
            data2 = pd.read_sql(query2,mydb)
            mydb.close() #close the connection
            return data,data2
        except Exception as e:
            #mydb.close()
            st.error("an error occured when loading database{}".format(str(e)))
            return None


# get the datatable
try :
    data,data_copy = get_data()
except:
    st.error("Unable to access database")
#data= pd.read_csv('MOCK_DATA.csv', index_col=0) 


with st.container():
    st.header("MatZap Anonymized files repository")
    st.write("Welcome to matzap public repository containing anonymized files using the app\
             . search for the file and select it via the check box and proceed to download.")
    
    st.info("**Note:** some files are privately owned please email the author to get access.")


gb = GridOptionsBuilder.from_dataframe(data)
gb.configure_default_column( autoHeight=True,minWidth=300, maxWidth=500)
gb.configure_column('File ID',minWidth=240, maxWidth=400)
gb.configure_column('Authors Name',minWidth=100, maxWidth=150)
gb.configure_column('Study Description',minWidth=100, maxWidth=150)
gb.configure_column('Email',minWidth=150, maxWidth=200)
gb.configure_column('Date',minWidth=150, maxWidth=150)
gb.configure_pagination(paginationAutoPageSize=False,paginationPageSize=17) #Add pagination
gb.configure_side_bar() #Add a sidebar
gb.configure_selection('single', use_checkbox=True, groupSelectsChildren="Group checkbox select children") #Enable multi-row selection
gridOptions = gb.build()


grid_response = AgGrid(
    data,
    gridOptions=gridOptions,
    data_return_mode='AS_INPUT', 
    update_mode='MODEL_CHANGED', 
    fit_columns_on_grid_load=False,
    enable_enterprise_modules=True,
    theme = "streamlit",
    reload_data=True,
    width='100%'
)

data = grid_response['data']
selected = grid_response['selected_rows'] 
df = pd.DataFrame(selected)



if len(df)>0:

    st.write(df[['File ID','Email','Date']])

    bucket_name = 'dicomdatabase'
    object_key = df['File ID'][0]
    

    url = create_presigned_url(bucket_name, object_key)

    df_filter = data_copy[data_copy['File ID'] == object_key]
    df_role = df_filter[df_filter['Role'] == 'admin']

    if  len(df_role) > 0:
        #print(df_filter.Email.values[0])
        st.info("This file is owned by admin please request for accesskey from `{0}` to download tih file!".format(df_filter.Email.values[0]))
        input_accessk = st.text_input("Input access key here")
        verify_btn = st.button("verify key")

        if verify_btn:
          
          if  input_accessk.strip() == df_filter['Accesskey'][0]:

            if url is not None:
                button_style = 'font-weight: 700; padding: 10px 13px; background-color: #5E5DF0; color: white; border: none; border-radius: 15px;'

                st.write(f'''
                        <a target="_self" href="{url}">
                            <button style="{button_style}">
                                Download file
                            </button>
                        </a>
                        ''',
                        unsafe_allow_html=True
                    )        


          else:
            st.error('Access key invlaid for file.')   


    else:
        
        if url is not None:

            button_style = 'font-weight: 700; padding: 10px 13px; background-color: #5E5DF0; color: white; border: none; border-radius: 15px;'

            st.write(f'''
                    <a target="_self" href="{url}">
                        <button style="{button_style}">
                            Download file
                        </button>
                    </a>
                    ''',
                    unsafe_allow_html=True
                )        





    # if len(df)>0:
    #     st.write(df[['File ID','Email','Date']])
    #     fs = s3fs.S3FileSystem(key='AKIAR5QMEOGORZU3VBIR',secret ='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' )
    #     print(df['File ID'][0])

    #     with fs.open("dicomdatabase/{}".format(df['File ID'][0]),"rb") as f:
    #         btn = st.download_button(
    #             label= "download file",
    #             data= f.read(),
    #             file_name=df['File ID'][0],
    #             mime="application/zip"
    #         )
    #Pass the selected rows to a new dataframe df

