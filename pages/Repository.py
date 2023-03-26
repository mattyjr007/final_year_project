import streamlit as st
import pandas as pd 
import mysql.connector
import numpy as np
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode
import webbrowser
import logging
import boto3
from botocore.exceptions import ClientError

st.set_page_config(page_title="Repository", page_icon="📄",layout="wide")



def create_presigned_url(bucket_name, object_name, expiration=3600):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    s3_client = boto3.client(
                's3',
                aws_access_key_id = 'AKIAR5QMEOGORZU3VBIR',
                aws_secret_access_key = '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',
                region_name = 'us-east-1'
            )
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
def get_data():

        try:
            mydb =  mysql.connector.connect(
                                        host="dicomdb.ccteipanw4qp.us-east-1.rds.amazonaws.com",
                                        port=3306,
                                        user="admin",
                                        password="Godisgood007",
                                        database = "dicomDB")
            
            query = "Select `File ID`, `Authors Name`,`Study Description`,`Email`, `Date` from Dicometa;"
            data = pd.read_sql(query,mydb)
            mydb.close() #close the connection
        except Exception as e:
            #mydb.close()
            st.error("an error occured when loading database{}".format(str(e)))

        return data


# get the datatable
data = get_data()

#data= pd.read_csv('MOCK_DATA.csv', index_col=0) 


with st.container():
    st.header("MatZap Anonymized files repository")
    st.write("Welcome to matzap public repository containing anonymized files using the app\
             . search for the file and select it via the check box and proceed to download.")
    
    st.info("**Note:** some files are privately owned please email the author to get access.")


gb = GridOptionsBuilder.from_dataframe(data)
gb.configure_default_column( autoHeight=True,minWidth=300, maxWidth=500)
gb.configure_column('File ID',minWidth=300, maxWidth=400)
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
    bucket_name = 'dicomdatabase'
    object_key = df['File ID'][0]

    url = create_presigned_url(bucket_name, object_key)

    btn = st.button('download file')

    if btn:
     
        if url is not None:
        #response = requests.get(url)
        #st.write(url)
            webbrowser.open(url)





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

