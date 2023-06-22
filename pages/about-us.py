import streamlit as st

st.set_page_config(page_title="About Us", page_icon="🕸️")

header = st.container()

with header:

    st.title("About us")
    st.write("This project `ANONYMIZATION OF MEDICAL IMAGES USING K-ANONYMITY`\
             was carried out by Mr Mathias Samuel and Supervised under Dr Alamu Femi of the University Of Lagos, Nigeria.\
             \
             ")
    st.write("The MondFly is used to Anonymize obtained medical images stored in DICOM formats from different imaging modalities.")
    st.write("It implements the k-anonymity algorithm on the imaging datasets.")
    st.write("The MondyFly is a variant of the k-anonymity algorithms (Mondrian and Datafly).")

    st.info("This project work is dedicated to the almighty God who gives me strength and knowledge. I also dedicate this work to my parents, Mr. and Mrs. Mathias whom I strive to make proud.")