import streamlit as st
import requests
import pandas  as pd
# import json

def post_spark_job(user, repo, job, token, codeurl, dataseturl):
    # Define the API endpoint
    url = 'https://api.github.com/repos/' + user + '/' + repo + '/dispatches'
    # Define the data to be sent in the POST request
    payload = {
      "event_type": job,
      "client_payload": {
        "codeurl": codeurl,
        "dataseturl": dataseturl
      }

    }

    headers = {
      'Authorization': 'Bearer ' + token,
      'Accept': 'application/vnd.github.v3+json',
      'Content-type': 'application/json'
    }

    st.write(url)
    st.write(payload)
    st.write(headers)

    # Make the POST request
    response = requests.post(url, json=payload, headers=headers)

    # Display the response in the app
    st.write(response)

st.title("Spark & streamlit dashboard Drivers by. adrimexico1") 

st.header("spark-submit Job")

github_user  =  st.text_input('Github user', value='username')
github_repo  =  st.text_input('Github repo', value='repo')
spark_job    =  st.text_input('Spark job', value='spark')
github_token =  st.text_input('Github token', value='***')
code_url =  st.text_input('Code URL', value='')
data_set =  st.text_input('Data Set', value='')

if st.button("POST spark submit"):
    post_spark_job(github_user, github_repo, spark_job, github_token, code_url, data_set)


def get_spark_results(url_results):
    response = requests.get(url_results)
    st.write(response)

    if  (response.status_code ==  200):
        st.write(response.json())

st.header("spark-submit all or summary results")

url_results=  st.text_input('URL results', value='https://raw.githubusercontent.com/adrimexico1/bigdata-reto/refs/heads/main/results/data.json')

if st.button("GET spark results"):
    get_spark_results(url_results)

