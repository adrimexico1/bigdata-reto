import streamlit as st
import requests
import pandas  as pd
import json
import pymongo

# Initialize connection.
# Uses st.cache_resource to only run once.
@st.cache_resource
def init_connection():
    return pymongo.MongoClient(**st.secrets["mongo"])

client = init_connection()

# Pull data from the collection.
# Uses st.cache_data to only rerun when the query changes or after 10 min.
@st.cache_data(ttl=600)
def get_data():
    db = client.people
    items = db.people.find()
    items = list(items)  # make hashable for st.cache_data
    return items


# Initialize connection.
conn = st.connection("postgresql", type="sql")

def post_spark_job(user, repo, job, token):
    # Define the API endpoint
    url = 'https://api.github.com/repos/' + user + '/' + repo + '/dispatches'
    # Define the data to be sent in the POST request
    payload = {
      "event_type": job
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

def get_spark_results(url_results):
    response = requests.get(url_results)
    st.write(response)

    if  (response.status_code ==  200):
        #response_dict = json.loads('[' + response.text + ']')
        #st.write(response_dict)

        #for i in response_dict:
        #     print("key: ", i, "val: ", response_dict[i])
        st.write(response.json())
        #st.write(json.dumps(response.text, indent=4, sort_keys=True, default=lambda o:'<not serializable>'))


#else:

# Main Streamlit app

st.title("Spark & streamlit dashboard Drivers by. adrimexico1") 

st.header("spark-submit Job")

github_user  =  st.text_input('Github user', value='adrimexico1')
github_repo  =  st.text_input('Github repo', value='bigdata-reto')
spark_job    =  st.text_input('Spark job', value='spark')
github_token =  st.text_input('Github token', value='***')
code_url =  st.text_input('Code URL', value='https://raw.githubusercontent.com/adrimexico1/bigdata-reto/refs/heads/main/spark/spark.py')
data_set =  st.text_input('Data Set', value='https://raw.githubusercontent.com/adrimexico1/drivers/refs/heads/main/drivers.csv')

if st.button("POST spark submit"):
    post_spark_job(github_user, github_repo, spark_job, github_token)


st.header("spark-submit results")

url_results=  st.text_input('URL results', value='https://raw.githubusercontent.com/adrimexico1/bigdata-reto/refs/heads/main/results/data.json')

if st.button("GET spark results"):
    get_spark_results(url_results)


if st.button("Query mongodb collection"):
    items = get_data()

    for item in items:
        # Convertir la cadena JSON en un diccionario
        item_data = json.loads(item["data"])
        
        # Mostrar en Streamlit
        st.write(f"{item_data['name']} : {item_data['birth']}")
        st.write(f"{item_data['driverRef']} : {item_data['number']}")

if st.button("Query Postgresql table"):
    # Perform query.
    df = conn.query('SELECT * FROM drivers;', ttl="10m")
    # Print results.
    for row in df.itertuples():
        st.write(row)
