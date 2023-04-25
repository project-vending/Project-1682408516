
import streamlit as st
import pandas as pd

# Import the scraped data
df = pd.read_csv('data/raw_data.csv')

st.title('Web Scraper and Analyzer')

# Show the raw data
st.header('Raw Data')
st.write(df)

# Perform basic analysis of the scraped data
st.header('Analysis')
st.write('Number of rows: ', len(df))
st.write('Average response time (ms): ', df['response_time'].mean())
st.write('Most common status code: ', df['status_code'].mode().iloc[0])
