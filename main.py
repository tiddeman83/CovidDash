import pandas as pd
import pyarrow
import datetime as datetime
from datetime import timedelta
import matplotlib.pyplot as plt
import streamlit as st
import os
import schedule 
import time


#specification of the dataset
rivm_gemeente = 'https://data.rivm.nl/covid-19/COVID-19_aantallen_gemeente_per_dag.json'
zkh = 'https://lcps.nu/wp-content/uploads/covid-19-datafeed.csv' 
    
          
def load_data():
    df = pd.read_parquet('updated_nl_covid_cases.parquet')
    return df

        
def today_reported():
    data = load_data()
    today = datetime.date.today().strftime('%Y-%m-%d')
    report_date = data['Date_of_publication'].max()
    print(report_date)
    if today > report_date:
        today = data['Date_of_publication'].max()
        today_data = data.loc[data['Date_of_publication'] == today]
        today_group = today_data.groupby(['Date_of_publication']).sum(['Total_reported'])
    
    elif today == report_date:
        today_data = data.loc[data['Date_of_publication'] == today]
        today_group = today_data.groupby(['Date_of_publication']).sum(['Total_reported'])

    return today_group

def hospital_information():
    data = pd.read_csv(zkh, sep=',', encoding=' utf-8')
    data['Datum'] = pd.to_datetime(data['Datum'], format='%d-%m-%Y')
    start_date = (datetime.datetime.today() - timedelta(45)).strftime('%Y-%m-%d')
    data = data.loc[data['Datum'] >= start_date]
    data = data.fillna(0)
    data = data.rename(columns={'Datum':'index'}).set_index('index')
    print(data)
    return data


def daily_information():
    df = load_data()
    df_report = df.groupby(['Date_of_publication']).sum(['Total reported'])
    
    return df_report


def information_per_municipality():
    df = load_data()
    start_date = (datetime.datetime.today() - timedelta(45)).strftime('%Y-%m-%d')
    df = df[df['Date_of_publication'] > start_date]
    return df



#Here starts the vis part of this script



header_container = st.container()
data_container = st.container()

with header_container:
    st.title('Familie Baas Covid-19 update app')
    st.write('As we are still interested in the status of the number of registred positive Covid-19 tests in NL, \
        we still keep track of these numbers.')
with data_container:
    st.write(today_reported())
    st.header('NL Infection charts')
    st.write('Below the graphs for the past 45 days.')
    #make slicer per municipality
    data = information_per_municipality()
    location = ['All'] + data['Municipality_name'].unique().tolist()
    municipality = st.selectbox('Selecteer een gemeente', location, key='gemeente')
    st.write('Number of reported C19 positive tests last 45 days for ' + str(municipality))
    if municipality != 'All':
        show_grouper = data[data['Municipality_name'] == municipality]
        display_data = show_grouper.groupby(['Date_of_publication']).sum(['Total_reported'])
        display_data['Avg7days_reported'] = display_data['Total_reported'].rolling(7).mean()
        display_data['Avg3days_reported'] = display_data['Total_reported'].rolling(3).mean()
    else:
        show_grouper = data.copy()
        display_data = show_grouper.groupby(['Date_of_publication']).sum(['Total_reported'])
        display_data['Avg7days_reported'] = display_data['Total_reported'].rolling(7).mean()
        display_data['Avg3days_reported'] = display_data['Total_reported'].rolling(3).mean()
    st.line_chart(display_data)
    st.write(display_data)
    st.header('Hospital data')
    st.write('Below the hospital information for the total of the Dutch Hospitals')
    
    show_hospital = st.selectbox('Select type of information', ['Hospital occupation', 'Hospital new addmissions'])
    data_hospital = pd.DataFrame()
    data_hospital = hospital_information()   
    if show_hospital == 'Hospital occupation':
        data = data_hospital.drop(columns=['IC_Nieuwe_Opnames_COVID_Nederland', 'Kliniek_Nieuwe_Opnames_COVID_Nederland'])
        st.area_chart(data)
    else:
        data = data_hospital.drop(columns=['IC_Bedden_COVID_Nederland', 
                                            'IC_Bedden_COVID_Internationaal',
                                            'IC_Bedden_Non_COVID_Nederland', 
                                            'Kliniek_Bedden_Nederland'])
        st.area_chart(data)


