from requests import Session
from requests.auth import HTTPBasicAuth  # or HTTPDigestAuth, or OAuth1, etc.
from zeep import Client
from zeep.transports import Transport
import datetime
import pandas as pd
import re

def MetricsDataService_Conn(mom, user, password):
    session = Session()
    session.auth = HTTPBasicAuth(user, password)
    client = Client('http://'+mom+'/introscope-web-services/services/MetricsDataService?wsdl',
    transport=Transport(session=session))
    return client

def historicoPorFrontend(client, frontend, from_ts, to_ts, resolution=60, health_only=True):
    if health_only: 
        art = client.service.getMetricData("(.*)\\|Custom Metric Process \\(Virtual\\)\\|Custom Business Application Agent \\(Virtual\\)","By Frontend\\|"+ frontend+"\\|Health:Average Response Time \\(ms\\)", from_ts, to_ts, resolution)
        epi = client.service.getMetricData("(.*)\\|Custom Metric Process \\(Virtual\\)\\|Custom Business Application Agent \\(Virtual\\)","By Frontend\\|"+ frontend+"\\|Health:Errors Per Interval", from_ts, to_ts, resolution)
        rpi = client.service.getMetricData("(.*)\\|Custom Metric Process \\(Virtual\\)\\|Custom Business Application Agent \\(Virtual\\)","By Frontend\\|"+ frontend+"\\|Health:Responses Per Interval", from_ts, to_ts, resolution)
    else:
        art = client.service.getMetricData("(.*)","Frontends\\|Apps\\|"+ frontend+ ":Average Response Time \\(ms\\)", from_ts, to_ts, resolution)
        epi = client.service.getMetricData("(.*)","Frontends\\|Apps\\|"+ frontend+":Errors Per Interval", from_ts, to_ts, resolution)
        rpi = client.service.getMetricData("(.*)","Frontends\\|Apps\\|"+ frontend+":Responses Per Interval", from_ts, to_ts, resolution)

    art_r = {'ts':[], 'art':[]}
    for rec in art:
        for data in rec['metricData']:
            art_r['art'].append(int(0) if data['metricValue'] == None else int(data['metricValue']))
            art_r['ts'].append(rec['timesliceStartTime'])
    
    df_art = pd.DataFrame(art_r)

    rpi_r = {'ts':[], 'rpi':[]}
    for rec in rpi:
        for data in rec['metricData']:
            rpi_r['rpi'].append(int(0) if data['metricValue'] == None else int(data['metricValue']))
            rpi_r['ts'].append(rec['timesliceStartTime'])
    
    df_rpi = pd.DataFrame(rpi_r)
    
    epi_r = {'ts':[], 'epi':[]}
    for rec in epi:
        for data in rec['metricData']:
            epi_r['epi'].append(int(0) if data['metricValue'] == None else int(data['metricValue']))
            epi_r['ts'].append(rec['timesliceStartTime'])
    
    df_epi = pd.DataFrame(epi_r)

    dataframe = df_art.merge(df_rpi, on='ts')
    dataframe = dataframe.merge(df_epi, on='ts')
    dataframe.index = dataframe['ts']
    dataframe['rate'] = round(dataframe['epi']/dataframe['rpi']*100, 2)
    dataframe['epi_per_period'] = round(dataframe['epi']/240)
    dataframe['rpi_per_period'] = round(dataframe['rpi']/240)
    dataframe['hr'] =  dataframe['ts'].dt.hour
    dataframe['wday'] =  dataframe['ts'].dt.weekday
    dataframe['day'] =  dataframe['ts'].dt.day
    dataframe = dataframe[['day','wday','hr','art','rpi','epi', 'rate','epi_per_period','rpi_per_period']]
    

    return dataframe

def historicoPorRegex(client, agent_regex, metric_regex, from_ts, to_ts,  layer, resolution=60):
    
    art = client.service.getMetricData(agent_regex,metric_regex, from_ts, to_ts, resolution)
    epi = client.service.getMetricData("(.*)\\|WebSphere\\|(.*)(SVC_wasfec|FEC_SVC)(.*)","WebServices\|Server\|http_//impl\.webservice\.fec\.app\.bsbr\.altec\.com/\|(.*):Responses Per Interval", from_ts, to_ts, resolution)
    rpi = client.service.getMetricData("(.*)\\|WebSphere\\|(.*)(SVC_wasfec|FEC_SVC)(.*)","WebServices\|Server\|http_//impl\.webservice\.fec\.app\.bsbr\.altec\.com/\|(.*):Errors Per Interval", from_ts, to_ts, resolution)
    
    art_r = {'metricName':[], 'ts':[], 'art':[]}
    for rec in art:
        for data in rec['metricData']:
            art_r['art'].append(int(0) if data['metricValue'] == None else int(data['metricValue']))
            art_r['ts'].append(rec['timesliceStartTime'])
            art_r['metricName'].append(data['metricName'])
    
    df_art = pd.DataFrame(art_r)

    rpi_r = {'metricName':[], 'ts':[], 'rpi':[]}
    for rec in rpi:
        for data in rec['metricData']:
            rpi_r['rpi'].append(int(0) if data['metricValue'] == None else int(data['metricValue']))
            rpi_r['ts'].append(rec['timesliceStartTime'])
            rpi_r['metricName'].append(data['metricName'])
    
    df_rpi = pd.DataFrame(rpi_r)
    
    epi_r = {'metricName':[], 'ts':[], 'epi':[]}
    for rec in epi:
        for data in rec['metricData']:
            epi_r['epi'].append(int(0) if data['metricValue'] == None else int(data['metricValue']))
            epi_r['ts'].append(rec['timesliceStartTime'])
            epi_r['metricName'].append(data['metricName'])
    
    df_epi = pd.DataFrame(epi_r)

    dataframe = df_art.merge(df_rpi, on='metricName')
    dataframe = dataframe.merge(df_epi, on='metricName')
    dataframe.index = dataframe['ts']
    dataframe['rate'] = round(dataframe['epi']/dataframe['rpi']*100, 2)
    dataframe['epi_per_period'] = round(dataframe['epi']/240)
    dataframe['rpi_per_period'] = round(dataframe['rpi']/240)
    dataframe['hr'] =  dataframe['ts'].dt.hour
    dataframe['wday'] =  dataframe['ts'].dt.weekday
    dataframe['day'] =  dataframe['ts'].dt.day
    dataframe = dataframe[['metricName', 'day','wday','hr','art','rpi','epi', 'rate','epi_per_period','rpi_per_period']]
    

    return dataframe
