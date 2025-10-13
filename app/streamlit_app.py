import streamlit as st    
import psycopg 




def get_data():
    dbconn = st.secrets["DB_CONN"]
    conn = psycopg.connect(dbconn)
    cur = conn.cursor()

    cur.execute('SELECT * FROM btc_data')
    data = cur.fetchall()
    print(data)
