import streamlit as st    
import psycopg 
import pandas as pd

def get_data(crypto):
    dbconn = st.secrets["DB_CONN"]
    conn = psycopg.connect(dbconn)
    cur = conn.cursor()

    cur.execute('SELECT * FROM btc_data')
    data = cur.fetchall()

    columns = ["date", "open", "high", "low", "close", "volume"]  # adjust as needed


    conn.commit()
    cur.close()
    conn.close()

    return pd.DataFrame(data, columns=columns)


st.badge("Hello Trader", icon="🍉", color="violet")

option = st.selectbox(
    "Which Crypto would oyu like to know the price for?",
    ("BTC", "ETH", "SOL"),
    index=None
)

st.write("You selected: ", option)

if option != None:
    data = get_data(option)
    st.dataframe(data)

    st.subheader("📈 Opening Price - Last 7 Days")
    st.line_chart(data, x='date', y='open')