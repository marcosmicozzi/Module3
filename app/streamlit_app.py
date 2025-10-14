import streamlit as st    
import psycopg 
import pandas as pd

def get_data(crypto):
    dbconn = st.secrets["DB_CONN"]
    conn = psycopg.connect(dbconn)
    cur = conn.cursor()

    cur.execute('SELECT * FROM btc_data')
    data = cur.fetchall()

    conn.commit()
    cur.close()
    conn.close()

    return pd.DataFrame(data, columns=["date", "open", "close"])


# st.badge("Hello Trader", icon="🍉", color="violet")

# option = st.selectbox(
#     "Which Crypto would oyu like to know the price for?",
#     ("BTC", "ETH", "SOL"),
#     index=None
# )

# st.write("You selected: ", option)

# if option != None:
#     data = get_data(option)
#     st.dataframe(data)