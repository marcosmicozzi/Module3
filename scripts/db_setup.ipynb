{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 39,
   "metadata": {},
   "outputs": [],
   "source": [
    "import requests\n",
    "import pandas as pd\n",
    "from datetime import datetime\n",
    "from dotenv import load_dotenv\n",
    "import os\n",
    "import psycopg\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 23,
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "python-dotenv could not parse statement starting at line 2\n"
     ]
    }
   ],
   "source": [
    "load_dotenv()\n",
    "dbconn = os.getenv(\"DB_CONN\")\n",
    "\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Update CSV"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 36,
   "metadata": {},
   "outputs": [],
   "source": [
    "df_sentiment = pd.read_csv(\"sentiment_data.csv\")\n",
    "\n",
    "# Create a zero-padded ID column: BTC0001, BTC0002, ...\n",
    "df_sentiment[\"article_id\"] = [\"BTC\" + str(i+1).zfill(4) for i in range(len(df_sentiment))]\n",
    "df_sentiment[\"sentiment_score\"] = None\n",
    "\n",
    "df_sentiment.to_csv(\"sentiment_data.csv\", index=False)"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Erase tables"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": [
    "# def erase_tables():\n",
    "#     conn = psycopg.connect(dbconn)\n",
    "#     cur = conn.cursor()\n",
    "\n",
    "#     cur.execute(\"DROP TABLE IF EXISTS btc_data, btc_articles, btc_sentiment;\")\n",
    "#     conn.commit()\n",
    "#     cur.close()\n",
    "#     conn.close()\n",
    "\n",
    "# erase_tables()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Create Tables"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 44,
   "metadata": {},
   "outputs": [],
   "source": [
    "def create_btc_table():\n",
    "    conn = psycopg.connect(dbconn)\n",
    "    cur = conn.cursor()\n",
    "    cur.execute(\n",
    "        '''\n",
    "            CREATE TABLE IF NOT EXISTS btc_data(\n",
    "                date TIMESTAMP PRIMARY KEY,\n",
    "                open FLOAT,\n",
    "                close FLOAT,\n",
    "                high FLOAT,\n",
    "                low FLOAT,\n",
    "                volume FLOAT\n",
    "            );\n",
    "        '''\n",
    "    )\n",
    "    conn.commit()\n",
    "    cur.close()\n",
    "    conn.close()\n",
    "\n",
    "def create_btc_articles_table():\n",
    "    conn = psycopg.connect(dbconn)\n",
    "    cur = conn.cursor()\n",
    "    cur.execute(\n",
    "        '''\n",
    "            CREATE TABLE IF NOT EXISTS btc_articles(\n",
    "                article_id TEXT PRIMARY KEY,\n",
    "                title TEXT,\n",
    "                author TEXT,\n",
    "                date TIMESTAMP\n",
    "            );\n",
    "        '''\n",
    "    )\n",
    "    conn.commit()\n",
    "    cur.close()\n",
    "    conn.close()\n",
    "\n",
    "def create_btc_sentiment_table():\n",
    "    conn = psycopg.connect(dbconn)\n",
    "    cur = conn.cursor()\n",
    "    cur.execute(\n",
    "        '''\n",
    "            CREATE TABLE IF NOT EXISTS btc_sentiment(\n",
    "                article_id TEXT PRIMARY KEY,\n",
    "                sentiment_score TEXT,\n",
    "                date TIMESTAMP\n",
    "            );\n",
    "        '''\n",
    "    )\n",
    "    conn.commit()\n",
    "    cur.close()\n",
    "    conn.close()\n",
    "\n",
    "\n",
    "\n",
    "create_btc_table()\n",
    "create_btc_articles_table()\n",
    "create_btc_sentiment_table()\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Populate Tables"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [
    {
     "ename": "UniqueViolation",
     "evalue": "duplicate key value violates unique constraint \"btc_articles_pkey\"\nDETAIL:  Key (article_id)=(sdgsd) already exists.",
     "output_type": "error",
     "traceback": [
      "\u001b[0;31m---------------------------------------------------------------------------\u001b[0m",
      "\u001b[0;31mUniqueViolation\u001b[0m                           Traceback (most recent call last)",
      "Cell \u001b[0;32mIn[45], line 36\u001b[0m\n\u001b[1;32m     33\u001b[0m conn \u001b[38;5;241m=\u001b[39m psycopg\u001b[38;5;241m.\u001b[39mconnect(dbconn)\n\u001b[1;32m     34\u001b[0m cur \u001b[38;5;241m=\u001b[39m conn\u001b[38;5;241m.\u001b[39mcursor()\n\u001b[0;32m---> 36\u001b[0m \u001b[43mcur\u001b[49m\u001b[38;5;241;43m.\u001b[39;49m\u001b[43mexecute\u001b[49m\u001b[43m(\u001b[49m\n\u001b[1;32m     37\u001b[0m \u001b[38;5;250;43m    \u001b[39;49m\u001b[38;5;124;43;03m'''\u001b[39;49;00m\n\u001b[1;32m     38\u001b[0m \u001b[38;5;124;43;03m        INSERT INTO btc_articles(date, title, author, article_id)\u001b[39;49;00m\n\u001b[1;32m     39\u001b[0m \u001b[38;5;124;43;03m        VALUES (%s, %s, %s, %s);\u001b[39;49;00m\n\u001b[1;32m     40\u001b[0m \u001b[38;5;124;43;03m    '''\u001b[39;49;00m\u001b[43m,\u001b[49m\u001b[43m \u001b[49m\n\u001b[1;32m     41\u001b[0m \u001b[43m    \u001b[49m\u001b[43m(\u001b[49m\u001b[43mdatetime\u001b[49m\u001b[38;5;241;43m.\u001b[39;49m\u001b[43mstrptime\u001b[49m\u001b[43m(\u001b[49m\u001b[38;5;124;43m\"\u001b[39;49m\u001b[38;5;124;43m2025-02-20\u001b[39;49m\u001b[38;5;124;43m\"\u001b[39;49m\u001b[43m,\u001b[49m\u001b[43m \u001b[49m\u001b[38;5;124;43m\"\u001b[39;49m\u001b[38;5;124;43m%\u001b[39;49m\u001b[38;5;124;43mY-\u001b[39;49m\u001b[38;5;124;43m%\u001b[39;49m\u001b[38;5;124;43mm-\u001b[39;49m\u001b[38;5;132;43;01m%d\u001b[39;49;00m\u001b[38;5;124;43m\"\u001b[39;49m\u001b[43m)\u001b[49m\u001b[43m,\u001b[49m\u001b[43m \u001b[49m\u001b[38;5;124;43m'\u001b[39;49m\u001b[38;5;124;43maasgfasf\u001b[39;49m\u001b[38;5;124;43m'\u001b[39;49m\u001b[43m,\u001b[49m\u001b[43m \u001b[49m\u001b[38;5;124;43m'\u001b[39;49m\u001b[38;5;124;43msdgsdg\u001b[39;49m\u001b[38;5;124;43m'\u001b[39;49m\u001b[43m,\u001b[49m\u001b[43m \u001b[49m\u001b[38;5;124;43m'\u001b[39;49m\u001b[38;5;124;43msdgsd\u001b[39;49m\u001b[38;5;124;43m'\u001b[39;49m\u001b[43m)\u001b[49m\n\u001b[1;32m     42\u001b[0m \u001b[43m)\u001b[49m\n\u001b[1;32m     44\u001b[0m conn\u001b[38;5;241m.\u001b[39mcommit()\n\u001b[1;32m     45\u001b[0m cur\u001b[38;5;241m.\u001b[39mclose()\n",
      "File \u001b[0;32m/opt/anaconda3/envs/MarcosPython10/lib/python3.10/site-packages/psycopg/cursor.py:97\u001b[0m, in \u001b[0;36mCursor.execute\u001b[0;34m(self, query, params, prepare, binary)\u001b[0m\n\u001b[1;32m     93\u001b[0m         \u001b[38;5;28mself\u001b[39m\u001b[38;5;241m.\u001b[39m_conn\u001b[38;5;241m.\u001b[39mwait(\n\u001b[1;32m     94\u001b[0m             \u001b[38;5;28mself\u001b[39m\u001b[38;5;241m.\u001b[39m_execute_gen(query, params, prepare\u001b[38;5;241m=\u001b[39mprepare, binary\u001b[38;5;241m=\u001b[39mbinary)\n\u001b[1;32m     95\u001b[0m         )\n\u001b[1;32m     96\u001b[0m \u001b[38;5;28;01mexcept\u001b[39;00m e\u001b[38;5;241m.\u001b[39m_NO_TRACEBACK \u001b[38;5;28;01mas\u001b[39;00m ex:\n\u001b[0;32m---> 97\u001b[0m     \u001b[38;5;28;01mraise\u001b[39;00m ex\u001b[38;5;241m.\u001b[39mwith_traceback(\u001b[38;5;28;01mNone\u001b[39;00m)\n\u001b[1;32m     98\u001b[0m \u001b[38;5;28;01mreturn\u001b[39;00m \u001b[38;5;28mself\u001b[39m\n",
      "\u001b[0;31mUniqueViolation\u001b[0m: duplicate key value violates unique constraint \"btc_articles_pkey\"\nDETAIL:  Key (article_id)=(sdgsd) already exists."
     ]
    }
   ],
   "source": [
    "from datetime import datetime\n",
    "\n",
    "df_btc = pd.read_csv(\"btc_data.csv\")\n",
    "\n",
    "\n",
    "conn = psycopg.connect(dbconn)\n",
    "cur = conn.cursor()\n",
    "\n",
    "cur.execute(\n",
    "    '''\n",
    "        INSERT INTO btc_data(date, open, close, high, low, volume)\n",
    "        VALUES (%s, %s, %s, %s, %s, %s);\n",
    "    ''', \n",
    "    (datetime.strptime(\"2025-02-20\", \"%Y-%m-%d\"), 92676.24, 92680.67, 92630.67, 92689.67, 92380.67)\n",
    ")\n",
    "\n",
    "conn.commit()\n",
    "cur.close()\n",
    "conn.close()\n",
    "\n",
    "conn = psycopg.connect(dbconn)\n",
    "cur = conn.cursor()\n",
    "\n",
    "cur.execute(\n",
    "    '''\n",
    "        INSERT INTO btc_articles(date, title, author, article_id)\n",
    "        VALUES (%s, %s, %s, %s);\n",
    "    ''', \n",
    "    (datetime.strptime(\"2025-02-20\", \"%Y-%m-%d\"), 'aasgfasf', 'sdgsdg', 'sdgsd')\n",
    ")\n",
    "\n",
    "conn.commit()\n",
    "cur.close()\n",
    "conn.close()\n",
    "\n",
    "conn = psycopg.connect(dbconn)\n",
    "cur = conn.cursor()\n",
    "\n",
    "cur.execute(\n",
    "    '''\n",
    "        INSERT INTO btc_articles(date, title, author, article_id)\n",
    "        VALUES (%s, %s, %s, %s);\n",
    "    ''', \n",
    "    (datetime.strptime(\"2025-02-20\", \"%Y-%m-%d\"), 'aasgfasf', 'sdgsdg', 'sdgsd')\n",
    ")\n",
    "\n",
    "conn.commit()\n",
    "cur.close()\n",
    "conn.close()"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {},
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "MarcosPython10",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.10.18"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
