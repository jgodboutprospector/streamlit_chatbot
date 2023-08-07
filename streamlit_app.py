import streamlit as st
import re
import warnings

from chain import load_chain
from utils.snowchat_ui import message_func
from utils.snowddl import Snowddl  
from snowflake.snowpark.exceptions import SnowparkSQLException
from utils.snow_connect import SnowflakeConnection

warnings.filterwarnings("ignore")
chat_history = []

# Snowflake creds
sf_account = 'your_account'  
sf_user = 'your_user'
sf_password = 'your_password'
sf_database = 'your_db'
sf_schema = 'your_schema' 

# Connect to Snowflake
conn = snowflake.connector.connect(
    user = sf_user,
    password = sf_password,
    account = sf_account,
    database = sf_database,
    schema = sf_schema
)

# Rename app  
st.title("Prospector")
st.caption("Talk your way through data")

model = st.radio(
    "",
    options=["GPT-3.5", "LLama-2"],
    index=0,
    horizontal=True,
)

st.session_state["model"] = model

INITIAL_MESSAGE = [
    {"role": "user", "content": "Hi!"},
    {
        "role": "assistant",
        "content": "Hey there, I'm Chatty McQueryFace, your SQL-speaking sidekick, ready to chat up Snowflake and fetch answers faster than a snowball fight in summer! ❄️🔍",
    },
]

with open("ui/sidebar.md", "r") as sidebar_file:
    sidebar_content = sidebar_file.read()

with open("ui/styles.md", "r") as styles_file:
    styles_content = styles_file.read()

# Show DDL for selected table 
snow_ddl = Snowddl(conn)
selected_table = st.sidebar.selectbox(
    "Select a table:", options=list(snow_ddl.ddl_dict.keys())
)
st.sidebar.markdown(f"### DDL for {selected_table} table")
st.sidebar.code(snow_ddl.ddl_dict[selected_table], language="sql")

# Reset button
if st.sidebar.button("Reset Chat"):
    # Clear all session state
    for key in st.session_state.keys():
        del st.session_state[key]
        
    st.session_state["messages"] = INITIAL_MESSAGE
    st.session_state["history"] = []

st.write(styles_content, unsafe_allow_html=True) 

# Initialize chat history
if "messages" not in st.session_state.keys():
    st.session_state["messages"] = INITIAL_MESSAGE

if "history" not in st.session_state:
    st.session_state["history"] = []
    
if "model" not in st.session_state:
    st.session_state["model"] = model

# Get user input  
if prompt := st.chat_input():
    st.session_state.messages.append({"role": "user", "content": prompt})

# Show chat history    
for message in st.session_state.messages:
    message_func(
        message["content"],
        True if message["role"] == "user" else False,
        True if message["role"] == "data" else False,
    )

# Load model   
chain = load_chain(st.session_state["model"])

# Chat utils
def append_chat_history(question, answer):
    st.session_state["history"].append((question, answer))
    
def get_sql(text):
    sql_match = re.search(r"```sql\n(.*)\n```", text, re.DOTALL)
    return sql_match.group(1) if sql_match else None

def append_message(content, role="assistant", display=False):
    message = {"role": role, "content": content}
    message_func(content, False, display)
    st.session_state.messages.append(message)
    if role != "data":
        append_chat_history(st.session_state.messages[-2]["content"], content)

# Execute SQL queries       
def execute_sql(query, conn, retries=2):
    if re.match(r"^\s*(drop|alter|truncate|delete|insert|update)\s", query, re.I):
        append_message("Sorry, I can't execute queries that can modify the database.")
        return None
    
    try:
        return conn.sql(query).collect()
    except SnowparkSQLException as e:
        return handle_sql_exception(query, conn, e, retries)

def handle_sql_exception(query, conn, e, retries=2):
   ...
   
# Main logic
if st.session_state.messages[-1]["role"] != "assistant":
    content = st.session_state.messages[-1]["content"]
    
    if isinstance(content, str):
        result = chain(
            {"question": content, "chat_history": st.session_state["history"]}
        )["answer"]
        append_message(result)
        
        if get_sql(result):
            df = execute_sql(get_sql(result), conn) 
            if df is not None:
                append_message(df, "data", True)
