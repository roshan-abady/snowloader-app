import streamlit as st
import pandas as pd
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
import configparser
import getpass
import os

st.title("Snowloader")
st.write(
        """Drag and Drop the ***CSV*** or ***Excel file***, you want to load into snowflake,
    or select it using the ***Browse files*** button.
        """
    )
# Initialize session state for form visibility
if 'show_form' not in st.session_state:
    st.session_state.show_form = True

# Add a button to toggle the visibility of the form
if st.button('Snowflake Connection Details'):
    st.session_state.show_form = not st.session_state.show_form
col1, col2 = st.columns(2)

# Load existing configuration from file
config = configparser.ConfigParser()
config.read('snowflake_config.ini')

os.environ['user'] = f'{getpass.getuser()}@myob.com'

if st.session_state.show_form:
    try:
        with col1.form("snowflake_config_form"):
            user = st.text_input("User", value=f'{getpass.getuser()}@myob.com')
            role = st.text_input("Role", value=config.get('Snowflake', 'role', fallback=''))
            warehouse = st.text_input("Warehouse", value=config.get('Snowflake', 'warehouse', fallback=''))
            database = st.text_input("Database", value=config.get('Snowflake', 'database', fallback=''))
            schema = st.text_input("Schema", value=config.get('Snowflake', 'schema', fallback=''))
            
            if st.form_submit_button("Save"):
                config['Snowflake'] = {
                    'account': 'bu20658.ap-southeast-2',
                    'user': user,
                    'role': role,
                    'warehouse': warehouse,
                    'database': database,
                    'schema': schema,
                    'authenticator': 'externalbrowser'
                }
                
                with open('snowflake_config.ini', 'w') as configfile:
                    config.write(configfile)
                
                st.success("Connection details saved!")
    except Exception as e:
        st.error(f"An error occurred: {e}")
try:            
    # Setup Snowflake connection
    if config.has_section('Snowflake'):
        snowflake_config = {
            'account': 'bu20658.ap-southeast-2',
            'user': config.get('Snowflake', 'user'),
            'role': config.get('Snowflake', 'role'),
            'warehouse': config.get('Snowflake', 'warehouse'),
            'database': config.get('Snowflake', 'database'),
            'schema': config.get('Snowflake', 'schema'),
            'authenticator': 'externalbrowser'
        }
except Exception as e:
    st.error(f"An error occurred while setting up the Snowflake connection: {e}")
    
with col2:
    try:
        # Streamlit UI for file upload
        uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv", "xlsx"])

        if uploaded_file:
            file_type = uploaded_file.name.split('.')[-1]
            
            # Check for supported file types
            if file_type not in ['csv', 'xlsx']:
                st.error("File type not supported.")
                proceed = False  # Set flag to False

            default_table_name = uploaded_file.name.split('.')[0]  # Default table name is the file name without extension
            table_name = st.text_input("Table Name:", default_table_name)  # Editable text box for table name

            if file_type == 'csv':
                df = pd.read_csv(uploaded_file)
            elif file_type == 'xlsx':
                df = pd.read_excel(uploaded_file)

            st.write("Preview of Data:")
            st.write(df.head())
            
            # Upload to Snowflake
            if st.button("Upload to Snowflake"):
                if table_name:  # Check if table_name is not empty
                    session = Session.builder.configs(snowflake_config).create()
                    snowparkDf = session.write_pandas(df, table_name, auto_create_table=True, overwrite=True)
                    st.success(f"Uploaded data to Snowflake table {table_name}.")
                else:
                    st.error("Table name is required.")
    except Exception as e:
        st.error(f"An error occurred: {e}")