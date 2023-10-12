import os
import toml
import getpass
import threading
import pandas as pd
import streamlit as st
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
import warnings
warnings.filterwarnings('ignore', category=FutureWarning, module='pyarrow.pandas_compat')

st.title("Snowloader")
st.write(
    """Drag and Drop the ***CSV*** or ***Excel file***, you want to load into snowflake,
    or select it using the ***Browse files*** button.
        """
)
# Initialize session state for form visibility
if "show_form" not in st.session_state:
    st.session_state.show_form = False

# Add a button to toggle the visibility of the form
if st.button("Snowflake Connection Details"):
    st.session_state.show_form = not st.session_state.show_form
col1, col2 = st.columns(2)

# Load existing configuration from file
try:
    config = toml.load("snowflake_config.toml")
except FileNotFoundError:
    st.error("Configuration file not found.")
    config = {}

os.environ["user"] = f"{getpass.getuser()}@myob.com"

if st.session_state.show_form:
    with col1.form("snowflake_config_form"):
        try:
            user = st.text_input("User", value=f"{getpass.getuser()}@myob.com")
            role = st.text_input(
                "Role", value=config.get("Snowflake", {}).get("role", "")
            )
            warehouse = st.text_input(
                "Warehouse", value=config.get("Snowflake", {}).get("warehouse", "")
            )
            database = st.text_input(
                "Database", value=config.get("Snowflake", {}).get("database", "")
            )
            schema = st.text_input(
                "Schema", value=config.get("Snowflake", {}).get("schema", "")
            )
        except Exception as e:
            st.error(f"An error occurred: {e}")

        if st.form_submit_button("Save"):
            try:
                config["Snowflake"] = {
                    "account": "bu20658.ap-southeast-2",
                    "user": user,
                    "role": role,
                    "warehouse": warehouse,
                    "database": database,
                    "schema": schema,
                    "authenticator": "externalbrowser",
                }

                with open("snowflake_config.toml", "w") as configfile:
                    toml.dump(config, configfile)

                st.success("Connection details saved!")
            except Exception as e:
                st.error(f"An error occurred while saving the configuration: {e}")


try:
    # Setup Snowflake connection
    if "Snowflake" in config:
        snowflake_config = {
            "account": "bu20658.ap-southeast-2",
            "user": config["Snowflake"].get("user", os.environ["user"]),
            "role": config["Snowflake"].get("role"),
            "warehouse": config["Snowflake"].get("warehouse"),
            "database": config["Snowflake"].get("database"),
            "schema": config["Snowflake"].get("schema"),
            "authenticator": config["Snowflake"].get("authenticator"),
            "CLIENT_SESSION_KEEP_ALIVE": config["Snowflake"].get("client_session_keep_alive")
            }
except Exception as e:
    st.error(f"An error occurred while setting up the Snowflake connection: {e}")


def snowflake_upload_operation(table_name, df, snowflake_config, results):
    try:
        print("Starting snowflake_upload_operation")  # Debugging print statement
        session = Session.builder.configs(snowflake_config).create()
        tables = session.sql(f"SHOW TABLES LIKE '{table_name}' IN SCHEMA {snowflake_config['schema']}").collect()

        if tables:  # If table already exists
            results["exists"] = True
        else:  # If table does not exist
            session.write_pandas(df, table_name, auto_create_table=True, overwrite=True)
            results["success"] = True
        print("Completed snowflake_upload_operation")  # Debugging print statement
    except Exception as e:
        results["error"] = str(e)

with col2:
    try:
        # Streamlit UI for file upload
        uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=["csv"])

        if uploaded_file:
            file_type = uploaded_file.name.split(".")[-1]

            # Check for supported file types
            if file_type not in ["csv"]:
                st.error("File type not supported.")

            default_table_name = uploaded_file.name.split(".")[0]
            table_name = st.text_input("Table Name:", default_table_name)

            if file_type == "csv":
                df = pd.read_csv(uploaded_file)
            else:
                st.error("File type not supported.")

            st.write("Preview of Data:")
            st.write(df.head())

            if st.button("Upload to Snowflake"):
                if table_name:
                    print("Upload to Snowflake button pressed")  # Debugging print statement                
                    results = {"exists": False, "success": False, "error": None}
                    thread = threading.Thread(target=snowflake_upload_operation, args=(table_name, df, snowflake_config, results))
                    thread.start()
                    thread.join()
                    if results["error"]:
                        st.error(f"An error occurred: {results['error']}")
                    # elif results[["name 'table_name' is not defined"]]:
                    #     st.warning("Please enter a table name.")
                    elif results["exists"]:
                        if "confirmed_overwrite" not in st.session_state:
                            if st.button("Table exists! If you want to overwrite, click here, otherwise change the file's name."):
                                st.session_state.confirmed_overwrite = True
                                st.success(f"Uploaded data to Snowflake table {table_name}.")
                if results["success"]:
                    st.success(f"Uploaded data to Snowflake table {table_name}.")
            
    except Exception as e:
        st.error(f"An error occurred: {e}")