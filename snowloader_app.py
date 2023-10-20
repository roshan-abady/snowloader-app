import os
import toml
import getpass
import warnings
import threading
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session


warnings.filterwarnings('ignore', category=FutureWarning, module='pyarrow.pandas_compat')


def load_config():
    try:
        return toml.load("config.toml")
    except FileNotFoundError:
        st.warning("Configuration file not found.\n \n Defaults will be used.\n \n Alternatively, create a config.toml file by saving your connection details.")
        return {}

def save_config(data):
    with open("config.toml", "w") as configfile:
        toml.dump(data, configfile)

def setup_snowflake_config(config):
    # If "Snowflake" section is missing, set defaults.
    if "Snowflake" not in config:
        config["Snowflake"] = {
            "account": "bu20658.ap-southeast-2",
            "user": f"{getpass.getuser()}@myob.com",
            "role": "OPERATIONS_ANALYTICS_DATA_LOADER",
            "warehouse": "OPERATIONS_ANALYTICS_WAREHOUSE_TEST",
            "database": "OPERATIONS_ANALYTICS",
            "schema": "RAW",
            "authenticator": "externalbrowser",
        }
    
    snowflake_data = config["Snowflake"]

    return {
        "account": snowflake_data["account"],
        "user": snowflake_data["user"],
        "role": snowflake_data["role"],
        "warehouse": snowflake_data["warehouse"],
        "database": snowflake_data["database"],
        "schema": snowflake_data["schema"],
        "authenticator": snowflake_data["authenticator"]
    }



def show_connection_form(config):
    with st.form("snowflake_config_form"):
        # account = st.text_input("Account", value=config.get("Snowflake", {}).get("account", ""))
        user = st.text_input("User", value=config.get("Snowflake", {}).get("user", ""))
        role = st.text_input("Role", value=config.get("Snowflake", {}).get("role", ""))
        warehouse = st.text_input("Warehouse", value=config.get("Snowflake", {}).get("warehouse", ""))
        database = st.text_input("Database", value=config.get("Snowflake", {}).get("database", ""))
        schema = st.text_input("Schema", value=config.get("Snowflake", {}).get("schema", ""))
        # authenticator = st.text_input("Authenticator", value=config.get("Snowflake", {}).get("authenticator", ""))
        if st.form_submit_button("Save"):
            config["Snowflake"] = {
                "account": "bu20658.ap-southeast-2",
                "user": user,
                "role": role,
                "warehouse": warehouse,
                "database": database,
                "schema": schema,
                "authenticator": "externalbrowser"
            }
            save_config(config)
            st.success("Connection details saved!")


def main():
    st.title("Snowloader")
    st.write(
        """Drag and Drop the ***CSV*** file you want to load into Snowflake, or select it using the ***Browse files*** button."""
    )

    # Toggle connection details form
    if st.button("Snowflake Connection Details"):
        st.session_state.show_form = not st.session_state.get("show_form", False)

    # Load configuration
    config = load_config()
    snowflake_config = setup_snowflake_config(config)

    col1, col2 = st.columns(2)  # Splitting the layout into two columns

    # Use the left column for the connection form
    with col1:
        if st.session_state.get("show_form", False):
            show_connection_form(config)

    # Use the right column for the file upload and operations
    with col2:
        # Streamlit UI for file upload
        uploaded_file = st.file_uploader("Choose a CSV file", type=["csv"])

        if uploaded_file:
            file_type = uploaded_file.name.split(".")[-1]

            # Check for supported file types
            if file_type not in ["csv"]:
                st.error("File type not supported.")

            default_table_name = uploaded_file.name.split(".")[0]
            table_name = st.text_input("Table Name:", default_table_name)

            df = pd.read_csv(uploaded_file)

            st.write("Preview of Data:")
            st.write(df.head())

            if st.button("Upload to Snowflake"):
                # (Your Snowflake upload logic would go here)

                st.success(f"Uploaded data to Snowflake table {table_name}.")


if __name__ == "__main__":
    main()
