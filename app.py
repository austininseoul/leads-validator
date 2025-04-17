import streamlit as st
from streamlit_autorefresh import st_autorefresh # Import the autorefresh component
import requests
import pandas as pd
import urllib.parse # Import for URL encoding
import psycopg2
from sqlalchemy import create_engine, text # Import text
import json # Import json

# Secrets are loaded from .streamlit/secrets.toml

# Initialize database engine using secrets
engine = create_engine(st.secrets.database.connection_string)

# --- Helper Functions ---
def load_csv():
    # Use a unique key for the file uploader
    uploaded_file = st.file_uploader("Upload CSV", type=["csv"], key="csv_uploader")
    if uploaded_file:
        # Store the file object in session state for potential saving
        st.session_state.uploaded_file_object = uploaded_file
        # Read into DataFrame for display purposes
        try:
            uploaded_file.seek(0) # Reset pointer after reading for display
            df = pd.read_csv(uploaded_file)
            uploaded_file.seek(0) # Reset pointer again for potential saving
            return df
        except Exception as e:
            st.error(f"Error reading uploaded CSV: {e}")
            # Clear session state if reading fails
            if 'uploaded_file_object' in st.session_state:
                del st.session_state['uploaded_file_object']
            return None
    else:
        # Clear session state if no file is uploaded or upload is cancelled
        if 'uploaded_file_object' in st.session_state:
            del st.session_state['uploaded_file_object']
    return None

def read_from_postgres(query, params=None):
    try:
        with engine.connect() as conn:
            return pd.read_sql(query, conn, params=params)
    except Exception as e:
        st.error(f"Database Read Error: {e}")
        return None # Return None on error

# Helper function for status color (defined at top level)
def get_status_color(status):
    if status == 'inprogress': return "blue"
    elif status == 'complete': return "green"
    elif status == 'notstarted': return "gray"
    else: return "orange" # For unexpected statuses

# --- Main App Logic ---
def main():
    st.title("Leads Validator AI")

    # Initialize session state variables if they don't exist
    if "logged_in" not in st.session_state:
        st.session_state.logged_in = False
    if 'selected_filename' not in st.session_state:
        st.session_state.selected_filename = None

    # --- Login Section ---
    if not st.session_state.logged_in:
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("Login"):
            if username == st.secrets.credentials.username and password == st.secrets.credentials.password:
                st.session_state.logged_in = True
                st.rerun() # Force rerun after successful login
            else:
                st.error("Invalid credentials")
        return # Stop execution if not logged in

    # --- Logged In Section ---
    st.write("Welcome! You are logged in.")

    # Auto-refresh every 60 seconds
    st_autorefresh(interval=60 * 1000, key="data_refresh")

    # Create tabs
    tab1, tab2 = st.tabs(["Main Dashboard", "Processed Leads"])

    # --- Tab 1: Main Dashboard ---
    with tab1:
        st.header("Main Dashboard")

        # --- CSV Upload Section ---
        st.subheader("CSV Upload")
        df_csv_display = load_csv() # Handles file upload and returns df for display
        if df_csv_display is not None:
            st.dataframe(df_csv_display) # Display the uploaded dataframe

            # Save Button Logic (only if a file object is in session state)
            if 'uploaded_file_object' in st.session_state and st.session_state.uploaded_file_object:
                if st.button("Save CSV to Postgres", key="save_button"):
                    try:
                        file_to_save = st.session_state.uploaded_file_object
                        filename = file_to_save.name

                        # Check for duplicate filename
                        with engine.connect() as conn_check:
                            check_sql = text("SELECT 1 FROM csv_uploads WHERE filename = :filename LIMIT 1")
                            exists = conn_check.execute(check_sql, {"filename": filename}).scalar_one_or_none()

                        if exists:
                            st.error(f"Duplicate filename '{filename}' detected. Please rename your file or delete the existing entry.")
                        else:
                            # Proceed with saving
                            file_to_save.seek(0)
                            csv_text = file_to_save.getvalue().decode("utf-8")
                            json_payload = json.dumps({"csv_content": csv_text})

                            with engine.connect() as conn_insert:
                                # Assuming 'status' column exists with default 'notstarted'
                                sql = text("INSERT INTO csv_uploads (filename, csv_data, status) VALUES (:filename, :data, 'notstarted')")
                                conn_insert.execute(sql, {"filename": filename, "data": json_payload})
                                conn_insert.commit()
                            st.success(f"CSV content from '{filename}' saved successfully to PostgreSQL!")
                            # Clear the uploaded file state after successful save
                            del st.session_state['uploaded_file_object']
                            st.rerun() # Rerun to update the dropdown below
                    except Exception as e:
                        st.error(f"Error saving CSV to PostgreSQL: {e}")

        st.divider()

        # --- File Selection and Processing Section ---
        st.subheader("Select File to Process")

        files_df = None
        selected_filename = None
        selected_file_status = None
        process_button_disabled = True # Default to disabled

        try:
            # Fetch filenames and statuses
            files_query = "SELECT id, filename, status FROM csv_uploads ORDER BY filename ASC;"
            files_df = read_from_postgres(files_query)
        except Exception as e:
            st.error(f"Error fetching file list: {e}") # Error handled in read_from_postgres

        if files_df is not None and not files_df.empty:
            filenames = files_df['filename'].tolist()

            # Initialize or validate selected_filename in session state
            current_selection = st.session_state.get('selected_filename')
            if current_selection not in filenames:
                 st.session_state.selected_filename = filenames[0] if filenames else None

            # Get the current index for the selectbox
            try:
                current_index = filenames.index(st.session_state.selected_filename) if st.session_state.selected_filename in filenames else 0
            except ValueError:
                current_index = 0
                st.session_state.selected_filename = filenames[0] if filenames else None

            if st.session_state.selected_filename:
                selected_filename_from_box = st.selectbox(
                    "Choose a file:",
                    filenames,
                    index=current_index,
                    key="file_selector"
                )

                # Update session state ONLY if the selection changed
                if selected_filename_from_box != st.session_state.selected_filename:
                     st.session_state.selected_filename = selected_filename_from_box
                     st.rerun() # Rerun immediately on selection change

                # Use the confirmed selection from session state
                selected_filename = st.session_state.selected_filename
                selected_file_id = None # Initialize id variable

                # Display Status of Selected File and get ID
                selected_file_info = files_df.loc[files_df['filename'] == selected_filename]
                if not selected_file_info.empty:
                    selected_file_id = int(selected_file_info['id'].iloc[0]) # Get the ID and cast to Python int
                    selected_file_status = selected_file_info['status'].iloc[0]
                    status_color = get_status_color(selected_file_status)
                    st.markdown(f"**Status:** :{status_color}[{selected_file_status.upper()}] (ID: {selected_file_id})") # Display ID too
                    process_button_disabled = (selected_file_status != 'notstarted')
                else:
                    st.warning("Selected file details temporarily unavailable.")
                    selected_file_status = None
                    process_button_disabled = True
            else:
                st.caption("No files available for selection.")
                process_button_disabled = True

        else:
            st.caption("No uploaded files found in the database.")
            process_button_disabled = True

        # --- Actions Section (Process Leads Button) ---
        st.subheader("Actions")

        if st.button("Process Leads", disabled=process_button_disabled, key="process_button"):
            # Use selected_file_id retrieved earlier
            if selected_file_id is not None and selected_file_status == 'notstarted':
                try:
                    # --- Fetch the CSV data for the selected file ID ---
                    csv_data_text = None
                    with engine.connect() as conn_fetch:
                        fetch_sql = text("SELECT csv_data FROM csv_uploads WHERE id = :id")
                        result = conn_fetch.execute(fetch_sql, {"id": selected_file_id}).scalar_one_or_none()
                        if result:
                            # Assuming csv_data is stored as JSON like {"csv_content": "..."}
                            try:
                                # Handle potential direct JSONB object from DB or string
                                csv_data_json = json.loads(result) if isinstance(result, str) else result
                                csv_data_text = csv_data_json.get("csv_content")
                            except (json.JSONDecodeError, AttributeError) as parse_error:
                                st.error(f"Failed to parse stored CSV data: {parse_error}")
                        else:
                            st.error(f"Could not find CSV data for ID {selected_file_id}.")

                    if csv_data_text:
                        # --- Send raw CSV data to the webhook ---
                        headers = {
                            'Content-Type': 'text/csv',
                            'x-id': str(selected_file_id) # Add the file ID as a header
                        }
                        response = requests.post(st.secrets.n8n.workflow_url, data=csv_data_text.encode('utf-8'), headers=headers)

                        if response.status_code == 200:
                            # Update status to 'inprogress' in DB using ID
                            with engine.connect() as conn_update:
                                update_sql = text("UPDATE csv_uploads SET status = 'inprogress' WHERE id = :id") # Use id in WHERE clause
                                conn_update.execute(update_sql, {"id": selected_file_id}) # Pass id parameter
                                conn_update.commit()
                            st.success(f"File '{selected_filename}' (ID: {selected_file_id}) sent for processing! Status updated to 'inprogress'.")
                            st.rerun() # Refresh UI
                    else:
                        st.error(f"Failed to send '{selected_filename}' for processing: {response.status_code} - {response.text}")
                except Exception as e:
                    st.error(f"Error processing leads for '{selected_filename}': {e}")
            else:
                st.warning(f"Cannot process file. Selected file: '{selected_filename}', Status: '{selected_file_status}'.")

        # Display appropriate caption based on state
        if process_button_disabled and selected_filename:
             st.caption(f"Cannot process file '{selected_filename}' with status '{selected_file_status}'. Only 'notstarted' files can be processed.")
        elif not selected_filename and files_df is not None and not files_df.empty:
             st.caption("Select a file to process.")
        elif files_df is None or files_df.empty:
             st.caption("Upload and save a CSV file first.")


    # --- Tab 2: Processed Leads ---
    with tab2:
        st.header("Processed Leads")

        # Display Total Processed Count
        try:
            count_query = "SELECT COUNT(*) FROM processed_leads;"
            with engine.connect() as conn:
                total_processed_count = conn.execute(text(count_query)).scalar_one_or_none()
            st.metric("Total Processed Leads in DB", total_processed_count if total_processed_count is not None else 0)
            # Removed refresh button here as autorefresh is active
        except Exception as e:
            st.error(f"Error getting processed leads count: {e}")

        st.divider()

        # Add Deduplication Button
        if st.button("Deduplicate by Username (Keep Oldest)", key="dedupe_button"): # Added key
            st.warning("This will permanently delete duplicate rows based on username, keeping the earliest entry. Are you sure?")
            if st.button("Yes, Deduplicate Now", key="dedupe_confirm_button"): # Added key
                try:
                    dedupe_sql = """
                        DELETE FROM processed_leads T1
                        USING processed_leads T2
                        WHERE T1.id > T2.id
                          AND T1.username = T2.username;
                    """
                    with engine.connect() as conn:
                        result = conn.execute(text(dedupe_sql))
                        conn.commit()
                    st.success(f"Deduplication complete. {result.rowcount} duplicate rows removed.")
                    st.rerun() # Rerun to refresh the table view
                except Exception as e:
                    st.error(f"Error during deduplication: {e}")

        # Display Processed Leads Table
        try:
            processed_query = """
                SELECT id, qualified, reason, username, profile_link, bio, category, email, full_name
                FROM processed_leads
                ORDER BY id DESC;
            """ # Added received_at back
            df_processed = read_from_postgres(processed_query)

            if df_processed is not None:
                if not df_processed.empty:
                    df_processed = df_processed.rename(columns={'id': 'db_id'})
                    st.dataframe(df_processed)
                else:
                    st.caption("No processed leads found yet.")
            # Error handled in read_from_postgres
        except Exception as e:
             # Catch potential errors during rename or display
            st.error(f"Error displaying processed leads table: {e}")


if __name__ == "__main__":
    main()