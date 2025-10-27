import streamlit as st
import pandas as pd
import requests
import time
import requests.exceptions # Import the exception

# --- Helper Function to Call the API (UPDATED TO GET ALL MATCHES) ---
def fetch_whitepages_data(api_key, street, city, state, zip_code):
    """
    Fetches data from the new Whitepages API (api.whitepages.com)
    and returns ALL matches, not just the first one.
    """
    
    API_ENDPOINT = "https://api.whitepages.com/v1/person/"
    
    params = {
        "street": street,
        "city": city,
        "state_code": state,
        "zipcode": zip_code,
    }
    
    headers = {
        "X-Api-Key": api_key
    }

    try:
        response = requests.get(API_ENDPOINT, params=params, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            
            # --- THIS IS THE NEW LOGIC ---
            if data and isinstance(data, list):
                
                # Create empty lists to hold all matches
                names_list = []
                phones_list = []
                emails_list = []

                # Loop through every person returned for this address
                for person in data:
                    names_list.append(person.get('name', 'N/A'))
                    
                    # Safely get the first phone for this person
                    phone_list_data = person.get('phones', [])
                    phone = phone_list_data[0].get('number', 'N/A') if phone_list_data else 'N/A'
                    phones_list.append(phone)

                    # Safely get the first email for this person
                    email_list_data = person.get('emails', [])
                    email = email_list_data[0] if email_list_data else 'N/A'
                    emails_list.append(email)

                # Join all found items with a " | " separator
                all_names = " | ".join(names_list)
                all_phones = " | ".join(phones_list)
                all_emails = " | ".join(emails_list)
                
                return all_names, all_phones, all_emails, f"Success ({len(names_list)} found)"
            
            else:
                # This runs if the address is valid but no people are linked
                return "Not Found", "Not Found", "Not Found", "Valid address, no person found"
            
        else:
            # --- SAFER ERROR HANDLING ---
            error_message = ""
            error_text = response.text 
            
            if not error_text:
                error_message = f"Empty response from server (Status Code: {response.status_code})."
            else:
                try:
                    error_message = response.json()
                except requests.exceptions.JSONDecodeError:
                    error_message = error_text[:100] + "..." 
            
            st.error(f"API Error (Row: {street}): {error_message}")
            return "Error", "Error", "Error", f"Status {response.status_code}: {error_message}"

    except Exception as e:
        st.exception(e)
        return "Exception", "Exception", "Exception", str(e)

# --- Streamlit App UI (No Changes from here down) ---

st.set_page_config(layout="wide")
st.title("Address Enrichment Tool (Whitepages API)")

st.info("Upload a CSV to enrich it with names, phones, and emails. \n\n**Your CSV must have separate columns for street, city, state, and zip.**")

st.subheader("CSV Column Configuration")
st.warning("Enter the *exact* column names from your CSV file.")

col1, col2, col3, col4 = st.columns(4)
with col1:
    street_col = st.text_input("Street Address Column", "street")
with col2:
    city_col = st.text_input("City Column", "city")
with col3:
    state_col = st.text_input("State Column (2-letter)", "state")
with col4:
    zip_col = st.text_input("Zip Code Column", "zip")

uploaded_file = st.file_uploader("Upload your CSV file", type=["csv"])

try:
    API_KEY = st.secrets["EKATA_API_KEY"] # This name is fine, it just holds the key
except KeyError:
    st.error("API Key not found. Please add your API_KEY to the Streamlit Cloud secrets.")
    st.stop() 

if uploaded_file is not None and all([street_col, city_col, state_col, zip_col]):
    
    try:
        df = pd.read_csv(uploaded_file)
        required_cols = [street_col, city_col, state_col, zip_col]
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            st.error(f"Error: The following columns were not found in your CSV: {', '.join(missing_cols)}")
        else:
            st.success(f"File uploaded! Found {len(df)} rows.")
            st.dataframe(df.head())

            if st.button(f"Process {len(df)} Addresses"):
                
                names, phones, emails, statuses = [], [], [], []
                progress_bar = st.progress(0)
                status_text = st.empty()

                for i, row in df.iterrows():
                    # Call the function with the CSV column values
                    name, phone, email, status = fetch_whitepages_data(
                        API_KEY, 
                        row[street_col],
                        row[city_col],
                        row[state_col],
                        row[zip_col]
                    )
                    
                    names.append(name)
                    phones.append(phone)
                    emails.append(email)
                    statuses.append(status)
                    
                    progress_percentage = (i + 1) / len(df)
                    progress_bar.progress(progress_percentage)
                    status_text.text(f"Processing row {i+1}/{len(df)}: {row[street_col]}")
                    
                    time.sleep(0.1) 

                status_text.success("Processing Complete!")
                
                df_results = df.copy()
                df_results['Enriched_Name'] = names
                df_results['Enriched_Phone'] = phones
                df_results['Enriched_Email'] = emails
                df_results['Processing_Status'] = statuses
                
                st.dataframe(df_results)
                
                @st.cache_data
                def convert_df_to_csv(df_to_convert):
                    return df_to_convert.to_csv(index=False).encode('utf-8')

                csv_data = convert_df_to_csv(df_results)

                st.download_button(
                    label="Download Enriched CSV",
                    data=csv_data,
                    file_name="enriched_addresses.csv",
                    mime="text/csv",
                )

    except Exception as e:
        st.error("An unexpected error occurred:")
        st.exception(e)
