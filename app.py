import streamlit as st
import pandas as pd
import requests
import time

# --- Helper Function to Call the API (UPDATED FOR SECRETS) ---
def fetch_whitepages_data(api_key, street, city, state, zip_code):
    """
    Fetches data from the Ekata (formerly Whitepages Pro) Identity Check API.
    """
    API_ENDPOINT = "https://api.ekata.com/1.0/identity_check"
    
    params = {
        "primary.address.street_line_1": street,
        "primary.address.city": city,
        "primary.address.state_code": state,
        "primary.address.postal_code": zip_code,
        "primary.address.country_code": "US"
    }
    
    # Use the API key (passed from st.secrets) for authentication
    auth = (api_key, '') 

    try:
        response = requests.get(API_ENDPOINT, params=params, auth=auth)
        
        if response.status_code == 200:
            data = response.json()
            
            # --- Safely parse the JSON response ---
            person = data.get('primary_belongs_to', [{}])[0]
            if not person: person = {}
            
            phone_data = data.get('phones', [{}])[0]
            if not phone_data: phone_data = {}

            email_data = data.get('emails', [{}])[0]
            if not email_data: email_data = {}
            
            # --- Extract data ---
            name = person.get('name', 'Not Found')
            phone = phone_data.get('phone_number', 'Not Found')
            email = email_data.get('email_address', 'Not Found')
            
            return name, phone, email, "Success"
            
        else:
            # Show a user-friendly error in the app
            st.error(f"API Error (Row: {street}): {response.status_code} - {response.json().get('error', {}).get('message', 'Unknown Error')}")
            return "Error", "Error", "Error", response.json().get('error', {}).get('message', 'API Error')

    except Exception as e:
        st.exception(e)
        return "Exception", "Exception", "Exception", str(e)

# --- Streamlit App UI (UPDATED FOR SECRETS) ---

st.set_page_config(layout="wide")
st.title("Address Enrichment Tool (Ekata API)")

st.info("Upload a CSV to enrich it with names, phones, and emails. \n\n**Your CSV must have separate columns for street, city, state, and zip.**")

# --- 1. Get Column Names (API Key is now hidden) ---
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

# --- 2. Check for API Key in Streamlit Secrets ---
try:
    # This will load the API key you add to the Streamlit Cloud dashboard
    API_KEY = st.secrets["EKATA_API_KEY"]
except KeyError:
    st.error("API Key not found. Please add your EKATA_API_KEY to the Streamlit Cloud secrets.")
    st.stop() # Stop the app if the key is missing

# --- 3. Run the App ---
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
                    name, phone, email, status = fetch_whitepages_data(
                        API_KEY, # Pass the secret key to the function
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
                    
                    time.sleep(0.1) # Rate Limiting

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
