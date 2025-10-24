import streamlit as st
import pandas as pd
import geopandas as gpd # We still need this to read geometries
import folium
from folium.plugins import Draw, MeasureControl
from streamlit_folium import st_folium
from shapely.geometry import Point, shape
import usaddress
import json

# --- ⚠️ CONFIGURATION - YOUR URL IS ADDED ---

# This is your correct Supabase bucket URL
SUPABASE_BASE_URL = "https://wrvonobxqimskkiajkft.supabase.co/storage/v1/object/public/data-splits/"

# --- CRITICAL COLUMNS - Check your files! ---
# If your columns have different names, change them here.

# 1. In your 'schools.csv' file:
#    The column with the unique school ID (e.g., EKEY_5)
SCHOOLS_ID_COLUMN = "EKEY_5" 
#    The column with the friendly school name to display
SCHOOLS_LABEL_COLUMN = "LABEL" 
#    The column with the school's latitude
SCHOOLS_LAT_COLUMN = "LAT"
#    The column with the school's longitude
SCHOOLS_LON_COLUMN = "LON"

# 2. In your Parquet files (from CAMS):
#    The column with the address latitude
ADDRESS_LAT_COLUMN = "LAT"
#    The column with the address longitude
ADDRESS_LON_COLUMN = "LON"
#    The column with the full street address for parsing
ADDRESS_FULL_COLUMN = "FullAddress"

# --- END CONFIG ---


# --- Caching Functions (to load data) ---

@st.cache_data
def load_main_files():
    """Loads the manifest and school list from Supabase."""
    try:
        manifest_url = f"{SUPABASE_BASE_URL}_manifest.csv"
        manifest = pd.read_csv(manifest_url)
        
        # The user's original schools file (with LAT/LON for zooming)
        schools_url = f"{SUPABASE_BASE_URL}schools.csv" 
        schools = pd.read_csv(schools_url)
        
        return manifest, schools
    except Exception as e:
        st.error(f"Fatal Error: Could not load '_manifest.csv' or 'schools.csv' from Supabase.")
        st.error(f"Details: {e}")
        st.error("Solution: Make sure you uploaded both '_manifest.csv' AND 'schools.csv' to the 'data-splits' bucket.")
        st.stop()

@st.cache_data
def load_address_data(file_paths_json):
    """
    Given a JSON string of file paths, downloads them from Supabase
    and combines them into one DataFrame.
    """
    file_urls = [f"{SUPABASE_BASE_URL}{path}" for path in json.loads(file_paths_json)]
    
    if not file_urls:
        return pd.DataFrame()

    df_list = []
    for url in file_urls:
        try:
            # We only read the columns we absolutely need to keep memory low
            df_list.append(pd.read_parquet(url, columns=[ADDRESS_LAT_COLUMN, ADDRESS_LON_COLUMN, ADDRESS_FULL_COLUMN]))
        except Exception as e:
            st.warning(f"Could not load data shard: {url}. Error: {e}")
            
    if not df_list:
        return pd.DataFrame()
        
    return pd.concat(df_list, ignore_index=True)

# --- Address Parsing Function (from your original code) ---

def parse_address_expanded(line):
    # Added a check for non-string (e.g., None or nan) values
    if not isinstance(line, str):
        return []
    try:
        parsed, _ = usaddress.tag(line)
        house_num = parsed.get("AddressNumber", "")
        street = " ".join([
            parsed.get("StreetNamePreDirectional", ""),
            parsed.get("StreetName", ""),
            parsed.get("StreetNamePostType", ""),
            parsed.get("StreetNamePostDirectional", ""),
        ]).strip()
        full_address = f"{house_num} {street}".strip()
        unit = parsed.get("OccupancyIdentifier", "")
        city = parsed.get("PlaceName", "")
        state = parsed.get("StateName", "")
        zip_code = parsed.get("ZipCode", "")

        rows = []
        # expand unit if it's a range
        if unit and "-" in unit:
            parts = unit.replace("–", "-").split("-")
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                start = int(parts[0])
                end = int(parts[1])
                for u in range(start, end + 1):
                    rows.append({
                        "Address": full_address,
                        "Unit": str(u),
                        "City": city,
                        "State": state,
                        "ZIP": zip_code,
                        "Original": line
                    })
                return rows
        # fallback — single row
        return [{
            "Address": full_address,
            "Unit": unit,
            "City": city,
            "State": state,
            "ZIP": zip_code,
            "Original": line
        }]
    except usaddress.RepeatedLabelError:
        return [{
            "Address": "",
            "Unit": "",
            "City": "",
            "State": "",
            "ZIP": "",
            "Original": line
        }]

# --- Main App ---
st.set_page_config(page_title="School Address Finder", layout="wide")
st.title("📍 School Community Address Finder")

# 1. Load the manifest and school list
try:
    manifest, schools = load_main_files()
except Exception as e:
    st.stop()

# 2. Merge data
#    This JOINS your 'schools.csv' with the 'manifest.csv'
#    It assumes 'boundary_id' from the manifest (which is EKEY_5)
#    matches the SCHOOLS_ID_COLUMN (EKEY_5) in 'schools.csv'
try:
    manifest['join_key'] = manifest['boundary_id'].astype(str)
    schools['join_key'] = schools[SCHOOLS_ID_COLUMN].astype(str)
    
    school_data_merged = pd.merge(
        schools, 
        manifest, 
        on="join_key",
        how="left"
    )
    
    if school_data_merged['file_paths_json'].isnull().all():
        st.error("Data Merge Failed: No schools in 'schools.csv' matched a boundary in '_manifest.csv'.")
        st.error(f"Check that the column '{SCHOOLS_ID_COLUMN}' in schools.csv matches the 'EKEY_5' IDs from your shapefile.")
        st.stop()
        
except Exception as e:
    st.error(f"Fatal Error: Could not merge 'schools.csv' and '_manifest.csv'.")
    st.error(f"Check your CONFIG variables. We tried to join on 'schools.csv' column '{SCHOOLS_ID_COLUMN}' and the manifest's 'boundary_id'.")
    st.error(f"Details: {e}")
    st.stop()


# 3. Create the school selector
school_list = school_data_merged[SCHOOLS_LABEL_COLUMN].sort_values().tolist()
site_selected = st.selectbox("Select a School Campus", school_list)

result_container = st.container()

if site_selected:
    # 4. Get selected school's info
    selected_school = school_data_merged[school_data_merged[SCHOOLS_LABEL_COLUMN] == site_selected].iloc[0]
    
    slon = selected_school[SCHOOLS_LON_COLUMN]
    slat = selected_school[SCHOOLS_LAT_COLUMN]
    file_paths_json = selected_school["file_paths_json"]

    # 5. Load the address data for *only this school*
    with st.spinner(f"Loading address data for {site_selected}..."):
        addresses_df = load_address_data(file_paths_json)
    
    if addresses_df.empty:
        st.error(f"No address data found for {site_selected}.")
        st.stop()
    
    # Data check
    if ADDRESS_LAT_COLUMN not in addresses_df.columns or ADDRESS_LON_COLUMN not in addresses_df.columns:
        st.error(f"Parquet files are missing required columns! We need '{ADDRESS_LAT_COLUMN}' and '{ADDRESS_LON_COLUMN}'.")
        st.error(f"Available columns: {', '.join(addresses_df.columns)}")
        st.stop()
    
    # Drop rows with missing geo-data
    addresses_df = addresses_df.dropna(subset=[ADDRESS_LAT_COLUMN, ADDRESS_LON_COLUMN])


    st.info(f"Loaded {len(addresses_df):,} addresses for this school's boundary. Now, draw your target area.")

    # 6. Create the map, auto-zoomed to the school
    fmap = folium.Map(location=[slat, slon], zoom_start=16)
    folium.Marker([slat, slon], tooltip=site_selected, icon=folium.Icon(color="blue")).add_to(fmap)

    draw = Draw(
        export=True,
        position='topleft',
        draw_options={
            'polyline': False, 'rectangle': True, 'circle': False, 
            'polygon': True, 'marker': False, 'circlemarker': False,
        },
        edit_options={'edit': True, 'remove': True}
    )
    draw.add_to(fmap)
    fmap.add_child(MeasureControl(primary_length_unit='miles'))

    # 7. Display the map
    map_data = st_folium(fmap, width="100%", height=500)

    # 8. Get drawn shapes
    features = []
    if map_data and "all_drawings" in map_data and map_data["all_drawings"]:
        features = map_data["all_drawings"]
    
    if not features:
        st.info("Tip: Use the tools on the left of the map to draw one or more shapes.")
        st.stop()

    # 9. Filter Logic
    with result_container:
        st.write("---")
        st.subheader("Filtered & Parsed Addresses")
        
        try:
            # Convert all drawn shapes into Shapely objects
            polygons = [shape(feature["geometry"]) for feature in features]
            
            if not polygons:
                st.warning("Please draw at least one shape to filter addresses.")
                st.stop()

            # Create Shapely Points from the address LAT/LON columns
            points = [Point(lon, lat) for lon, lat in zip(addresses_df[ADDRESS_LON_COLUMN], addresses_df[ADDRESS_LAT_COLUMN])]
            
            # Create a new GeoDataFrame for efficient spatial filtering
            gpd_addresses = gpd.GeoDataFrame(addresses_df, geometry=points)

            # Find all points that fall inside *any* of the drawn polygons
            filtered_mask = gpd_addresses.geometry.apply(lambda point: any(poly.contains(point) for poly in polygons))
            filtered_df = addresses_df[filtered_mask]

        except Exception as e:
            st.error(f"Could not filter addresses. Error: {e}")
            st.stop()

        if filtered_df.empty:
            st.info("No addresses found within the drawn area(s). Try drawing a different shape.")
        else:
            # 10. Parse the filtered addresses
            all_rows = []
            
            if ADDRESS_FULL_COLUMN not in filtered_df.columns:
                st.error(f"Error: The address column '{ADDRESS_FULL_COLUMN}' was not found.")
                st.error(f"Available columns: {', '.join(filtered_df.columns)}")
                st.stop()

            for addr in filtered_df[ADDRESS_FULL_COLUMN].tolist():
                all_rows.extend(parse_address_expanded(addr))

            parsed_df = pd.DataFrame(all_rows)

            st.markdown(f"**Found {len(parsed_df)} mailable addresses.**")
            st.dataframe(parsed_df.head())

            csv = parsed_df.to_csv(index=False).encode("utf-8")

            st.download_button(
                label=f"Download {len(parsed_df)} Parsed Addresses",
                data=csv,
                file_name=f"{site_selected.replace(' ', '_')}_parsed_addresses.csv",
                mime='text/csv'
            )
            
            st.info("To get contact info (phones/emails), upload this CSV to the **Contact Enricher** app.")
