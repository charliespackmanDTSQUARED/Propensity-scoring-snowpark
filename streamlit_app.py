# Import libraries
import streamlit as st
from snowflake.snowpark.session import Session
import pandas
import numpy

# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def create_session_object():
   connection_parameters = st.secrets["snowflake"]
   session = Session.builder.configs(connection_parameters).create()
   return session

conn = create_session_object()

# Function to perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def sql_query(query):
    return conn.sql(query).to_pandas()

@st.experimental_memo(ttl=600)
def sql_execute(query):
    return conn.sql(query).collect()


# Get a unique list of products to select from
products = sql_query("SELECT DISTINCT COMMODITY_DESC from CHARLIE_FEATURE_STORE WHERE COMMODITY_DESC != 'ADULT INCONTINENCE' ORDER BY COMMODITY_DESC")

st.title("Propensity to Buy")
st.caption("👋 Hello, welcome to our customer propensity scoring app! Choose a product from the drop down below and then select a propensity score range using the blue toggle, the model will then generate a list of househouses and their propensity to buy the product you have selected.")
# Our propensity scoring model was trained on a data set containing transactions over two years from a group of 2,500 households who are frequent shoppers at a retailer. For certain households, demographic information and marketing contact history were included.")


st.subheader("Refresh data")
# Run model button
refresh_data = st.button("Refresh Data")

st.caption("Press this button to update the underlying data with the most recent transactions (this will we take 2-3 minutes).")

if refresh_data:
    st.write("👨🏼‍💻 Refresh started...")
    sql_execute("CALL CREATE_FEATURE_INFERENCE_STORE_V2(['30', '60', '90'], [1, 31, 61, 91])")
    st.write("✅ Refresh complete!")


st.subheader("Run model")
st.caption("Select a product to generate propensity scores for and press the 'Generate Propensity' button.")

# Ask user for product selection
product_selection = st.selectbox(
    "🎯 Select a product...",
    options = products
)


# Ask user for propensity score range 
## UPDATE SO THAT THE SLIDE UPDATES THE DATAFRAME DYNAMICALLY
prop_range = st.slider(
        '🎯 Select a propensity score range...',
        min_value=0.0,
        max_value = 1.0,
        value = (0.0, 1.0)
)

# Run model button
run_model = st.button("Generate Propensity")

# When button is pressed
if run_model:
    st.text("👨🏼‍💻 Running the model...")
    
    # Push ML to Snowflake
    sql_execute(f"CALL TRAIN_PROPENSITY_MODEL('{product_selection}')")

    # Get predictions
    data = sql_query("SELECT * FROM CHARLIE_INFERENCE_PREDICTIONS")

    # Data for streamlit table
    display_data = data[['HOUSEHOLD_KEY', 'PREDICTION']].sort_values(by='PREDICTION', ascending= False)
    
    # Subset for slider range
    display_data = display_data.loc[(display_data['PREDICTION']>prop_range[0]) & (display_data['PREDICTION'] < prop_range[1]),:]

    st.text("✅ Model created!")

    st.subheader("Results")
    st.caption("The table below shows each household's propensity to buy the item you selected, ordered from highest to lowest.")
    
    # Output table
    if display_data.empty:
        st.write("No results")
    else:
        st.dataframe(display_data)