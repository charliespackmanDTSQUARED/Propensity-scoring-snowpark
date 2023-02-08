# Import libraries
import streamlit as st
from snowflake.snowpark.session import Session
import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras import layers

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
def run_query(query: str):
    data = conn.sql(query).to_pandas()
    return data

# Get a unique list of products to select from
products = run_query("SELECT DISTINCT COMMODITY_DESC from CHARLIE_FEATURE_STORE WHERE COMMODITY_DESC != 'ADULT INCONTINENCE' ORDER BY COMMODITY_DESC")

st.title("Propensity to Buy")
st.caption("👋 Hello, welcome to our customer propensity scoring app! Choose a product from the drop down below and then select a propensity score range using the blue toggle, the model will then generate a list of househouses and their propensity to buy the product you have selected.")
# Our propensity scoring model was trained on a data set containing transactions over two years from a group of 2,500 households who are frequent shoppers at a retailer. For certain households, demographic information and marketing contact history were included.")​

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
    
    # Retrieve data and test data based on product selection
    data = run_query("SELECT * FROM CHARLIE_FEATURE_STORE WHERE COMMODITY_DESC = '{}';".format(product_selection))
    test_data = run_query("SELECT * FROM CHARLIE_FEATURES WHERE COMMODITY_DESC = '{}';".format(product_selection))

    # Function to pre-process raw snowflake data 
    def process_tensors(model_data):

        # Subset for chosen product to model
        model_data = data.drop(columns=['COMMODITY_DESC', 'DATE', 'HOUSEHOLD_KEY'])
        # reset index to ensure it can be rejoined later
        model_data = model_data.reset_index(drop = True)
        # Convert to float - needed for TF
        model_data['PURCHASED'] = model_data['PURCHASED'].astype(float)

        # Remove NaN
        model_data = model_data.fillna(0)

        # Split to features and labels
        model_data_x = model_data.iloc[:, :144]
        model_data_y = model_data.iloc[:, 144]

        return model_data_x, model_data_y

    # Pre-process the data and test data
    ds_model_data_x, ds_model_data_y = process_tensors(data)
    ds_test_model_data_x, ds_test_model_data_y = process_tensors(test_data)

    # Define and train TF model
    model = tf.keras.Sequential(
        [layers.Dense(128, activation = 'relu'),
        layers.Dense(128, activation = 'relu'),
        layers.Dense(128, activation = 'relu'),
        layers.Dense(1)]
        )

    model.compile(
        optimizer='adam',
        loss=tf.keras.losses.BinaryCrossentropy(from_logits=True),
        metrics=["accuracy", "AUC"])

    # Model training
    model.fit(ds_model_data_x.to_numpy(), ds_model_data_y.to_numpy(), epochs=30)

    # Append predictions to dataset
    data['PREDICTION'] = np.round(tf.nn.sigmoid(model.predict(ds_test_model_data_x.to_numpy())), 4)

    # Data for streamlit table
    display_data = data[['HOUSEHOLD_KEY', 'PREDICTION']].sort_values(by='PREDICTION', ascending= False)
    
    # Subset for slider range
    display_data = display_data.loc[(display_data['PREDICTION']>prop_range[0]) & (display_data['PREDICTION'] < prop_range[1]),:]

    st.subheader("Results")
    st.caption("The table below shows each household's propensity to buy the item you selected, ordered from highest to lowest.")
    
    # Output table
    if display_data.empty:
        st.write("No results")
    else:
        st.dataframe(display_data)