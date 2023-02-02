# Import libraries
import streamlit as st
import snowflake.connector
import pandas as pd
import numpy as np
import tensorflow as tf
from tensorflow.keras import layers

# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )

conn = init_connection()

# Function to perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetch_pandas_all()

# Get a unique list of products to select from
products = pd.DataFrame(run_query("SELECT DISTINCT COMMODITY_DESC from CHARLIE_FEATURE_STORE ORDER BY COMMODITY_DESC"))

st.title("Customer Propensity Scoring for Products")
st.header("Choose your Product and Propensity Score range")

# Ask user for product selection
product_selection = st.selectbox(
    "Select a product...",
    options = products
)

# Ask user for propensity score range 
## UPDATE SO THAT THE SLIDE UPDATES THE DATAFRAME DYNAMICALLY
prop_range = st.slider(
        'Select Propensity Score range...',
        min_value=0.0,
        max_value = 1.0,
        value = (0.0, 1.0)
)

# Run model button
run_model = st.button("Run model")

# When button is pressed
if run_model:

    # Retrieve data and test data based on product selection
    data = pd.DataFrame(run_query("SELECT * FROM CHARLIE_FEATURE_STORE WHERE COMMODITY_DESC = '{}';".format(product_selection)), )
    test_data = pd.DataFrame(run_query("SELECT * FROM CHARLIE_FEATURES WHERE COMMODITY_DESC = '{}';".format(product_selection)), )

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
        [tf.keras.layers.Dense(128, activation = 'relu'),
        tf.keras.layers.Dense(128, activation = 'relu'),
        tf.keras.layers.Dense(128, activation = 'relu'),
        tf.keras.layers.Dense(1)]
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

    st.header("Results")

    # Output table
    if display_data.empty:
        st.write("No results")
    else:
        st.dataframe(display_data)