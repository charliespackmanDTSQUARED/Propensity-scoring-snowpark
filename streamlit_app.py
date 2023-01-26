import streamlit as st
import snowflake.connector
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder

# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(
        **st.secrets["snowflake"], client_session_keep_alive=True
    )

conn = init_connection()

# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.experimental_memo(ttl=600)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetch_pandas_all()

products = pd.DataFrame(run_query("SELECT DISTINCT COMMODITY_DESC from CHARLIE_PRODUCT ORDER BY COMMODITY_DESC"))
data = pd.DataFrame(run_query("SELECT * FROM CHARLIE_FEATURE_STORE;"), )


st.title("Customer Propensity Scoring for Products")


st.header("Choose your Product and Propensity Score range")
# get product selection
product_selection = st.selectbox(
    "Select a product...",
    options = products
)


prop_range = st.slider(
        'Select Propensity Score range...',
        min_value=0.0,
        max_value = 1.0,
        value = (0.0, 1.0)
)

run_model = st.button("Run model")

if run_model:

    # Subset for chosen product to model
    data = data.loc[data['COMMODITY_DESC'] == product_selection]
    model_data = data.drop(columns=['COMMODITY_DESC', 'DATE', 'HOUSEHOLD_KEY'])

    # Feature conversion - categorical to numerical
    cols = [
        'AGE_DESC',
        'MARITAL_STATUS_CODE',
        'INCOME_DESC',
        'HOMEOWNER_DESC',
        'HH_COMP_DESC',
        'HOUSEHOLD_SIZE_DESC',
        'KID_CATEGORY_DESC']


    for col in cols:
        # Create blank encoder
        label_encoder = LabelEncoder()

        # Get encoding of column
        encoding = label_encoder.fit_transform(model_data[col].astype(str))
        
        # Drop existing column
        model_data = model_data.drop(columns = [col])

        # Append encoding to model data
        model_data[col] = encoding


    # Remove NaN
    model_data = model_data.fillna(0)

    # Split data into feature and label sets
    x = model_data.drop(columns = ['PURCHASES'])
    y = model_data['PURCHASES']
    # Create a model
    model = LogisticRegression().fit(x, y)
    # Test model performance
    data['PREDICTION'] = np.round(model.predict_proba(x)[:,0],2)

    display_data = data[['HOUSEHOLD_KEY', 'PREDICTION']].sort_values(by='PREDICTION', ascending= False)

    display_data = display_data.loc[(display_data['PREDICTION']>prop_range[0]) & (display_data['PREDICTION'] < prop_range[1]),:]

    st.header("Results")

    if display_data.empty:
        st.write("No results")
    else:
        st.dataframe(display_data)