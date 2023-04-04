# Import libraries
import streamlit as st
from snowflake.snowpark.session import Session
import pandas
import numpy

# Initialize connection.
# Uses st.experimental_singleton to only run once.
def create_session_object():
   connection_parameters = st.secrets["snowflake"]
   session = Session.builder.configs(connection_parameters).create()
   return session

conn = create_session_object()

# Function to perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 min.
@st.cache_data()
def sql_query(query):
    return conn.sql(query).to_pandas()

@st.cache_data()
def sql_execute(query):
    return conn.sql(query).collect()

st.set_page_config(
  page_title="Propensity Scoring App",
)

st.markdown("""
<style>
.big-font {
    font-size:20px;
} </style> """, unsafe_allow_html=True)

# Get a unique list of products to select from
products = sql_query("SELECT DISTINCT COMMODITY_DESC from CHARLIE_FEATURE_STORE WHERE COMMODITY_DESC != 'ADULT INCONTINENCE' ORDER BY COMMODITY_DESC")

st.title("Propensity to Buy")
st.caption("👋 Hello, welcome to our customer propensity scoring app! Choose a product from the drop down below and then select a propensity score range using the blue toggle, the model will then generate a list of househouses and their propensity to buy the product you have selected.")


# Explain the code
st.markdown('<p class="big-font">Data Overview</p>', unsafe_allow_html=True)
st.caption("To train the model, we used [the Complete Journey](https://www.kaggle.com/datasets/frtgnn/dunnhumby-the-complete-journey?select=campaign_table.csv) dataset by [Dunnhumby](https://www.dunnhumby.com/). This dataset contains over two years of transactions for 2,500 households who frequently shop at a retailer. Within this data you can find all the purchases made by each household. For certain households you will also find demographic information and direct marketing contact history.")
st.caption("The Propensity Scoring model is an Artificial Neural Network with 3 hidden layers, trained using [TensorFlow](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/45166.pdf) in Python (30 epochs over ~4,000 training examples). The code for this app can be found on [GitHub](https://github.com/charliespackmanDTSQUARED/Propensity-scoring-snowpark).")   

# Refresh the data
st.markdown('<p class="big-font">Refresh Data</p>', unsafe_allow_html=True)
refresh_data = st.button("Refresh Data") # Run model button
st.caption("Press this button to update the underlying data with the most recent transactions (this will we take 2-3 minutes).")
if refresh_data:
    st.write("👨🏼‍💻 Refresh started...")
    sql_execute("CALL CREATE_FEATURE_INFERENCE_STORE_V2(['30', '60', '90'], [1, 31, 61, 91])")
    st.write("✅ Refresh complete!")

# Run the model
st.markdown('<p class="big-font">Run Model</p>', unsafe_allow_html=True)
st.caption("Select a product to generate propensity scores for and press the 'Generate Propensity' button.")

# Ask user for product selection
product_selection = st.selectbox(
    "🎯 Select a product...",
    options = products
)

# Ask user for propensity score range 
## UPDATE SO THAT THE SLIDE UPDATES THE DATAFRAME DYNAMICALLY
prop_range = st.slider(
        '🎯 Select a propensity score % ...',
        min_value= 0,
        max_value = 100,
        value = (0, 100)
)
prop_range = [prop_range[0]/100, prop_range[1]/100]

# Run model button
run_model = st.button("Generate Propensity")

# When button is pressed
if run_model:
    st.text("👨🏼‍💻 Running the model...")
    
    # Push ML to Snowflake
    metrics = sql_execute(f"CALL TRAIN_PROPENSITY_MODEL('{product_selection}')")
    
    # Get predictions
    data = sql_query("SELECT CHARLIE_INFERENCE_PREDICTIONS.HOUSEHOLD_KEY, CHARLIE_INFERENCE_PREDICTIONS.PREDICTION, CHARLIE_HH_DEMOGRAPHIC.AGE_DESC, CHARLIE_HH_DEMOGRAPHIC.MARITAL_STATUS_CODE, CHARLIE_HH_DEMOGRAPHIC.INCOME_DESC FROM CHARLIE_INFERENCE_PREDICTIONS LEFT JOIN CHARLIE_HH_DEMOGRAPHIC ON CHARLIE_INFERENCE_PREDICTIONS.HOUSEHOLD_KEY = CHARLIE_HH_DEMOGRAPHIC.HOUSEHOLD_KEY")
    
    # Data for streamlit table
    display_data = data[['HOUSEHOLD_KEY', 'PREDICTION', 'AGE_DESC', 'MARITAL_STATUS_CODE', 'INCOME_DESC']].sort_values(by='PREDICTION', ascending= False)

    # Subset for slider range
    display_data = display_data.loc[(display_data['PREDICTION']>prop_range[0]) & (display_data['PREDICTION'] < prop_range[1]),:]
    display_data['PREDICTION'] = (display_data['PREDICTION']*100).round(2).astype(str) + ' %'
    
    display_data.rename(columns = {'HOUSEHOLD_KEY':'Household Number'}, inplace = True)
    display_data.rename(columns = {'PREDICTION':'Prediction'}, inplace = True)
    display_data.rename(columns = {'AGE_DESC':'Age'}, inplace = True)
    display_data.rename(columns = {'MARITAL_STATUS_CODE':'Marital Status'}, inplace = True)
    display_data.rename(columns = {'INCOME_DESC':'Income'}, inplace = True)
    
    st.text("✅ Model created!")

    st.markdown('<p class="big-font">Results</p>', unsafe_allow_html=True)
    st.caption("The table below shows each household's propensity to buy the item you selected, ordered from highest to lowest.")

    # CSS to inject contained in a string
    hide_dataframe_row_index = """
                <style>
                .row_heading.level0 {display:none}
                .blank {display:none}
                </style>
                """
                    
    # Output table
    if display_data.empty:
        st.write("No results")
    else:          
        # Inject CSS with Markdown
        st.markdown(hide_dataframe_row_index, unsafe_allow_html=True) # Inject CSS with Markdown
        st.dataframe(display_data) # Display a static table
    
    print(metrics) 
