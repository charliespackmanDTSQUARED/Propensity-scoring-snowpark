# Import libraries
import snowflake.connector
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import LabelEncoder
import pandas as pd

# Test connection to Snowflake
try:
    ctx = snowflake.connector.connect(
        user='',
        password='',
        account='dtsquaredpartner.eu-west-1'
        )
    cs = ctx.cursor()

    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print("Connection success - version: " + one_row[0])
except:
    print("Connection unsuccessful")
    raise SystemExit
# else:
#     cs.close()
#     ctx.close()


# Run procedure to clear tables of data


# Run procedure to create the 30 day feature sets


# Run procedure for feature store


# Read feature data
#data = pd.read_csv("Feature store.csv")

# Subset for chosen product to model
model_data = data.loc[data['COMMODITY_DESC'] == 'SOFT DRINKS']
model_data = model_data.drop(columns=['COMMODITY_DESC', 'DATE', 'HOUSEHOLD_KEY'])

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

# Split data into train and test sets
x_train, x_test, y_train, y_test = train_test_split(model_data.drop(columns = ['PURCHASES']), model_data['PURCHASES'])

# Create a model
model = LogisticRegression().fit(x_train, y_train)

# Test model performance
model.predict_proba(x_test)



