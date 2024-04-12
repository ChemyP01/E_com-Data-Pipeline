
import pandas as pd
import numpy as np

# Read data from files
m1_customers = pd.read_json('RawData/Market 1 Customers.json')
m1_deliveries = pd.read_csv('RawData/Market 1 Deliveries.csv')
m1_orders = pd.read_csv('RawData/Market 1 Orders.csv')

# Transformation on the Market 1 Customers
m1_customers.drop(columns=['Number of employees'], inplace=True)
m1_customers['Created At'] = pd.to_datetime(m1_customers['Created At'])

# Extract components
m1_customers['Date_Created'] = m1_customers['Created At'].dt.date
m1_customers['Day_Created'] = m1_customers['Created At'].dt.day_name()
m1_customers['Month Created'] = m1_customers['Created At'].dt.month_name()
m1_customers['Year_Created'] = m1_customers['Created At'].dt.year
m1_customers['Time-Created'] = m1_customers['Created At'].dt.strftime('%I:%M:%S %p')


m1_customers = m1_customers.drop('Created At',axis=1)


# Transformation on the Market 1 Deliveries
m1_deliveries.drop(columns=['Unnamed: 30','Unnamed: 31'], inplace=True)
m1_deliveries.drop(columns=['Tags'], inplace=True)

m1_deliveries['Order_ID'] = m1_deliveries['Order_ID'].str.replace('YR-', '').str.replace(',0', '')
m1_deliveries['Order_ID']


# Transformation on the Market 1 Orders
m1_orders.drop(columns=['Cancellation Reason','Commission Payout Status','Commission Amount',
                       'Merchant Earning','Reviews','Ratings','Promo Code','Transaction Status','Customization Option','Customization Group'], inplace=True)

m1_orders['Order ID'] = m1_orders['Order ID'].astype(str)

m1_orders['Order ID'] = m1_orders['Order ID'].str.rstrip('.0')

m1_orders['Order ID'] = m1_orders['Order ID'].astype(int)

m1_orders.rename(columns={'Order ID': 'Order_ID'}, inplace=True)

# Merge data
new_data = pd.merge(m1_customers, m1_orders, on='Customer ID', how = 'outer')
#import numpy as np
new_data['Order_ID'] = new_data['Order_ID'].fillna('').astype(str).str.rstrip('.0')
new_data['Order_ID'] = new_data['Order_ID'].replace('', np.nan)


new_data = pd.merge(new_data, m1_deliveries, on='Order_ID', how='outer')

# Save merged data to a CSV file
new_data.to_csv('RefineData/market1_data.csv', index=False)
