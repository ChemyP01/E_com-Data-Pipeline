import pandas as pd

## READING RAW FILES ##
c_= pd.read_json('raw_data/Market 2 Customers.json')
d_= pd.read_csv('raw_data/Market 2 Deliveries.csv')
o_= pd.read_csv('raw_data/Market 2 Orders.csv')

##Transform customer data

c_['Created At'] = pd.to_datetime(c_['Created At'])

c_['Date_Created'] = c_['Created At'].dt.date
c_['Day_Created'] = c_['Created At'].dt.day_name()
c_['Month_Created'] = c_['Created At'].dt.month_name()
c_['Year_Created'] = c_['Created At'].dt.year
c_['Time_Created'] = c_['Created At'].dt.strftime('%I:%M:%S %p')

c_= c_.drop('Created At',axis=1)




## Transform deliveries data 
d_.drop(columns=['Tags','Notes'], inplace=True)

d_['Order_ID'] = d_['Order_ID'].str.replace('YR-', '').str.replace(',0', '')

d_['Order_ID'] = d_['Order_ID'].astype(int)





## Transform orders data
o_.drop(columns=['Cancellation Reason','Commission Payout Status','Commission Amount',
                 'Merchant Earning','Reviews','Ratings','Promo Code','Transaction Status',
                 'Customization Option','Customization Group'], inplace=True)

o_['Order ID'] = o_['Order ID'].astype(str)

o_['Order ID'] = o_['Order ID'].str.rstrip('.0')

o_['Order ID'] = o_['Order ID'].astype(int)

o_.rename(columns={'Order ID': 'Order_ID'}, inplace=True)


## MERGING DATA
new_data = pd.merge(c_, o_, on='Customer ID', how = 'outer')
new_data = pd.merge(new_data, d_, on='Order_ID', how='outer')

new_data.to_csv('transformed_data/market2_data.csv', index = False)