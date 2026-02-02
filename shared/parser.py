from datetime import datetime 
import pandas as pd
from shared.schema import *

date_format = '%Y/%m/%d'
time_format = '%H%M'

# def date_check(str):
#     if str != '':
#         date = datetime.strptime(str, date_format)
#         return date
#     else:
#         return None

def parse_excel(df):
    upload_df = df.rename(columns=rename_dict)
    
    upload_df['main'] = upload_df.loc[:, 'sub'].apply(lambda x: x[:9])

    upload_df['customer_no'] = upload_df.loc[:, 'customer'].apply(lambda x: x[:7])
    upload_df['customer'] = upload_df.loc[:, 'customer'].apply(lambda x: x[7:])

    upload_df['dept_pickup'] = upload_df.loc[:, 'dept_shipping'].replace('0001 總倉(2F廠務辦公室)', '')
    upload_df['dept_pickup'] = upload_df.loc[:, 'dept_shipping'].replace('0001 總倉(2F廠務辦公室)', '')

    upload_df['transport'] = upload_df.loc[:, 'transport'].replace('', '門市自取')
    
    upload_df['date_delivery'] = upload_df.loc[:, 'date_delivery'].apply(lambda x: datetime.strptime(x, date_format))
    upload_df['date_created'] = upload_df.loc[:, 'date_created'].apply(lambda x: datetime.strptime(x, date_format))
    upload_df['date_shipping'] = None

    upload_df['time_delivery_start'] = upload_df.loc[:, 'time_delivery'].copy().apply(lambda x: x[:4])
    upload_df['time_delivery_end'] = upload_df.loc[:, 'time_delivery'].copy().apply(lambda x: x[-4:])
    upload_df['postnumber'] = ''
    upload_df['boxes'] = 1
    
    upload_df['custom'] = upload_df['custom_qty'].apply(lambda x: False if not x else True)

    upload_df['date_shipping'] = None
    upload_df['last_updated'] = datetime.now()


    print(upload_df.tail())

    return(upload_df)

