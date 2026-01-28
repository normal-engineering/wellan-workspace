orders = upload_df[[
    'sub',
    'main',
    'date_created',
    'customer_no',
    'creator',
    'customer',
    'dept_sales',
    'dept_fulfillment',
    'dept_shipping',
    'dept_pickup',
    'transport',
    'thermo',
    'status',
    'time_delivery_start',
    'time_delivery_end',
    'date_delivery',
    'date_shipping',
    'comment_1',
    'comment_2',
    'comment_3',
    'telephone_day',
    'mobile',
    'recipient',
    'custom',
    'last_updated'
]]

order_address = upload_df.query('transport == "黑貓宅急便"')[[
    'sub',
    'address',
    'postnumber'
]]

full_order_address = upload_df[[
    'sub',
    'address',
    'postnumber'
]]

order_items = upload_df[[
    'sub',
    'sku',
    'product',
    'qty',
    'comment'
]]
order_custom = upload_df.query('custom == True')[[

    'sub',
    'custom_sku',
    'custom_product',
    'custom_qty',
]]
order_tracking = upload_df.query('transport == "黑貓宅急便"')[[
    'sub'
]]

# print(len(orders['sub'].drop_duplicates()))
# print(len(full_order_address.drop_duplicates()))

# print(len(order_address.drop_duplicates()))
# print(len(order_tracking.drop_duplicates()))

# print(len(orders.query('custom == True')))
# print(len(order_custom))
