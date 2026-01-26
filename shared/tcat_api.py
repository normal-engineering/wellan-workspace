import httpx

API_URL = "https://api.suda.com.tw/api/Egs/"

CUSTOMER_ID = "428609240200"  # Replace with your customer ID
CUSTOMER_TOKEN = "2fn7he2h"
HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
}

async def Label_PostNumber(client, order):
    ENDPOINT = "ParsingAddress"
    address = order[['address']].rename(columns={'address' : 'Search'})
    if address is not None:
        # Now it's safe to subscript
        address = address
    data =  {
                "Content-Type": "application/json;charset=UTF-8",
                "CustomerId": CUSTOMER_ID,
                "CustomerToken": CUSTOMER_TOKEN,
                "PostType": "01",
                "PrintOBTType": "01",
                "Addresses" : address.to_dict(orient='records')
                }
    try:
        response = await client.post(url=f'{API_URL}{ENDPOINT}', json=data, headers=HEADERS, timeout=None)
        response_data = response.json()
        print(response_data['Data']['Addresses'])
        return response_data['Data']['Addresses']
    except ValueError:  # includes simplejson.decoder.JSONDecodeError
        print('Decoding JSON has failed')
        return []

async def Batch_Label_PostNumber(client, batch):
    ENDPOINT = "ParsingAddress"
    address = batch[['address']].rename(columns={'address' : 'Search'})
    if address is not None:
        # Now it's safe to subscript
        address = address
    data =  {
                "Content-Type": "application/json;charset=UTF-8",
                "CustomerId": CUSTOMER_ID,
                "CustomerToken": CUSTOMER_TOKEN,
                "PostType": "01",
                "PrintOBTType": "01",
                "Addresses" : address.to_dict(orient='records')
                }
    try:
        response = await client.post(url=f'{API_URL}{ENDPOINT}', json=data, headers=HEADERS, timeout=None)
        response_data = response.json()
        print(response_data['Data']['Addresses'])
        return response_data['Data']['Addresses']
    except ValueError:  # includes simplejson.decoder.JSONDecodeError
        print('Decoding JSON has failed')
        return []