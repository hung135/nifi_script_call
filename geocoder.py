import requests
import json
import time
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

url = 'https://d1asepric093.irmnet.ds2.dhs.gov:6443/arcgis/rest/services/USA_StreetAddress2/GeocodeServer/findAddressCandidates'

world_url = 'https://igp.ice.dhs.gov/igportal/sharing/servers/8e0198bedb3a45d9af5552b739355b8d/rest/services/World/GeocodeServer'

world_url2 = 'https://igp.ice.dhs.gov/igportal/sharing/servers/e3d3f034f1ad41d7b0c27bc4aca69b40/rest/services/World/GeocodeServer'

def generate_token():
    token_url = 'https://igp.ice.dhs.gov/igportal/sharing/generateToken'
    
    data = {
            'username' : 'RAVEN0',
            'password' : 'RAVEN12@34',
            'client' : 'referer',
            'expiration' : '21600',
            'referer' : world_url2,
            'f' : 'json'
            }
    
    response = requests.post(url=token_url, data=data, verify=False)
    print(response.text)
    
def clean_raw_address(full_address_raw):
    data = {}
    data['Single Line Input'] = full_address_raw
    data['f'] = 'pjson'
    
    while True:
        try:
            response = requests.post(url, data=data, verify=False)

            j = json.loads(response.text)
            address = j['candidates'][0]['address']
            address = address.split(',')
            
            addr_dict = {}
            addr_dict['Full_Address'] = j['candidates'][0]['address']
            addr_dict['Street'] = address[0]
            addr_dict['City'] = address[1]
            addr_dict['State'] = address[2]
            addr_dict['Postal'] = address[3]
            addr_dict['Latitude'] = j['candidates'][0]['location']['x']
            addr_dict['Longitude'] = j['candidates'][0]['location']['y']
            return addr_dict
        except ConnectionError:
            print('Connection Error, trying again in 30 seconds')
            time.sleep(30)
            pass
        except (KeyError, IndexError): #Usually come up when passed a blank string
            addr_dict = {}
            addr_dict['Full_Address'] = 'TEST'
            addr_dict['Street'] = 'TEST'
            addr_dict['City'] = 'TEST'
            addr_dict['State'] = 'TEST'
            addr_dict['Postal'] = 'TEST'
            addr_dict['Latitude'] = 'TEST'
            addr_dict['Longitude'] = 'TEST'
            return addr_dict

def address_builder(street_raw, city_raw, state_raw, zip_raw):
    
    data = {}
    data['Street'] = street_raw
    data['City'] = city_raw
    data['State'] = state_raw
    data['Zip'] = zip_raw
    data['f'] = 'pjson'
    
    while True:
        try:
            response = requests.post(url, data=data, verify=False)
            j = json.loads(response.text)
            address = j['candidates'][0]['address']
            address = address.split(',')
            
            addr_dict = {}
            addr_dict['Full_Address'] = j['candidates'][0]['address']
            addr_dict['Street'] = address[0]
            addr_dict['City'] = address[1]
            addr_dict['State'] = address[2]
            addr_dict['Postal'] = address[3]
            addr_dict['Latitude'] = j['candidates'][0]['location']['x']
            addr_dict['Longitude'] = j['candidates'][0]['location']['y']
            return addr_dict
        except ConnectionError:
            print('Connection Error, trying again in 30 seconds')
            time.sleep(30)
            pass
        except (KeyError, IndexError, ValueError): #Usually come up when passed a blank string
            addr_dict = {}
            addr_dict['Full_Address'] = 'TEST'
            addr_dict['Street'] = 'TEST'
            addr_dict['City'] = 'TEST'
            addr_dict['State'] = 'TEST'
            addr_dict['Postal'] = 'TEST'
            addr_dict['Latitude'] = 'TEST'
            addr_dict['Longitude'] = 'TEST'
            return addr_dict      
    
def clean_raw_address_dict(raw_addr):
    raw_addr = {}
    raw_addr['f'] = 'pjson'
    raw_addr['SingleLine'] = raw_addr
    raw_addr['token'] = '4jRHTDpQYJb76S1dZucWSGL--q3wSdHIZ7Y11TgUPvfTpQyG4kmn_AOtFX4zAgx9hkIizD1iMPUBP-pBuIM1zYrqAYcuy1hWxzk24CtuArDW6QU9PEZcAMPYr5JX4tMglWWm4hW6tW3jQy-FkxUCJuD0t5nEjiTX0_aBJCtQzAmqdAp4CDhauRIBnzn9dPdnEhNiAHk2K-Un8F-CODke8pH7CMv-LfdrjzM_3euexdNSp9_rjYJqY_6UUVxQlhBiKGZy-uBxyBUthIfT4DwGog..'
    
    addr_dict = []
    
    while True:
        try:
            response = requests.post(world_url2, data=raw_addr, verify=False)

            j = json.loads(response.text)

        except ConnectionError:
            print('Connection Error, trying again in 30 seconds')
            time.sleep(30)
            pass
    
    for can in j['candidates']:
        address = can['address'].split(',')
        
        swap_dict = {}
        swap_dict['Full_Address'] = can['address']
        swap_dict['Street'] = address[0]
        swap_dict['City'] = address[1]
        swap_dict['State'] = address[2]
        swap_dict['Postal'] = address[3]
        swap_dict['Latitude'] = j['candidates'][0]['location']['x']
        swap_dict['Longitude'] = j['candidates'][0]['location']['y']
        
        addr_dict.append(swap_dict)
        
    return addr_dict