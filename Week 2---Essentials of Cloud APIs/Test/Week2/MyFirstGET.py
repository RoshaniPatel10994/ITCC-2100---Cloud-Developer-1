import requests

def PostItem(url, item):
    #Save to DynamoDb table
    try:
        requests.post(url = url, json = item)
    except:
        print('Error saving item.')
    
    print ('Successfully saved item!')


def GetItem(url):
    #Define params dictionary for parameters to API
    PARAMS = {'pk':'b1','sk':'Thiamine'}
    #send get requests and save ersponse
    r=requests.get(url=url, params=PARAMS)
    #extracting data in json format
    item=r.json()
    return item

def DeleteItem(url,item):
    #Delete from DynamoDb table
    try:
        retVal = requests.delete(url=url, json=item)
        if retVal.ok == True:
          print ('Successfully deleted item!')
    except:
        print('Error deleting item.')



def main():
    #URL to use function
    URL = "https://0jhr0r0epf.execute-api.us-east-2.amazonaws.com/itcc2100/items"
    #What goes into the database

    item = {
    "pk": "b1",
    "sk": "Thiamine",
    "Vendor": "Now Foods",
    "Title": "Now Foods Thiamine B1 Energy Support",
    "Description": "Vitamin B1, called Thiamine. Thiamine Hydrochloride, C12H17N4OS+",
    "PrimaryUnits": "Pills",
    "PrimaryUnitsPerBottle": 100,
    "DoseUnit": "mg",
    "DosePerPrimaryUnit": 100,
    "Price Units": "USD",
    "Price Per Primary Unit": 19.99,
    "Country Sold in": "Worldwide"
    }

    #Post item
    #PostItem(URL,item)

    #Get item
    #data = GetItem(URL)
    
    #Delete item
    item2 = {
    "pk": "b1",
    "sk": "Thiamine"
    }
    DeleteItem(URL,item2)


main()