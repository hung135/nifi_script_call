import requests
import json
import time
import dateutil
import math
from re import sub

def post_to_db(cmd):
    url = 'http://localhost:2480/batch/ariffle'

    post_data = {'transaction' : True, 'operations' : [
            {
                'type' : 'script',
                'language' : 'sql',
                'script' : [cmd]
            }
        ]
    }

    script_encoded = json.dumps(post_data)

    headers = {
            'Accept-Encoding': 'gzip,deflate'
#            'Content-Length' : bytes(len(script_encoded))
          }

    while True:
        try:
            r = requests.post(url, headers=headers, data=script_encoded, auth=('root', 'greenzonesba8a'))
            response = json.loads(r.text)
            return response
        except ConnectionError as e:
            print(e)
            print('Connection error, retrying in 30 seconds.  Error message: ' + str(e))
            time.sleep(30)

def check_columns(expected_columns, data_columns):
    missing_cols = []

    for col in expected_cols:
        if col not in data_cols:
            missing_cols.append(col)

    try:
        if len(missing_cols) > 0:
            raise ValueError
    except ValueError:
        print('Missing the following columns:')
        for d in missing_cols:
            print(d)
        raise

def get_rids():

    cmd = \
    ()

    return cmd

def graphload_ingest_event(Added_Date_Time, Added_User, Source_Type, Source_Description, Record_Owner_Operation_Name,
                                Record_Owner_Case_Number, Record_Owner_User, Record_Owner_Grand_Jury_Number,
                              Disclosure_Instructions, Restrictions_Flag, Company, UUID):


    cmd = ('UPDATE Ingest_Event SET ' +
           'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", ' +
           'Source_Type = "' + Source_Type + '", Source_Description = "' + Source_Description + '", ' +
           'Record_Owner_Operation_Name = "' + Record_Owner_Operation_Name + '", ' +
           'Record_Owner_Case_Number = "' + Record_Owner_Case_Number + '", ' +
           'Record_Owner_User  = "' + Record_Owner_User  + '", ' +
           'Record_Owner_Grand_Jury_Number = "' + Record_Owner_Grand_Jury_Number + '", ' +
           'Disclosure_Instructions  = "' + Disclosure_Instructions + '", Restrictions_Flag = "' + Restrictions_Flag + '", ' +
           'Company  = "' + Company + '", UUID = "' + UUID + '" UPSERT WHERE UUID = "' + UUID + '";\n\n')

    return cmd

#---------------------{MSB Transaction Section}-----------------------------------------------------------------------

def graphLoad_MSB_Transaction(Added_Date_Time, Added_User, Disclosure_Instructions, Restrictions_Flag, Send_Date_Time, Pay_Date_Time, Cancelled_Date_Time, Send_Amount_USD, Pay_Amount_USD, Refund_Code, Sender_Fee_Local,
                             Sender_Face_Local, Sender_Currency, Sender_Fee_USD, Sender_Total_Amount, Payee_Face_Local, Payee_Currency, Payee_Total_Amount,
                             Transaction_Status, Transaction_Type, Aggregator_Confirmation_Number, First_Transaction_Load_Date, Last_Transaction_Load_Date, Send_Date_Time_Clean, Company, Company_Number,
                             Payee_Amount_USD_Clean, Pay_Date_Time_Clean, Sender_Payment_Method, Transaction_Look_Up, Sender_Amount_USD_Clean, Event_ID, UUID):
    
    if len(Transaction_Look_Up) == 0:
        return ''

    try:
        Added_Date_Time = str(dateutil.parser.parse(Added_Date_Time))
    except:
        Added_Date_Time = '1900-01-01 00:00:00'
    try:
        Send_Date_Time = str(dateutil.parser.parse(Send_Date_Time))
    except:
        Send_Date_Time = '1900-01-01 00:00:00'
    try:
        Pay_Date_Time = str(dateutil.parser.parse(Pay_Date_Time))
    except:
        Pay_Date_Time = '1900-01-01 00:00:00'
    try:
        Cancelled_Date_Time = str(dateutil.parser.parse(Cancelled_Date_Time))
    except:
        Cancelled_Date_Time = '1900-01-01 00:00:00'
    try:
        First_Transaction_Load_Date = str(dateutil.parser.parse(First_Transaction_Load_Date))
    except:
        First_Transaction_Load_Date = '1900-01-01 00:00:00'
    try:
        Last_Transaction_Load_Date = str(dateutil.parser.parse(Last_Transaction_Load_Date))
    except:
        Last_Transaction_Load_Date = '1900-01-01 00:00:00'
    try:
        Send_Date_Time_Clean = str(dateutil.parser.parse(Send_Date_Time_Clean))
    except:
        Send_Date_Time_Clean = '1900-01-01 00:00:00'
    try:
        Pay_Date_Time_Clean = str(dateutil.parser.parse(Pay_Date_Time_Clean))
    except:
        Pay_Date_Time_Clean = '1900-01-01 00:00:00'

    if len(Send_Amount_USD) == 0:
        Send_Amount_USD = '0.00'

    if len(Pay_Amount_USD) == 0:
        Pay_Amount_USD = '0.00'

    if len(Sender_Fee_Local) == 0:
        Sender_Fee_Local = '0.00'

    if len(Sender_Face_Local) == 0:
        Sender_Face_Local = '0.00'

    if len(Sender_Fee_USD) == 0:
        Sender_Fee_USD = '0.00'

    if len(Sender_Total_Amount) == 0:
        Sender_Total_Amount = '0.00'

    if len(Payee_Amount_USD_Clean) == 0:
        Payee_Amount_USD_Clean = '0.00'

    if len(Sender_Amount_USD_Clean) == 0:
        Sender_Amount_USD_Clean = '0.00'

    if len(Payee_Total_Amount) == 0:
        Payee_Total_Amount = '0.00'

#    nums = ['Send_Amount_USD', 'Pay_Amount_USD', 'Sender_Fee_Local', 'Sender_Face_Local', 'Sender_Fee_USD', 'Sender_Total_Amount', 'Payee_Amount_USD_Clean', 'Sender_Amount_USD_Clean']

    Send_Amount_USD = sub('[$]', '', Send_Amount_USD)
    Send_Amount_USD = sub(',', '', Send_Amount_USD)
    Pay_Amount_USD = sub('[$]', '', Pay_Amount_USD)
    Pay_Amount_USD = sub(',', '', Pay_Amount_USD)
    Sender_Fee_Local = sub('[$]', '', Sender_Fee_Local)
    Sender_Fee_Local = sub(',', '', Sender_Fee_Local)
    Sender_Face_Local = sub('[$]', '', Sender_Face_Local)
    Sender_Face_Local = sub(',', '', Sender_Face_Local)
    Sender_Fee_USD = sub('[$]', '', Sender_Fee_USD)
    Sender_Fee_USD = sub(',', '', Sender_Fee_USD)
    Sender_Total_Amount = sub('[$]', '', Sender_Total_Amount)
    Sender_Total_Amount = sub(',', '', Sender_Total_Amount)
    Payee_Amount_USD_Clean = sub('[$]', '', Payee_Amount_USD_Clean)
    Payee_Amount_USD_Clean = sub(',', '', Payee_Amount_USD_Clean)
    Payee_Total_Amount = sub('[$]', '', Payee_Total_Amount)
    Payee_Total_Amount = sub(',', '', Payee_Total_Amount)
    Sender_Amount_USD_Clean = sub('[$]', '', Sender_Amount_USD_Clean)
    Sender_Amount_USD_Clean = sub(',', '', Sender_Amount_USD_Clean)

    cmd = ('UPDATE MSB_Transaction SET ' +
           'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", ' +
           'Disclosure_Instructions = "' + Disclosure_Instructions + '", Restrictions_Flag = "' + Restrictions_Flag + '", ' +
           'Send_Date_Time  = "' + Send_Date_Time + '", Pay_Date_Time = "' + Pay_Date_Time + '", ' +
           'Cancelled_Date_Time  = "' + Cancelled_Date_Time + '", Send_Amount_USD = "' + Send_Amount_USD + '", ' +
           'Pay_Amount_USD  = "' + Pay_Amount_USD + '", Refund_Code  = "' + Refund_Code + '", ' +
           'Sender_Fee_Local  = "' + Sender_Fee_Local + '", Sender_Face_Local = "' + Sender_Face_Local + '", ' +
           'Sender_Currency = "' + Sender_Currency + '", Sender_Fee_USD = "' + Sender_Fee_USD + '", ' +
           'Payee_Total_Amount  = "' + Payee_Total_Amount + '", ' +
           'Transaction_Status  = "' + Transaction_Status + '", Transaction_Type = "' + Transaction_Type + '", ' +
           'Aggregator_Confirmation_Number = "' + Aggregator_Confirmation_Number + '", ' +
           'First_Transaction_Load_Date = "' + First_Transaction_Load_Date + '", ' +
           'Last_Transaction_Load_Date  = "' + Last_Transaction_Load_Date + '", Send_Date_Time_Clean = "' + Send_Date_Time_Clean + '", Company  = "' + Company + '", Company_Number = "' + Company_Number + '", Payee_Amount_USD_Clean = "' + Payee_Amount_USD_Clean + '", '
           'Pay_Date_Time_Clean = "' + Pay_Date_Time_Clean + '", Sender_Payment_Method = "' + Sender_Payment_Method + '", ' + 'UUID = "' + UUID + '", ' +
           'Transaction_Look_Up  = "' + Transaction_Look_Up + '", Sender_Amount_USD_Clean = "' + Sender_Amount_USD_Clean + '" ' +
           'UPSERT WHERE Transaction_Look_Up = "' + Transaction_Look_Up + '";\n\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE("Has_Transaction_Record")) FROM MSB_Transaction WHERE Transaction_Look_Up = "' + Transaction_Look_Up + '";\n' +
           'LET $2 = SELECT expand(bothE("Has_Transaction_Record")) FROM Ingest_Event WHERE UUID = "' + Event_ID + '";\n' +
           'LET $3 = SELECT INTERSECT($1, $2);\n' +
           'IF($3.INTERSECT.size() == 0) {\n'
           'LET $4 = CREATE EDGE Has_Transaction_Record FROM (SELECT FROM Ingest_Event WHERE UUID = "' + Event_ID + '") TO (SELECT FROM MSB_Transaction WHERE Transaction_Look_Up = "' + Transaction_Look_Up + '");\n' +
           '}\n\n')

    return cmd

#---------------------{MSB Enity Section}-----------------------------------------------------------------------


def graphLoad_MSB_Entity(Added_Date_Time, Added_User, Entity_Type, Entity_Edge_Type, Transaction_ID, UUID):
    
    if len(UUID) == 0:
        return ''

    edge_query = 'ERROR edge_query not assigned'

    if 'Sender' in Entity_Type:
        edge_query = 'LET $4 = CREATE EDGE ' + Entity_Edge_Type + ' FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + UUID + '" AND UUID = "' + UUID + '") TO (SELECT FROM MSB_Transaction WHERE UUID = "' + Transaction_ID + '");\n'

    if 'Pay' in Entity_Type:
        edge_query = 'LET $4 = CREATE EDGE ' + Entity_Edge_Type + ' FROM (SELECT FROM MSB_Transaction WHERE UUID = "' + Transaction_ID + '") TO (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + UUID + '" AND UUID = "' + UUID + '");\n'

    cmd = ('UPDATE ' + Entity_Type + ' SET ' +
           'Add_Date_Time = "' + Added_Date_Time + '", UUID = "' + UUID + '", Add_User = "' + Added_User + '" ' +
           'UPSERT WHERE UUID = "' + UUID + '";\n\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE("' + Entity_Edge_Type +'")) FROM ' + Entity_Type + ' WHERE UUID = "' + UUID + '";\n' +
           'LET $2 = SELECT expand(bothE("' + Entity_Edge_Type +'")) FROM MSB_Transaction WHERE UUID = "' + Transaction_ID + '";\n' +
           'LET $3 = SELECT INTERSECT($1, $2);\n' +
           'IF($3.INTERSECT.size() == 0) {\n' +
            edge_query +

           '}\n\n')

    return cmd

#---------------------{Name Section and Segment}-----------------------------------------------------------------------

def graphLoad_Name(Added_Date_Time, Added_User, Name, Clean_Full_Name, Name_Clean_Prefix,
                   First_Name_Clean, Middle_Name_Clean, First_Last_Name_Clean,
                   Second_Last_Name_Clean, Name_Clean_Suffix, Name_Clean_Gender, Entity_Type, Entity_ID, UUID):

    if (len(Clean_Full_Name)) == 0:
        return ''

    cmd = ('UPDATE Name SET ' +
           'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", ' +
           'Name = "' + Name + '", Clean_Prefix  = "' + Name_Clean_Prefix + '", ' +
           'Clean_First_Name = "' + First_Name_Clean + '", Clean_Middle_Name  = "' + Middle_Name_Clean + '", ' +
           'Clean_First_Last_Name  = "' + First_Last_Name_Clean + '", UUID = "' + UUID + '", ' +
           'Clean_Second_Last_Name = "' + Second_Last_Name_Clean + '", Clean_Full_Name = "' + Clean_Full_Name + '", ' +
           'Clean_Suffix = "' + Name_Clean_Suffix + '", Clean_Gender  = "' + Name_Clean_Gender + '", UUID = "' + UUID + '" UPSERT WHERE Clean_Full_Name = "' + Clean_Full_Name + '";\n\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE(Name_Provided)) FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '";\n' +
           'LET $2 = SELECT expand(bothE(Name_Provided)) FROM Name WHERE UUID = "' + UUID + '";\n' +
           'LET $3 = SELECT INTERSECT($1, $2);\n' +
           'IF($3.INTERSECT.size() == 0) {\n' +
           'LET $4 = CREATE EDGE Name_Provided FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '") TO (SELECT FROM Name WHERE UUID = "' + UUID + '");\n' +

           '}\n\n')

    return cmd

def graphLoad_Address(Added_Date_Time, Added_User, Full_Address_Clean, Street_Clean, City_Clean, State_Clean, Postal_Code_Clean, Country_Clean, Lat, Lon, Entity_Type, Entity_ID, UUID):

    if len(Full_Address_Clean) == 0:
        return ''

    # Lat = '-86.46946426287947'
    # Lon = '30.52288671519998'

    trun_lat = str(math.floor(float(Lat) * 10 ** 4) / 10 ** 4)
    trun_lon = str(math.floor(float(Lon) * 10 ** 4) / 10 ** 4)

    cmd = ('UPDATE Address_Normalized SET ' +
           'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", Full_Address_Clean = "' + Full_Address_Clean + '", Street_Clean = "' + Street_Clean + '", City_Clean = " ' + City_Clean + '",'
           'State_Clean = "' + State_Clean + '", Latitude = "' + Lat + '", Longitude = "' + Lon + '", ' +
           'Postal_Code_Clean = "' + Postal_Code_Clean + '", Country_Clean = "' + Country_Clean + '", UUID = "' + UUID + '", Latitude = "' + Lat + '", Longitude = "' + Lon + '" ' +
           'UPSERT WHERE Full_Address_Clean = "' + Full_Address_Clean + '";\n\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE(Address_Provided_Normalized)) FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '";\n' +
           'LET $2 = SELECT expand(bothE(Address_Provided_Normalized)) FROM Address_Normalized WHERE Full_Address_Clean = "' + Full_Address_Clean + '";\n' +
           'LET $3 = SELECT INTERSECT($1, $2);\n' +
           'IF($3.INTERSECT.size() == 0) {\n' +
           'LET $4 = CREATE EDGE Address_Provided_Normalized FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '") TO (SELECT FROM Address_Normalized WHERE Full_Address_Clean = "' + Full_Address_Clean + '");\n' +

           '}\n\n')
    
    #Handle blank or Lat/Lon == 0
    if not len(Lat) == 0 or not len(Lon) == 0 or not Lat + Lon == 0:
        cmd = cmd + ('UPDATE Geocode SET Latitude = ' + trun_lat + ', Longitude = ' + trun_lon + ', Add_Date_Time = "' + Added_Date_Time + '", ' +
                     'Add_User = "' + Added_User + '", Coordinates = "' + trun_lon + ', ' + trun_lat + '" ' + 
                     'UPSERT WHERE Latitude = ' + trun_lat + ' AND Longitude = ' + trun_lon + ';\n\n')
        
        cmd = cmd + ('LET $1 = SELECT expand(bothE(Address_Provided_Geocoded)) FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '";\n' +
           'LET $2 = SELECT expand(bothE(Address_Provided_Geocoded)) FROM Geocode WHERE Latitude = ' + trun_lat + ' AND Longitude = ' + trun_lon + ';\n' +
           'LET $3 = SELECT INTERSECT($1, $2);\n' +
           'IF($3.INTERSECT.size() == 0) {\n' +
           'LET $4 = CREATE EDGE Address_Provided_Geocoded FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '") TO (SELECT FROM Geocode WHERE Latitude = ' + trun_lat + ' AND Longitude = ' + trun_lon + ');\n' +
    
           '}\n\n')
    
        cmd = cmd + ('LET $1 = SELECT expand(bothE(Address_Has_Coordinates)) FROM Address_Normalized WHERE Full_Address_Clean = "' + Full_Address_Clean + '";\n' +
           'LET $2 = SELECT expand(bothE(Address_Has_Coordinates)) FROM Geocode WHERE Latitude = ' + trun_lat + ' AND Longitude = ' + trun_lon + ';\n' +
           'LET $3 = SELECT INTERSECT($1, $2);\n' +
           'IF($3.INTERSECT.size() == 0) {\n' +
           'LET $4 = CREATE EDGE Address_Has_Coordinates FROM (SELECT FROM Address_Normalized WHERE Full_Address_Clean = "' + Full_Address_Clean + '") TO (SELECT FROM Geocode WHERE Latitude = ' + trun_lat + ' AND Longitude = ' + trun_lon + ');\n' +
    
           '}\n\n')

    return cmd

#-----Date Of Birth Section Starts Here-------------------------------------------------
def graphLoad_Date_Of_Birth(Added_Date_Time, Added_User, DOB, Date_Of_Birth_Exclude, Entity_Type, Entity_ID, UUID):

    if len(DOB) == 0:
        return ''

    d = dateutil.parser.parse(DOB)
    Date_Of_Birth_Clean = d.strftime('%m-%d-%Y')

    cmd = ('UPDATE Date_Of_Birth SET ' +
           'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", ' +
           'Date_Of_Birth_Clean = "' + Date_Of_Birth_Clean + '", UUID = "' + UUID + '" ' +
           'UPSERT WHERE Date_Of_Birth_Clean = "' + Date_Of_Birth_Clean + '";\n\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE(Date_Of_Birth_Provided)) FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '";\n' +
           'LET $2 = SELECT expand(bothE(Date_Of_Birth_Provided)) FROM Date_Of_Birth WHERE Date_Of_Birth_Clean = "' + Date_Of_Birth_Clean + '";\n' +
           'LET $3 = SELECT INTERSECT($1, $2);\n' +
           'IF($3.INTERSECT.size() == 0) {\n' +
           'LET $4 = CREATE EDGE Date_Of_Birth_Provided FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '") TO (SELECT FROM Date_Of_Birth WHERE Date_Of_Birth_Clean = "' + Date_Of_Birth_Clean + '");\n' +

           '}\n\n')

    return cmd

def graphLoad_Phone_Normalized(Added_Date_Time, Added_User, E164, Country, Entity_Type, Entity_ID, UUID):

    if len(E164) == 0:
        return ''

    cmd = ('UPDATE Phone_Normalized SET ' +
            'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", ' +
            'E164 = "' + E164 + '", Country = "' + Country + '", UUID = "' + UUID + '" UPSERT WHERE E164 = "' + E164 + '";\n\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE(Phone_Provided_Normalized)) FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '";\n' +
       'LET $2 = SELECT expand(bothE(Phone_Provided_Normalized)) FROM Phone_Normalized WHERE UUID = "' + UUID + '";\n' +
       'LET $3 = SELECT INTERSECT($1, $2);\n' +
       'IF($3.INTERSECT.size() == 0) {\n' +
       'LET $4 = CREATE EDGE Phone_Provided_Normalized FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID ="' + Entity_ID + '") TO (SELECT FROM Phone_Normalized WHERE UUID = "' + UUID + '");\n' +

       '}\n\n')

    return cmd

#----Company_Customer_ID Section Starts Here---------------------------------------------------------
def graphLoad_Company_Customer_ID(Added_Date_Time, Added_User, Company_Customer_ID, Online_Customer_Flag, Entity_Type, Entity_ID, UUID):

    if len(Company_Customer_ID) == 0:
        return ''

    cmd = ('UPDATE Company_Customer_ID SET ' +
           'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", ' +
           'Company_Customer_ID = "' + Company_Customer_ID + '", Online_Customer_Flag = "' + Online_Customer_Flag  +'", UUID = "' + UUID + '", UUID = "' + UUID + '" UPSERT WHERE Company_Customer_ID = "' + Company_Customer_ID + '";\n\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE(Company_Customer_ID_Captured)) FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '";\n' +
   'LET $2 = SELECT expand(bothE(Company_Customer_ID_Captured)) FROM Company_Customer_ID WHERE Company_Customer_ID = "' + Company_Customer_ID + '";\n' +
   'LET $3 = SELECT INTERSECT($1, $2);\n' +
   'IF($3.INTERSECT.size() == 0) {\n' +
   'LET $4 = CREATE EDGE Company_Customer_ID_Captured FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '") TO (SELECT FROM Company_Customer_ID WHERE Company_Customer_ID = "' + Company_Customer_ID + '");\n' +

   '}\n\n')

    return cmd

#----Credit_Card Section Starts Here--------------------------------------------

def graphLoad_Credit_Card(Added_Date_Time, Added_User, Credit_Card_Number, Credit_Card_Number_Exclude, Entity_Type, Entity_ID, UUID, Customer_ID, Online_Flag):

    Credit_Card_Number = Credit_Card_Number.replace(' ', '')
    Credit_Card_Number = Credit_Card_Number.replace('-', '')

    if len(Credit_Card_Number) == 0:
        return ''

    cmd = ('UPDATE Credit_Card SET ' +
           'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", ' +
           'Card_Number = "' + Credit_Card_Number + '", Credit_Card_Exclude = "' + Credit_Card_Number_Exclude + '", ' +
           'UUID = "' + UUID + '" ' +
           'UPSERT WHERE Card_Number = "' + Credit_Card_Number + '";\n\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE(Credit_Card_Provided)) FROM ' + Entity_Type + ' WHERE UUID = "' +  Entity_ID + '";\n' +
       'LET $2 = SELECT expand(bothE(Credit_Card_Provided)) FROM Credit_Card WHERE Card_Number = "' + Credit_Card_Number + '";\n' +
       'LET $3 = SELECT INTERSECT($1, $2);\n' +
       'IF($3.INTERSECT.size() == 0) {\n' +
       'LET $4 = CREATE EDGE Credit_Card_Provided FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '") TO (SELECT FROM Credit_Card WHERE Card_Number = "' + Credit_Card_Number + '");\n' +

       '}\n\n')

    return cmd

#----Occupation Section Starts Here-----------------------------------------------------------

def graphLoad_Occupation(Added_Date_Time, Added_User, Occupation, Entity_Type, Entity_ID, UUID):

    if len(Occupation) == 0:
        return ''

    cmd = ('UPDATE Occupation SET ' + 'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", ' + 'Occupation = "' + Occupation + '", UUID = "' + UUID + '" ' +
           'UPSERT WHERE Occupation = "' + Occupation + '";\n\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE(Occupation_Provided)) FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '";\n' +
       'LET $2 = SELECT expand(bothE(Occupation_Provided)) FROM Occupation WHERE Occupation = "' + Occupation + '";\n' +
       'LET $3 = SELECT INTERSECT($1, $2);\n' +
       'IF($3.INTERSECT.size() == 0) {\n' +
       'LET $4 = CREATE EDGE Occupation_Provided FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '") TO (SELECT FROM Occupation WHERE Occupation = "' + Occupation + '");\n' +

       '}\n\n')

    return cmd

#----Loyalty_Card Section Starts Here-----------------------------------------------------

def graphLoad_Loyalty_Number(Added_Date_Time, Added_User, Company, Loyalty_Card_Number, Loyalty_Card_Look_Up, Loyalty_Card_Exclude, Entity_Type, Entity_ID, UUID):

    if len(Loyalty_Card_Number) == 0:
        return ''

    cmd = ('UPDATE Company_Loyalty_Number SET ' +
           'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", Company = "' + Company + '", ' +
           'Card_Number = "' + Loyalty_Card_Number + '", Loyalty_Card_Look_Up = "' + Loyalty_Card_Look_Up + '", ' +
           'Loyalty_Card_Exclude = "' + Loyalty_Card_Exclude + '", UUID = "' + UUID + '" ' +
           'UPSERT WHERE Loyalty_Card_Look_Up = "' + Loyalty_Card_Look_Up + '";\n\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE(Loyalty_Number_Provided)) FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '";\n' +
       'LET $2 = SELECT expand(bothE(Loyalty_Number_Provided)) FROM Company_Loyalty_Number WHERE Loyalty_Card_Look_Up = "' + Loyalty_Card_Look_Up + '";\n' +
       'LET $3 = SELECT INTERSECT($1, $2);\n' +
       'IF($3.INTERSECT.size() == 0) {\n' +
       'LET $4 = CREATE EDGE Loyalty_Number_Provided FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '") TO (SELECT FROM Company_Loyalty_Number WHERE Loyalty_Card_Look_Up = "' + Loyalty_Card_Look_Up + '");\n' +

       '}\n\n')

    return cmd

#----Bank_Account Section Starts Here-----------------------------------------------------

def graphLoad_Bank_Account(Added_Date_Time, Added_User, Bank_Account_Number, Bank_Account_Look_Up, Bank_Account_Exclude,
                           Bank_Account_Type, Holder_Name, Bank_Name, BIC, Bank_Rounting_Number, Bank_Internal_Customer_ID,
                           Bank_ID, Bank_City, Branch_Name, Branch_Code, Bank_Card_IIN, Entity_Type, Entity_ID, UUID):

    if len(Bank_Account_Number) == 0:
        return ''

    cmd = ('UPDATE Bank_Account SET ' +
           'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", ' +
           'Bank_Account_Number = "' + Bank_Account_Number + '", Bank_Account_Look_Up = "' + Bank_Account_Look_Up + '", ' +
           'Bank_Account_Exclude = "' + Bank_Account_Exclude + '", Bank_Account_Type = "' + Bank_Account_Type + '", ' +
           'Bank_Account_Holder_Name = "' + Holder_Name + '", BIC = "' + BIC + '", ' +
           'Bank_Routing_Number  = "' + Bank_Rounting_Number  + '", Bank_Internal_Customer_ID = "' + Bank_Internal_Customer_ID + '", ' +
           'Bank_ID = "' + Bank_ID + '", Bank_City = "' + Bank_City + '", ' +
           'Bank_Branch_Name  = "' + Branch_Name + '", Bank_Branch_Code = "' + Branch_Code + '", Bank_Card_IIN = "' + Bank_Card_IIN + '" ' +
           'UPSERT WHERE Bank_Account_Look_Up = "' + Bank_Account_Look_Up + '";\n\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE(Bank_Account_Provided)) FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '";\n' +
       'LET $2 = SELECT expand(bothE(Bank_Account_Provided)) FROM Bank_Account WHERE Bank_Account_Look_Up = "' + Bank_Account_Look_Up + '";\n' +
       'LET $3 = SELECT INTERSECT($1, $2);\n' +
       'IF($3.INTERSECT.size() == 0) {\n' +
       'LET $4 = CREATE EDGE Bank_Account_Provided FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '") TO (SELECT FROM Bank_Account WHERE Bank_Account_Look_Up = "' + Bank_Account_Look_Up + '");\n'

       '}\n\n')

    return cmd

#----Identification Section Starts Here---------------------------------------------------

def graphLoad_Identification(Added_Date_Time, Added_User, ID_Number, ID_Type, ID_Description,
                           ID_Location, ID_State, ID_Country, ID_Look_Up, ID_Exclude, Entity_Type, Entity_ID, UUID):

    if len(ID_Number) == 0:
        return ''

    cmd = ('UPDATE Identification_Number SET ' +
           'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", ' +
           'ID_Number = "' + ID_Number + '", Type = "' + ID_Type + '", UUID = "' + UUID + '", ' +
           'Description = "' + ID_Description + '", Location = "' + ID_Location + '", ' +
           'State = "' + ID_State + '", Country = "' + ID_Country + '", Look_Up = "' + ID_Look_Up + '", ' +
           'Exclude  = "' + ID_Exclude + '" UPSERT WHERE ID_Number = "' + ID_Number + '";\n\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE(Identification_Provided)) FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '";\n' +
       'LET $2 = SELECT expand(bothE(Identification_Provided)) FROM Identification_Number WHERE ID_Number = "' + ID_Number + '";\n' +
       'LET $3 = SELECT INTERSECT($1, $2);\n' +
       'IF($3.INTERSECT.size() == 0) {\n' +
       'LET $4 = CREATE EDGE Identification_Provided FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '") TO (SELECT FROM Identification_Number WHERE ID_Number = "' + ID_Number + '");\n' +
       '}\n\n')

    return cmd

#----IP_Address section Starts Here---------------------------------------------------

def graphLoad_IP_Address(Added_Date_Time, Added_User, IP_Address, IP_Address_Exclude, Entity_Type, Entity_ID, UUID):

    if len(IP_Address) == 0:
        return ''

    cmd = ('UPDATE IP_Address SET ' +
       'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", UUID = "' + UUID + '", ' +
       'Address = "' + IP_Address + '", Exclude = "' + IP_Address_Exclude + '" ' +
       'UPSERT WHERE Address = "' + IP_Address + '";\n\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE(IP_Address_During_Transaction)) FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '";\n' +
       'LET $2 = SELECT expand(bothE(IP_Address_During_Transaction)) FROM IP_Address WHERE Address = "' + IP_Address + '";\n' +
       'LET $3 = SELECT INTERSECT($1, $2);\n' +
       'IF($3.INTERSECT.size() == 0) {\n' +
       'LET $4 = CREATE EDGE IP_Address_During_Transaction FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '") TO (SELECT FROM IP_Address WHERE Address = "' + IP_Address + '");\n' +

       '}\n\n')

    return cmd

#----Device section Starts Here---------------------------------------------------

def graphLoad_Device(Added_Date_Time, Added_User, Device_Description, Device_OS, Device_ID, Device_Exclude, Entity_Type, Entity_ID, UUID):

    if len(Device_ID) == 0:
        return ''

    cmd = ('UPDATE Electronic_Device SET ' +
           'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", ' +
           'Device_ID = "' + Device_ID + '", Exclude = "' + Device_Exclude + '", ' +
           'Description = "' + Device_Description + '", Operating_System = "' + Device_OS + '", UUID = "' + UUID + '" ' +
           'UPSERT WHERE Device_ID = "' + Device_ID + '";\n\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE(Device_Used_During_Transaction)) FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '";\n' +
       'LET $2 = SELECT expand(bothE(Device_Used_During_Transaction)) FROM Electronic_Device WHERE Device_ID = "' + Device_ID + '";\n' +
       'LET $3 = SELECT INTERSECT($1, $2);\n' +
       'IF($3.INTERSECT.size() > 0) {\n' +
       'LET $4 = CREATE EDGE Device_Used_During_Transaction FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '") TO (SELECT FROM Electronic_Device WHERE Device_ID = "' + Device_ID + '") SET Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '";\n' +

       '}\n\n')

    return cmd

#---Email section Starts Here---------------------------------------------------

def graphLoad_Email(Added_Date_Time, Added_User, Email, Email_Exclude, Entity_Type, Entity_ID, UUID):

    if len(Email) == 0:
        return ''

    Email = Email.lower()

    cmd = ('UPDATE Email SET ' +
           'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", ' +
           'Email = "' + Email + '", Exclude = "' + Email_Exclude + '", UUID = "' + UUID + '" ' +
           'UPSERT WHERE Email = "' + Email + '";\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE(Email_Provided)) FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '";\n' +
       'LET $2 = SELECT expand(bothE(Email_Provided)) FROM Email WHERE Email = "' + Email + '";\n' +
       'LET $3 = SELECT INTERSECT($1, $2);\n' +
       'IF($3.INTERSECT.size() == 0) {\n' +
       'LET $4 = CREATE EDGE Email_Provided FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '") TO (SELECT FROM Email WHERE Email = "' + Email + '");\n' +
       '}\n\n')

    return cmd

#---Agent section Starts Here---------------------------------------------------

def graphLoad_Agent(Added_Date_Time, Added_User, Agent_Number, Agent_Name, Agent_Type, Entity_Type, Entity_ID, UUID):

    if len(Agent_Number) + len(Agent_Name) == 0:
        return ''

    cmd = ('UPDATE Agent SET ' +
           'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", ' +
           'Agent_Number = "' + Agent_Number + '", Agent_Type = "' + Agent_Type + '", ' +
           'Agent_Name = "' + Agent_Name + '", UUID = "' + UUID + '" ' +
           'UPSERT WHERE Agent_Number = "' + Agent_Number + '";\n\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE(Agent_Conducting_Transaction)) FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '";\n' +
       'LET $2 = SELECT expand(bothE(Agent_Conducting_Transaction)) FROM Agent WHERE Agent_Name = "' + Agent_Name + '";\n' +
       'LET $3 = SELECT INTERSECT($1, $2);\n' +
       'IF($3.INTERSECT.size() == 0) {\n' +
       'LET $4 = CREATE EDGE Agent_Conducting_Transaction FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '") TO (SELECT FROM Agent WHERE UUID = "' + UUID + '");\n' +

       '}\n\n')

    return cmd

#---Clerk section Starts Here---------------------------------------------------

def graphLoad_Clerk(Added_Date_Time, Added_User, Agent_Clerk, Agent_Number, Agent_Name, Agent_Type, Entity_Type, Entity_ID, UUID):

    if len(Agent_Clerk) == 0:
        return ''

    cmd = ('UPDATE Clerk SET ' +
           'Add_Date_Time = "' + Added_Date_Time + '", Add_User = "' + Added_User + '", ' +
           'Agent_Number = "' + Agent_Number + '", Agent_Type = "' + Agent_Type + '", ' +
           'Agent_Name = "' + Agent_Name + '", Agent_Clerk = "' + Agent_Clerk + '", UUID = "' + UUID + '" ' +
           'UPSERT WHERE Agent_Clerk = "' + Agent_Clerk + '";\n')

    cmd = cmd + ('LET $1 = SELECT expand(bothE(Clerk_Conducting_Transaction)) FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '";\n' +
                   'LET $2 = SELECT expand(bothE(Clerk_Conducting_Transaction)) FROM MSB_Clerk WHERE UUID = "' + UUID + '";\n' +
                   'LET $3 = SELECT INTERSECT($1, $2);\n' +
                   'IF($3.INTERSECT.size() == 0) {\n' +
                   'LET $4 = CREATE EDGE Clerk_Conducting_Transaction FROM (SELECT FROM ' + Entity_Type + ' WHERE UUID = "' + Entity_ID + '") TO (SELECT FROM Clerk WHERE Agent_Clerk = "' + Agent_Clerk + '");\n' +

                   '}\n\n')

    return cmd
