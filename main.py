import os
os.chdir(r"/data/fast/hortonworks/msb_script_test")

import sys
import datetime
import pandas as pd
import msb_ingester as ig
import geocoder as gc
import phone_cleaner as pc
import time
import hashlib
import uuid

# ----------------------------------------------------------------------------------------
#---- Main function starts here ----------------------------------------------------------
# ----------------------------------------------------------------------------------------
#Setup the data map dictionary

entities = {}
Added_Date_Time = str(datetime.datetime.now().replace(second=0, microsecond=0))
Added_User = 'ARiffle'
Source_Type = 'test'
Source_Description = 'Ben'
Record_Owner_Operation_Name = 'Not sure'
Record_Owner_Case_Number = '1234'
Record_Owner_User = 'ARiffle'
Record_Owner_Grand_Jury_Number ='1234'
Disclosure_Instructions ='Tell everyone'
Restrictions_Flag = 'False'
Company = 'Greenzone Solutions'

#Reset output file
output_file = open('output.txt', 'w')
output_file.write('')
output_file.close()

#Reset errors file
error_file = open('errors.txt', 'w')
error_file.write('')
error_file.close()

post_max_size = 10
count = 0
total_time = 0
print(str(datetime.datetime.now()))

query = ('TRUNCATE CLASS E POLYMORPHIC UNSAFE;\n' +
       'TRUNCATE CLASS V POLYMORPHIC UNSAFE;\n\n')

# query = ''

Event_ID = 'event-' + str(uuid.uuid1())
query = query + ig.graphload_ingest_event(Added_Date_Time, Added_User, Source_Type, Source_Description, Record_Owner_Operation_Name, Record_Owner_Case_Number, Record_Owner_User,
                                      Record_Owner_Grand_Jury_Number, Disclosure_Instructions, Restrictions_Flag, Company, Event_ID)

flowfile = sys.stdin

#Pull the user data from incoming MSB document
infile = pd.read_csv(flowfile, na_filter=False, chunksize=1000, sep=',', dtype=object)

###################################Check for all required functions#####################################
#chunk = infile.getchunk(1)
#data_cols = list(chunk.columns.values)
#expected_cols = pd.read_csv('expected_cols.csv', na_filter=False, dtype=object, sep=',')
#
#override_column_check = False
#
#if override_column_check == False:
#    ig.check_columns(expected_cols, data_cols)

###############Geocoder section##########################
#for chunk in infile:
#    for line in chunk:
#		addresses=\
#		{           "records": [
#				{
#					"attributes": {
#						"OBJECTID": 1,
#						"SingleLine": "380 New York St., Redlands, CA, 92373"
#					}
#				},
#		   # {
#					# "attributes": {
#						# "OBJECTID": 2,
#						# "SingleLine": "1 World Way, Los Angeles, CA, 90045"
#					# }
#				# }
#			]
#		}

#Loop through the MSB Document to add the MSB_Import_Event
for chunk in infile:
    for index, line in chunk.iterrows():
#----Start of Transaction Section---------------------------------------------------------------------------------

        if count % post_max_size == 0:
            start_time = time.time()

        Transaction_ID = 'tx-' + str(hashlib.sha256(line['transactionLookUp'].encode('utf-8')).hexdigest())

        query = query + ig.graphLoad_MSB_Transaction(Added_Date_Time, Added_User, line['disclosureInstructions'],
                         Restrictions_Flag , line['sendDateTime'], line['payDateTime'],
                         line['canceledDateTime'], line['sAmountUSD'], line['pAmountUSD'], line['refundCode'], line['sFeeLocal'],
                         line['sFaceLocal'], line['sCurrency'], line['sFeeUSD'], line['sTotalAmount'], line['pFaceLocal'],
                         line['pCurrency'], line['pTotalAmount'],
                         line['transactionStatus'], line['transactionType'], line['aggregatorConfirmNum'],
                         line['firstTransLoadDate'], line['lastTransLoadDate'], line['sendDateTime_Clean'], line['company'], line['controlNumber'],
                         line['pAmountUSD'], line['payDateTime_Clean'], line['sPaymentMethod'], line['transactionLookUp'],
                         line['sAmountUSD_Clean'], Event_ID, Transaction_ID)

#    #----END of Transaction Section--------------------------------------------------------------------------------------
#    #--------------------------------------------------------------------------------------------------------------------
#    #----Start of Sender Section-----------------------------------------------------------------------------------------

        #Entity
        Entity_ID = 'entity-' + str(uuid.uuid1())
        Sender_Entity_UUID = Entity_ID
        Entity_Type = 'MSB_Sender_Entity'
        Entity_Edge_Type = 'MSB_Transaction_Sender'
        query = query + ig.graphLoad_MSB_Entity(Added_Date_Time, Added_User, Entity_Type, Entity_Edge_Type, Transaction_ID, Entity_ID)

        #Name
        Name_ID = 'name-' + str(hashlib.sha256(line['sNameClean_fullName'].title().encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Name(Added_Date_Time, Added_User, line['sName'], line['sNameClean_fullName'], line['sNameClean_prefix'],
                  line['sNameClean_firstName'], line['sNameClean_middleName'], line['sNameClean_firstLName'],
                  line['sNameClean_secondLName'], line['sNameClean_suffix'], line['sNameClean_gender'], Entity_Type, Entity_ID, Name_ID)

        #Address
#        addr_dict = gc.address_builder(line['sAddress'], line['sCity'], line['sState'], line['sPostalCode'])
#        Address_ID = 'address-' + str(hashlib.sha256(addr_dict['Full_Address'].encode('utf-8')).hexdigest())
#        addr_dict['Country'] = 'Merica'
#        query = query + ig.graphLoad_Address(Added_Date_Time, Added_User, addr_dict['Full_Address'], addr_dict['Street'], addr_dict['City'],
#             addr_dict['State'], addr_dict['Postal'], addr_dict['Country'], addr_dict['Latitude'], addr_dict['Longitude'], Entity_Type, Entity_ID, Address_ID)


        Address_ID = 'address-' + str(hashlib.sha256(line['sAddrClean_fullAddress'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Address(Added_Date_Time, Added_User, str(line['sAddrClean_numbers'] + ' ' + line['sAddrClean_fullAddress'] + ' ' + line['sAddrClean_streetDirection'] + ' ' + line['sAddrClean_streetType']), line['sAddrClean_streetName'], line['sAddrClean_city'], line['sAddrClean_state'], line['sAddrClean_postalCode'], line['sAddrClean_country'], line['sAddrClean_latitude'], line['sAddrClean_longitude'], Entity_Type, Entity_ID, Address_ID)

        #DOB
        DOB_ID = 'dob-' + str(hashlib.sha256(line['sDOBClean'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Date_Of_Birth(Added_Date_Time, Added_User, line['sDOBClean'], line['sDOB_exclude'], Entity_Type, Entity_ID, DOB_ID)

        #Phone
        phone_dict = pc.normalize_single_number(line['sPhoneNumber'])
        Phone_Cleaned_ID = 'phone-' + str(hashlib.sha256(phone_dict['E164'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Phone_Normalized(Added_Date_Time, Added_User, phone_dict['E164'], phone_dict['Country'], Entity_Type, Entity_ID, Phone_Cleaned_ID)

        #Customer ID and Credit Card, with handling for offline and online customers
        if len(line['sCustomerID_online']) > 0: #Online flag = True
            Customer_ID = 'customerID-' + str(hashlib.sha256(line['sCustomerID_online'].encode('utf-8')).hexdigest())
            query = query + ig.graphLoad_Company_Customer_ID(Added_Date_Time, Added_User, line['sCustomerID_online'], 'true', Entity_Type, Entity_ID, Customer_ID)
            Credit_Card_ID = 'creditcard-' + str(hashlib.sha256(line['sCreditCardNumber'].encode('utf-8')).hexdigest())
            query = query + ig.graphLoad_Credit_Card(Added_Date_Time, Added_User, line['sCreditCardNumber'], line['sCreditCardNum_exclude'], Entity_Type, Entity_ID, Credit_Card_ID, line['sCustomerID_online'], 'true')
        if len(line['sCustomerID_online']) == 0: #Online flag = False
            Customer_ID = 'person-' + str(hashlib.sha256(line['sCustomerID'].encode('utf-8')).hexdigest())
            query = query + ig.graphLoad_Company_Customer_ID(Added_Date_Time, Added_User, line['sCustomerID'], 'false', Entity_Type, Entity_ID, Customer_ID)
            Credit_Card_ID = 'creditcard-' + str(hashlib.sha256(line['sCreditCardNumber'].encode('utf-8')).hexdigest())
            query = query + ig.graphLoad_Credit_Card(Added_Date_Time, Added_User, line['sCreditCardNumber'], line['sCreditCardNum_exclude'], Entity_Type, Entity_ID, Credit_Card_ID, line['sCustomerID'], 'false')

        #Occupation
        Occupation_ID = 'occupation-' + str(hashlib.sha256(line['sOccupation'].title().encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Occupation(Added_Date_Time, Added_User, line['sOccupation'], Entity_Type, Entity_ID, Occupation_ID)

        #Loyalty ID
        Loyalty_ID = 'misc-' + str(hashlib.sha256(line['sLoyaltyLookUp'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Loyalty_Number(Added_Date_Time, Added_User, line['company'], line['sLoyaltyCardNumber'], line['sLoyaltyLookUp'], line['sLoyalty_exclude'], Entity_Type, Entity_ID, Loyalty_ID)

        #Bank Account
        Bank_Account_ID = 'bankaccount-' + str(hashlib.sha256(line['sBankAcnt_LookUp'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Bank_Account(Added_Date_Time, Added_User, line['sBank_AccountNum'], line['sBankAcnt_LookUp'], line['sBankAcnt_exclude'], line['sBank_AccountType'], line['sBank_AccountHolderName'], line['sBank_Name'], line['sBank_BIC'],
                                                  line['sBank_RtnNum'], line['sBank_CustomerID'], line['sBank_ID'], line['sBank_City'], line['sBank_BranchName'], line['sBank_BranchCode'], line['sBank_CardIIN'], Entity_Type, Entity_ID, Bank_Account_ID)

        #----Identification_1----
        Identification_ID = 'id-' + str(hashlib.sha256(line['sID1LookUp'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Identification(Added_Date_Time, Added_User, line['sID1Number'], line['sID1Type'],
                            line['sID1Description'], line['sID1Location'], line['sID1State'], line['sID1Country'], line['sID1LookUp'], line['sID1_exclude'], Entity_Type, Entity_ID, Identification_ID)

        #----Identification_2----
        Identification_ID = 'id-' + str(hashlib.sha256(line['sID2LookUp'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Identification(Added_Date_Time, Added_User, line['sID2Number'], line['sID2Type'],
                            line['sID2Description'], line['sID2Location'], line['sID2State'], line['sID2Country'], line['sID2LookUp'], line['sID2_exclude'], Entity_Type, Entity_ID, Identification_ID)

        #----IP_Address_1----
        IP_Address_ID = 'ip-' + str(hashlib.sha256(line['sIPAddr1'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_IP_Address(Added_Date_Time, Added_User, line['sIPAddr1'], line['sIPAddr1_exclude'],
                        Entity_Type, Entity_ID, IP_Address_ID)

        #----IP_Address_2----
        IP_Address_ID = 'ip-' + str(hashlib.sha256(line['sIPAddr2'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_IP_Address(Added_Date_Time, Added_User, line['sIPAddr2'], line['sIPAddr2_exclude'],
                        Entity_Type, Entity_ID, IP_Address_ID)

        #----Device_1----
        Device_ID = 'device-' + str(hashlib.sha256(line['sDeviceID1'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Device(Added_Date_Time, Added_User, line['sDeviceDesc1'], line['sDeviceOS1'],
                    line['sDeviceID1'], line['sDevice1_exclude'], Entity_Type, Entity_ID, Device_ID)

        #----Device_2----
        Device_ID = 'device-' + str(hashlib.sha256(line['sDeviceID2'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Device(Added_Date_Time, Added_User, line['sDeviceDesc2'], line['sDeviceOS2'],
                    line['sDeviceID2'], line['sDevice2_exclude'], Entity_Type, Entity_ID, Device_ID)

        #----Email_1----
        Email_ID = 'email-' + str(hashlib.sha256(line['sEMail1'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Email(Added_Date_Time, Added_User, line['sEMail1'], line['sEmail1_exclude'], Entity_Type, Entity_ID, Email_ID)

        #----Email_2----
        Email_ID = 'email-' + str(hashlib.sha256(line['sEMail2'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Email(Added_Date_Time, Added_User, line['sEMail2'], line['sEmail2_exclude'], Entity_Type, Entity_ID, Email_ID)

    #----END of Sender Section--------------------------------------------------------------------------------------
    #---------------------------------------------------------------------------------------------------------------
    #----Start of Payee Section-------------------------------------------------------------------------------------

        #Entity
        Entity_ID = 'entity-' + str(uuid.uuid1())
        Entity_Type = 'MSB_Payee_Entity'
        Entity_Edge_Type = 'MSB_Transaction_Payee'
        query = query + ig.graphLoad_MSB_Entity(Added_Date_Time, Added_User, Entity_Type, Entity_Edge_Type, Transaction_ID, Entity_ID)

        #Name
        Name_ID = 'name-' + str(hashlib.sha256(line['pNameClean_fullName'].title().encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Name(Added_Date_Time, Added_User, line['pName'], line['pNameClean_fullName'], line['pNameClean_prefix'],
                   line['pNameClean_firstName'], line['pNameClean_middleName'], line['pNameClean_firstLName'],
                   line['pNameClean_secondLName'], line['pNameClean_suffix'], line['pNameClean_gender'], Entity_Type, Entity_ID, Name_ID)

        #Address
#        addr_dict = gc.address_builder(line['pAddress'], line['pCity'], line['pState'], line['pPostalCode'])
#        Address_ID = 'str_addr' + str(hashlib.sha256(addr_dict['Full_Address'].encode('utf-8')).hexdigest())
#        addr_dict['Country'] = 'Merica'
#        query = query + ig.graphLoad_Address(Added_Date_Time, Added_User, addr_dict['Full_Address'], addr_dict['Street'], addr_dict['City'],
#             addr_dict['State'], addr_dict['Postal'], addr_dict['Country'], addr_dict['Latitude'], addr_dict['Longitude'], Entity_Type, Entity_ID, Address_ID)

        Address_ID = 'str_addr' + str(uuid.uuid1())
        query = query + ig.graphLoad_Address(Added_Date_Time, Added_User, line['pAddrClean_fullAddress'], line['pAddrClean_numbers'] + ' ' + line['pAddrClean_streetName'] + ' ' + line['pAddrClean_streetDirection'] + ' ' + line['pAddrClean_streetType'], line['pAddrClean_city'], line['pAddrClean_state'], line['pAddrClean_city'], line['pAddrClean_postalCode'], line['pAddrClean_latitude'], line['pAddrClean_longitude'], Entity_Type, Entity_ID, Address_ID)

        #DOB
        DOB_ID = 'dob-' + str(hashlib.sha256(line['pDOBClean'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Date_Of_Birth(Added_Date_Time, Added_User, line['pDOBClean'],
                  line['pDOB_exclude'], Entity_Type, Entity_ID, DOB_ID)

        #Phone
        phone_dict = pc.normalize_single_number(line['pPhoneNumber'])
        Phone_ID = 'phone-' + str(hashlib.sha256(phone_dict['E164'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Phone_Normalized(Added_Date_Time, Added_User, phone_dict['E164'], phone_dict['Country'], Entity_Type, Entity_ID, Phone_ID)

        #Customer ID and Credit Card with handling for offline and online customers
        if len(line['pCustomerID_online']) > 0: #Online flag = True
            Customer_ID = 'person-' + str(hashlib.sha256(line['pCustomerID_online'].encode('utf-8')).hexdigest())
            query = query + ig.graphLoad_Company_Customer_ID(Added_Date_Time, Added_User, line['pCustomerID_online'], 'true', Entity_Type, Entity_ID, Customer_ID)
            Credit_Card_ID = 'creditcard-' + str(hashlib.sha256(line['pCreditCardNumber'].encode('utf-8')).hexdigest())
            query = query + ig.graphLoad_Credit_Card(Added_Date_Time, Added_User, line['pCreditCardNumber'], line['pCreditCardNum_exclude'], Entity_Type, Entity_ID, Credit_Card_ID, line['pCustomerID_online'], 'true')
        if len(line['pCustomerID_online']) == 0: #Online flag = False
            Customer_ID = 'person-' + str(hashlib.sha256(line['pCustomerID'].encode('utf-8')).hexdigest())
            query = query + ig.graphLoad_Company_Customer_ID(Added_Date_Time, Added_User, line['pCustomerID'], 'false', Entity_Type, Entity_ID, Customer_ID)
            Credit_Card_ID = 'creditcard-' + str(hashlib.sha256(line['pCreditCardNumber'].encode('utf-8')).hexdigest())
            query = query + ig.graphLoad_Credit_Card(Added_Date_Time, Added_User, line['pCreditCardNumber'], line['pCreditCardNum_exclude'], Entity_Type, Entity_ID, Credit_Card_ID, line['pCustomerID'], 'false')

        #Occupation
        Occupation_ID = 'occupation-' + str(hashlib.sha256(line['pOccupation'].title().encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Occupation(Added_Date_Time, Added_User, line['pOccupation'], Entity_Type, Entity_ID, Occupation_ID)

        #Loyalty ID
        Loyalty_ID = 'loyalty-' + str(hashlib.sha256(line['pLoyaltyLookUp'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Loyalty_Number(Added_Date_Time, Added_User, line['company'], line['pLoyaltyCardNumber'], line['pLoyaltyLookUp'], line['pLoyalty_exclude'], Entity_Type, Entity_ID, Loyalty_ID)

        #Bank Account
        Bank_Account_ID = 'bank_account-' + str(hashlib.sha256(line['pBankAcnt_lookUp'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Bank_Account(Added_Date_Time, Added_User, line['pBank_AccountNumber'], line['pBankAcnt_lookUp'],
                           line['pBankAcnt_exclude'], line['pBank_AccountType'], line['pBank_AccountHolderName'], line['pBank_Name'],
                           line['pBank_BIC'], line['pBank_RtnNum'], line['pBank_CustomerID'], line['pBank_ID'],
                           line['pBank_City'], line['pBank_BranchName'], line['pBank_BranchCode'], line['pBank_CardIIN'], Entity_Type, Entity_ID, Bank_Account_ID)

         #----Identification_1----
        Identification_ID = 'identification-' + str(hashlib.sha256(line['pID1LookUp'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Identification(Added_Date_Time, Added_User, line['pID1Number'], line['pID1Type'],
                             line['pID1Description'], line['pID1Location'], line['pID1State'], line['pID1Country'], line['pID1LookUp'],
                             line['pID1_exclude'], Entity_Type, Entity_ID, Identification_ID)

         #----Identification_2----
        Identification_ID = 'identification-' + str(hashlib.sha256(line['pID2LookUp'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Identification(Added_Date_Time, Added_User, line['pID2Number'], line['pID2Type'],
                             line['pID2Description'], line['pID2Location'], line['pID2State'], line['pID2Country'], line['pID2LookUp'],
                             line['pID2_exclude'], Entity_Type, Entity_ID, Identification_ID)

         #----IP_Address_1----
        IP_Address_ID = 'ip-' + str(hashlib.sha256(line['pIPAddr1'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_IP_Address(Added_Date_Time, Added_User, line['pIPAddr1'], line['pIPAddr1_exclude'], Entity_Type,
                         Entity_ID, IP_Address_ID)

         #----IP_Address_2----
        IP_Address_ID = 'ip-' + str(hashlib.sha256(line['pIPAddr2'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_IP_Address(Added_Date_Time, Added_User, line['pIPAddr2'], line['pIPAddr2_exclude'], Entity_Type,
                         Entity_ID, IP_Address_ID)

         #----Device_1----
        Device_ID = 'device-' + str(hashlib.sha256(line['pDeviceID1'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Device(Added_Date_Time, Added_User, line['pDeviceDesc1'], line['pDeviceOS1'],
                     line['pDeviceID1'], line['pDevice1_exclude'], Entity_Type, Entity_ID, Device_ID)

         #----Device_2----
        Device_ID = 'device-' + str(hashlib.sha256(line['pDeviceID2'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Device(Added_Date_Time, Added_User, line['pDeviceDesc2'], line['pDeviceOS2'],
                     line['pDeviceID2'], line['pDevice2_exclude'], Entity_Type, Entity_ID, Device_ID)

         #----Email_1----
        Email_ID = 'email-' + str(hashlib.sha256(line['pEMail1'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Email(Added_Date_Time, Added_User, line['pEMail1'], line['pEmail1_exclude'], Entity_Type, Entity_ID, Email_ID)

         #----Email_2----
        Email_ID = 'email-' + str(hashlib.sha256(line['pEMail2'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Email(Added_Date_Time, Added_User, line['pEMail2'], line['pEmail2_exclude'], Entity_Type, Entity_ID, Email_ID)

    # #----END of Payee Section---------------------------------------------------------------------------------------
    # #---------------------------------------------------------------------------------------------------------------
    # #----Start of On Behalf Sender Section--------------------------------------------------------------------------

        #Entity
        Entity_ID = 'entity-' + str(uuid.uuid1())
        Entity_Type = 'MSB_Sender_On_Behalf_Of_Entity'
        Entity_Edge_Type = 'MSB_On_Behalf_Sender_Transaction'
        query = query + ig.graphLoad_MSB_Entity(Added_Date_Time, Added_User, Entity_Type, 'MSB_On_Behalf_Sender_Transaction', Transaction_ID, Entity_ID)

        #Name
        Name_ID = 'name-' + str(hashlib.sha256(line['sOnBehalf_NameClean_fullName'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Name(Added_Date_Time, Added_User, line['sOnBehalf_Name'], line['sOnBehalf_NameClean_fullName'], line['sOnBehalf_NameClean_prefix'],
                   line['sOnBehalf_NameClean_firstName'], line['sOnBehalf_NameClean_middleName'], line['sOnBehalf_NameClean_firstLName'],
                   line['sOnBehalf_NameClean_secondLName'], line['sOnBehalf_NameClean_suffix'], line['sOnBehalf_NameClean_gender'], Entity_Type, Entity_ID, Name_ID)

        #Address
#        addr_dict = gc.address_builder(line['sOnBehalf_Address'], line['sOnBehalf_City'], line['sOnBehalf_State'], line['sOnBehalf_PostalCode'])
#        Address_ID = 'address-' + str(hashlib.sha256(addr_dict['Full_Address'].encode('utf-8')).hexdigest())
#        addr_dict['Country'] = 'Merica'
#        query = query + ig.graphLoad_Address(Added_Date_Time, Added_User, addr_dict['Full_Address'], addr_dict['Street'], addr_dict['City'],
#             addr_dict['State'], addr_dict['Postal'], addr_dict['Country'], addr_dict['Latitude'], addr_dict['Longitude'], Entity_Type, Entity_ID, Address_ID)

        Address_ID = 'str_addr' + str(uuid.uuid1())
        query = query + ig.graphLoad_Address(Added_Date_Time, Added_User, line['sOnBehalf_AddrClean_fullAddress'], line['sOnBehalf_AddrClean_numbers'] + ' ' + line['sOnBehalf_AddrClean_streetName'] + ' ' + line['sOnBehalf_AddrClean_streetDirection'] + ' ' + line['sOnBehalf_AddrClean_streetType'], line['sOnBehalf_AddrClean_city'], line['sOnBehalf_AddrClean_state'], line['sOnBehalf_AddrClean_city'], line['sOnBehalf_AddrClean_postalCode'], line['sOnBehalf_AddrClean_latitude'], line['sOnBehalf_AddrClean_longitude'], Entity_Type, Entity_ID, Address_ID)

        #DOB
        DOB_ID = 'dob-' + str(hashlib.sha256(line['sOnBehalf_DOBClean'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Date_Of_Birth(Added_Date_Time, Added_User,  line['sOnBehalf_DOBClean'],
                     line['sOnBehalf_DOB_Exclude'], Entity_Type, Entity_ID, DOB_ID)

        #Phone
        phone_dict = pc.normalize_single_number(line['sOnBehalf_PhoneNumber'])
        Phone_ID = 'phone-' + str(hashlib.sha256(phone_dict['E164'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Phone_Normalized(Added_Date_Time, Added_User, phone_dict['E164'], phone_dict['Country'], Entity_Type, Entity_ID, Phone_ID)

        #Customer ID, no need to handle online or offline
        Customer_ID = 'person-' + str(hashlib.sha256(line['sOnBehalf_CustomerID'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Company_Customer_ID(Added_Date_Time, Added_User, line['sOnBehalf_CustomerID'], 'false', Entity_Type, Entity_ID, Customer_ID)

        #Occupation
        Occupation_ID = 'occupation-' + str(hashlib.sha256(line['sOnBehalf_Occupation'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Occupation(Added_Date_Time, Added_User, line['sOnBehalf_Occupation'], Entity_Type, Entity_ID, Occupation_ID)

         #----Identification_1----
        Identification_ID = 'id_num-' + str(hashlib.sha256(line['sOnBehalf_ID1LookUp'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Identification(Added_Date_Time, Added_User, line['sOnBehalf_ID1Number'], line['sOnBehalf_ID1Type'],
                             line['sOnBehalf_ID1Description'], line['sOnBehalf_ID1Location'], line['sOnBehalf_ID1State'], line['sOnBehalf_ID1LookUp'],
                             line['sOnBehalf_ID1Country'], line['sOnBehalf_ID1Exclude'], Entity_Type, Entity_ID, Identification_ID)

         #----Identification_2----
        Identification_ID = 'id_num-' + str(hashlib.sha256(line['sOnBehalf_ID2LookUp'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Identification(Added_Date_Time, Added_User, line['sOnBehalf_ID2Number'], line['sOnBehalf_ID2Type'],
                             line['sOnBehalf_ID2Description'], line['sOnBehalf_ID2Location'], line['sOnBehalf_ID2State'], line['sOnBehalf_ID2LookUp'],
                             line['sOnBehalf_ID2Country'], line['sOnBehalf_ID2Exclude'], Entity_Type, Entity_ID, Identification_ID)

    # #----END of On Behalf Sender Section--------------------------------------------------------------------------------------
    # #-------------------------------------------------------------------------------------------------------------------------
    # #----Start of Sender Agent Section-----------------------------------------------------------------------------------------------

        #Entity
        Entity_ID = 'entity-' + str(uuid.uuid1())
        Entity_Type = 'MSB_Sender_Agent_Entity'
        Entity_Edge_Type = 'MSB_Transaction_Sender_Agent'
        query = query + ig.graphLoad_MSB_Entity(Added_Date_Time, Added_User, Entity_Type, Entity_Edge_Type, Transaction_ID, Entity_ID)

        #Agent
        Agent_ID = 'agent-' + str(hashlib.sha256(line['sAgentName'].title().encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Agent(Added_Date_Time, Added_User, line['sAgentNumber'], line['sAgentName'], line['sAgentType'], Entity_Type, Entity_ID, Agent_ID)

        #Address
#        addr_dict = gc.address_builder(line['sAgentAddress'], line['sAgentCity'], line['sAgentState'], line['sAgentPostalCode'])
#        Address_ID = 'address-' + str(hashlib.sha256(addr_dict['Full_Address'].encode('utf-8')).hexdigest())
#        addr_dict['Country'] = 'Merica'
#        query = query + ig.graphLoad_Address(Added_Date_Time, Added_User, addr_dict['Full_Address'], addr_dict['Street'], addr_dict['City'],
#             addr_dict['State'], addr_dict['Postal'], addr_dict['Country'], addr_dict['Latitude'], addr_dict['Longitude'], Entity_Type, Entity_ID, Address_ID)

        Address_ID = 'str_addr' + str(uuid.uuid1())
        query = query + ig.graphLoad_Address(Added_Date_Time, Added_User, line['sAgent_AddrClean_fullAddress'], line['sAgent_AddrClean_numbers'] + ' ' + line['sAgent_AddrClean_streetName'] + ' ' + line['sAgent_AddrClean_streetDirection'] + ' ' + line['sAgent_AddrClean_streetType'], line['sAgent_AddrClean_city'], line['sAgent_AddrClean_state'], line['sAgent_AddrClean_city'], line['sAgent_AddrClean_postalCode'], line['sAgent_AddrClean_latitude'], line['sAgent_AddrClean_longitude'], Entity_Type, Entity_ID, Address_ID)

        #Phone
        phone_dict = pc.normalize_single_number(line['sAgentPhoneNumber'])
        Phone_ID = 'phone-' + str(hashlib.sha256(phone_dict['E164'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Phone_Normalized(Added_Date_Time, Added_User, phone_dict['E164'], phone_dict['Country'], Entity_Type, Entity_ID, Phone_ID)

        #Clerk
        Clerk_ID = 'clerk-' + str(hashlib.sha256(line['sAgentClerk'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Clerk(Added_Date_Time, Added_User, line['sAgentClerk'], line['sAgentNumber'], line['sAgentName'], line['sAgentType'], Entity_Type, Entity_ID, Clerk_ID)

    # #----END of On Sender Agent Section--------------------------------------------------------------------------------------
    # #-------------------------------------------------------------------------------------------------------------------------
    # #----Start of Payee Agent Section-----------------------------------------------------------------------------------------------

        #Entity
        Entity_ID = 'entity-' + str(uuid.uuid1())
        Entity_Type = 'MSB_Payee_Agent_Entity'
        Entity_Edge_Type = 'MSB_Transaction_Payee_Agent'
        query = query + ig.graphLoad_MSB_Entity(Added_Date_Time, Added_User, Entity_Type, Entity_Edge_Type, Transaction_ID, Entity_ID)

        #Agent
        Agent_ID = 'agent-' + str(hashlib.sha256(line['pAgentName'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Agent(Added_Date_Time, Added_User, line['pAgentNumber'], line['pAgentName'], line['pAgentType'], Entity_Type, Entity_ID, Agent_ID)

        #Address
#        addr_dict = gc.address_builder(line['pAgentAddress'], line['pAgentCity'], line['pAgentState'], line['pAgentPostalCode'])
#        Address_ID = 'address-' + str(hashlib.sha256(addr_dict['Full_Address'].encode('utf-8')).hexdigest())
#        addr_dict['Country'] = 'Merica'
#        query = query + ig.graphLoad_Address(Added_Date_Time, Added_User, addr_dict['Full_Address'], addr_dict['Street'], addr_dict['City'],
#             addr_dict['State'], addr_dict['Postal'], addr_dict['Country'], addr_dict['Latitude'], addr_dict['Longitude'], Entity_Type, Entity_ID, Address_ID)

        Address_ID = 'str_addr' + str(uuid.uuid1())
        query = query + ig.graphLoad_Address(Added_Date_Time, Added_User, line['pAgent_AddrClean_fullAddress'], line['pAgent_AddrClean_numbers'] + ' ' + line['pAgent_AddrClean_streetName'] + ' ' + line['pAgent_AddrClean_streetDirection'] + ' ' + line['pAgent_AddrClean_streetType'], line['pAgent_AddrClean_city'], line['pAgent_AddrClean_state'], line['pAgent_AddrClean_city'], line['pAgent_AddrClean_postalCode'], line['pAgent_AddrClean_latitude'], line['pAgent_AddrClean_longitude'], Entity_Type, Entity_ID, Address_ID)

        #Phone
        phone_dict = pc.normalize_single_number(line['pAgentPhoneNumber'])
        Phone_ID = 'phone-' + str(hashlib.sha256(phone_dict['E164'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Phone_Normalized(Added_Date_Time, Added_User, phone_dict['E164'], phone_dict['Country'], Entity_Type, Entity_ID, Phone_ID)

        #Clerk
        Clerk_ID = 'clerk-' + str(hashlib.sha256(line['pAgentClerk'].encode('utf-8')).hexdigest())
        query = query + ig.graphLoad_Clerk(Added_Date_Time, Added_User, line['pAgentClerk'], line['pAgentNumber'], line['pAgentName'], line['pAgentType'], Entity_Type, Entity_ID, Clerk_ID)

    # #----END of Payee Agent Section-------------------------------------------------------------------------------------------
    # #-------------------------------------------------------------------------------------------------------------------------

        count = count + 1

        if count % post_max_size == 0:
            print(count)
            while True:
                try:
#                    response = ig.post_to_db(query)
#                    print(response)
                    break
                except:
                    print('Error reaching server')
                    time.sleep(30)
            print('Took %s seconds to load ' % str(time.time() - start_time) + str(post_max_size) + ' records')
            
#            output_file = open('output.txt', 'a')
#            output_file.write(str(count - 10) + ' - ' + str(count) + '\n' + str(response) + '\n' + 'Took %s seconds to load ' % str(time.time() - start_time) + '\n\n')
#            output_file.close()

#            if 'errors' in response:
#                error_file = open('errors.txt', 'a')
#                error_file.write(str(count - 10) + ' - ' + str(count) + '\n')
#                error_file.write(str(response) + '\n')
#                error_file.write(str(query) + '\n\n')
            print(len(query))
#            query = ''
            break
    break

    ############Send to DB#####################
#        if cnt % post_max_size == 0:
#            response = ig.post_to_db(query)
#            print(response)
#
#        if cnt > 499 and cnt % 500 == 0:
#            print("Users: " + str(cnt) + " records have been loaded so far.")
#            cnt += 1
#response = ig.post_to_db(query)
#print(count)

print(query)
