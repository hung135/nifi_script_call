import phonenumbers as pn
from phonenumbers import geocoder as pngc

def normalize_single_number(phone):
    try:
        parsed = pn.parse(phone, 'US')
    except:
        phones = {'E164' : 'Invalid Number', 'International' : 'Invalid Number', 'National' : 'Invalid Number', 
                  'Country' : 'Invalid Number'}
        return phones
    
    if pn.is_possible_number(parsed) != True:
        phones = {'E164' : 'Invalid Number', 'International' : 'Invalid Number', 'National' : 'Invalid Number', 
                  'Country' : 'Invalid Number'}
        return phones
    else:
        E164 = pn.format_number(parsed, pn.PhoneNumberFormat.E164)
        International = pn.format_number(parsed, pn.PhoneNumberFormat.INTERNATIONAL)
        National = pn.format_number(parsed, pn.PhoneNumberFormat.NATIONAL)
        Country = pngc.country_name_for_number(parsed, 'en')
    
    phones = {'E164' : E164, 'International' : International, 'National' : National, 'Country' : Country}
    
    return phones

def extract_normalize_from_text(text):
    phones = {}
    index = 0
    
    for match in pn.PhoneNumberMatcher(text, "US"):
        phones[index] = {'E164' : pn.format_number(match.number, pn.PhoneNumberFormat.E164), 
              'International' : pn.format_number(match.number, pn.PhoneNumberFormat.INTERNATIONAL),
              'National' : pn.format_number(match.number, pn.PhoneNumberFormat.NATIONAL) }
        index += 1
        
    return phones

def extract_phones_from_file(file):
    phones = []    
      
    for line in file:
        for match in pn.PhoneNumberMatcher(line.strip(), "US"):
            phones.append(pn.format_number(match.number, pn.PhoneNumberFormat.E164))
       
    return phones