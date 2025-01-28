from pyconcaldates.duck_reader import duck_read

# Extract dates from contacts and add them to calendar 
# GNU GENERAL PUBLIC LICENSE Version 3

contacts_csv = "data/contacts.csv"


ddb = duck_read(contacts_csv)
print(type(ddb))
print(ddb)