import pymongo
from pymongo import MongoClient
import os
import json


def get_data_from_file():
    file_name = input("Enter the path of your file: ")
    assert os.path.exists(file_name), "I did not find the file at, " + str(file_name)
    f = open(file_name)
    ls = json.load(f)
    return ls


client = MongoClient('localhost', 27017)
mydb = client['mydatabase']
mycol = mydb['customers']


if mydb['customers'].count() != 0:
    mydb['customers'].remove({})

mylist = get_data_from_file()

x = mycol.insert_many(mylist)

#insert One
mydict = { 'name': 'Rotem', 'Gender': 'Female ', 'Age': 22}
x = mycol.insert_one(mydict)

#for getting all the rows
print("----- Get all documents -----")
for x in mycol.find():
    print(x)


print("----- Get documents by specific keys -----")
for x in mycol.find({},{'_id': 0, "name": 1, 'Age': 1}):
    print(x)


#Filtering Data in PyMongo
#GT- Greater Than
#LT – Less Than
#ET – Equal To

print("----- Filter documents -----")
myquery = {'Age': {'$gt': 27}}
mydoc = mycol.find(myquery)
for x in mydoc:
    print(x)


#sorting Acending order
print("----- Sort documents by age -----")
mydoc = mycol.find().sort('Age')
for x in mydoc:
    print(x)

#update one row
print("----- Update documents ; change 'name' value from 'Benny' to 'Benjamin -----")
myquery = {'name': 'Benny'}
newvalues = {'$set': {'name': 'Benjamin'}}
mycol.update_one(myquery, newvalues)
for x in mycol.find():
    print(x)


#Updating Multiple Rows
print("----- Update multiple documents by condition -----")
myquery = {'name': {'$regex': '^D'}}
newvalues = {'$set': {'name': 'King'}}
x = mycol.update_many(myquery, newvalues)
for x in mycol.find():
    print(x)


#Deleting Multiple Documents(Rows)
myquery = { 'name': {'$regex': '^R'} }
x = mycol.delete_many(myquery)
print(x.deleted_count, ' documents deleted.')


#Aggregate
print("----- Aggregate by key -----")
myquery = [
    {'$match': {'Age': {'$gt': 20}}},
    {'$group': {'_id': {'City': '$Address'}, 'TotalPersons': {'$sum': 1}}},
    {'$sort': {'TotalPersons': -1}}
          ]
x = mycol.aggregate(myquery)
for d in x:
    print(d)


print("----- Get distance from location coordinates -----")
mycol.ensure_index([('location', pymongo.GEOSPHERE)])
myquery = [{'$geoNear': {'near': {'type': "Point", 'coordinates': [34.8200052, 32.1133416]},
                         'distanceField': "dist.calculated", 'spherical': 'True'}}]
x = mycol.aggregate(myquery)

for d in x:
    print(d['name'], d['Address'], d['dist']['calculated'])
