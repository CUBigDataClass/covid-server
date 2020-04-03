import pymongo
import dns # required for connecting with SRV
import json
import pandas as pd
import pprint
import requests
import csv

class DataBaseSetup():
    def setup_client(self):
        self.client = pymongo.MongoClient("mongodb://maurawins:coronabigdata@cluster0-shard-00-00-ud77s.gcp.mongodb.net:27017,cluster0-shard-00-01-ud77s.gcp.mongodb.net:27017,cluster0-shard-00-02-ud77s.gcp.mongodb.net:27017/test?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin&retryWrites=true&w=majority")
        self.db = self.client.test
        print(self.db)
        self.corona_database = self.client["coron_virus_data"]

    def setup_collections(self):
        self.total_cases_collection = self.corona_database["total_case"]
        self.total_deaths_collection = self.corona_database["total_deaths"]
        self.new_cases_collection = self.corona_database["new_cases"]
        self.new_deaths_collection = self.corona_database["new_deaths"]
        #self.collections = {self.total_cases_collection: "https://covid.ourworldindata.org/data/ecdc/total_cases.csv", self.total_deaths_collection: "https://covid.ourworldindata.org/data/ecdc/total_deaths.csv", self.new_cases_collection: "https://covid.ourworldindata.org/data/ecdc/new_cases.csv", self.new_deaths_collection: "https://covid.ourworldindata.org/data/ecdc/new_deaths.csv"}
    def drop_data(self):
        result = self.total_cases_collection.remove()
        print("removing...", result)
        self.total_deaths_collection.remove()
        self.new_cases_collection.remove()
        self.new_deaths_collection.remove()
    def load_collections(self, collection_name, url):
        df = pd.read_csv(url)
        data_json = json.loads(df.to_json(orient = 'records'))
        collection_name.insert(data_json)

    def call_loader(self):
        self.load_collections(self.total_cases_collection, "https://covid.ourworldindata.org/data/ecdc/total_cases.csv")
        self.load_collections(self.total_deaths_collection, "https://covid.ourworldindata.org/data/ecdc/total_deaths.csv")
        self.load_collections(self.new_cases_collection, "https://covid.ourworldindata.org/data/ecdc/new_cases.csv")
        self.load_collections(self.new_deaths_collection, "https://covid.ourworldindata.org/data/ecdc/new_deaths.csv")
        #for collection, url in self.collections.items():
            #self.load_collections(collection, url)
    
    def print(self):
        for post in self.new_cases_collection.find():
            pprint.pprint(post)
    def tear_down(self):
        self.client.close()


                
'''

mydb = client["corona_virus_data"]

total_cases_col = mydb["total_cases"]
total_cases_col.remove()
fname = "https://covid.ourworldindata.org/data/ecdc/total_cases.csv"
df = pd.read_csv(fname)
#df.to_json('total_cases.json', orient = 'index')

data_json = json.loads(df.to_json(orient='records'))
#db_cm.remove()
total_cases_col.insert(data_json)
#print(type(json_obj))
#print(json_obj)
#print("type: ", type(df))
#with open('total_cases.json') as f:
    #file_data = json.load(f)

#for row in file_data:
    #print(row)
#total_cases_col.insert_many(file_data)
for post in total_cases_col.find():
    pprint.pprint(post)
client.close()'''

def main():
    db_setup = DataBaseSetup()
    db_setup.setup_client()
    db_setup.setup_collections()
    db_setup.drop_data()
    db_setup.call_loader()
    #db_setup.print()
    db_setup.tear_down()
    #client.close()


main()
