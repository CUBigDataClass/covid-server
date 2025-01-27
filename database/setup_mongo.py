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
        self.corona_database = self.client["corona_virus_data"]

    def setup_collections(self):
        self.total_cases_collection = self.corona_database["total_cases"]
        self.total_deaths_collection = self.corona_database["total_deaths"]
        self.new_cases_collection = self.corona_database["new_cases"]
        self.new_deaths_collection = self.corona_database["new_deaths"]
        self.coords_collection = self.corona_database["coordinates"]
        #self.collections = {self.total_cases_collection: "https://covid.ourworldindata.org/data/ecdc/total_cases.csv", self.total_deaths_collection: "https://covid.ourworldindata.org/data/ecdc/total_deaths.csv", self.new_cases_collection: "https://covid.ourworldindata.org/data/ecdc/new_cases.csv", self.new_deaths_collection: "https://covid.ourworldindata.org/data/ecdc/new_deaths.csv"}
    
    def drop_data(self):
        self.total_cases_collection.drop()
        self.total_deaths_collection.drop()
        self.new_cases_collection.drop()
        self.new_deaths_collection.drop()
        self.coords_collection.drop()

    def load_collections(self, collection_name, url):
        df = pd.read_csv(url)


        data_json = json.loads(df.to_json(orient = 'records'))
        collection_name.insert(data_json)

    def create_coord_collection(self):
        df = pd.read_csv("https://raw.githubusercontent.com/albertyw/avenews/master/old/data/average-latitude-longitude-countries.csv")
        df = df.drop('ISO 3166 Country Code', axis = 1) #drop extraneous country code
        df.set_index("Country", inplace=True) #set index to country 

        country_data = self.total_cases_collection.find_one()
        for country, stat in country_data.items():
            try:
                latt = df.loc[country][1]
                longg = df.loc[country][0]
                self.coords_collection.insert_one({"_id": country, "latitude": latt, "longitude": longg})
                
            except Exception:
                pass
            

    def call_loader(self):
        self.load_collections(self.total_cases_collection, "https://covid.ourworldindata.org/data/ecdc/total_cases.csv")
        self.load_collections(self.total_deaths_collection, "https://covid.ourworldindata.org/data/ecdc/total_deaths.csv")
        self.load_collections(self.new_cases_collection, "https://covid.ourworldindata.org/data/ecdc/new_cases.csv")
        self.load_collections(self.new_deaths_collection, "https://covid.ourworldindata.org/data/ecdc/new_deaths.csv")
    
    def print(self):
        print(self.coords_collection.find_one())
        for post in self.coords_collection.find():
            pprint.pprint(post)

    def tear_down(self):
        self.client.close()

def main():
    db_setup = DataBaseSetup()
    db_setup.setup_client()
    db_setup.setup_collections()
    db_setup.drop_data()
    db_setup.call_loader()
    
    db_setup.create_coord_collection()
    #db_setup.print()
    db_setup.tear_down()
    #client.close()


if __name__ == '__main__':
    main()





    
