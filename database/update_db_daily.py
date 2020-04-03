import pymongo
import dns
import requests
import pandas as pd
import csv
import pprint
import json

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

    def load_collections(self, collection_name, url):
        df = pd.read_csv(url)
        current_date = df.tail(1)
        print(current_date)
        data_json = json.loads(current_date.to_json(orient = 'records'))
        collection_name.insert(data_json)

    def call_loader(self):
        self.load_collections(self.total_cases_collection, "https://covid.ourworldindata.org/data/ecdc/total_cases.csv")
        self.load_collections(self.total_deaths_collection, "https://covid.ourworldindata.org/data/ecdc/total_deaths.csv")
        self.load_collections(self.new_cases_collection, "https://covid.ourworldindata.org/data/ecdc/new_cases.csv")
        self.load_collections(self.new_deaths_collection, "https://covid.ourworldindata.org/data/ecdc/new_deaths.csv")


    def print_data(self):
        for post in self.total_cases_collection.find():
            pprint.pprint(post)
    
    def tear_down(self):
        self.client.close()



def main():
    db_setup = DataBaseSetup()
    db_setup.setup_client()
    db_setup.setup_collections()
    db_setup.call_loader()
    db_setup.print_data()
    db_setup.tear_down()

                            #client.close()


main()
