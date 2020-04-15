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
        self.coords_collection = self.corona_database["coordinates"]
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

    def create_coord_collection(self):
        df = pd.read_csv("https://raw.githubusercontent.com/albertyw/avenews/master/old/data/average-latitude-longitude-countries.csv")
        df = df.drop('ISO 3166 Country Code', axis = 1) #drop extraneous country code
        df.set_index("Country", inplace=True) #set index to country 

        country_data = self.total_cases_collection.find_one()
        for country, stat in country_data.items():
            #print(country)
            try:
                latt = df.loc[country][1]
                longg = df.loc[country][0]
                #print(latt, longg)
                self.coords.insert_one({country:[latt, longg]})
                
            except Exception:
                #print("error, couldn't find: ", country)
                pass
            

    def call_loader(self):
        self.load_collections(self.total_cases_collection, "https://covid.ourworldindata.org/data/ecdc/total_cases.csv")
        self.load_collections(self.total_deaths_collection, "https://covid.ourworldindata.org/data/ecdc/total_deaths.csv")
        self.load_collections(self.new_cases_collection, "https://covid.ourworldindata.org/data/ecdc/new_cases.csv")
        self.load_collections(self.new_deaths_collection, "https://covid.ourworldindata.org/data/ecdc/new_deaths.csv")
        #for collection, url in self.collections.items():
            #self.load_collections(collection, url)
    
    def print(self):
        print(self.coords_collection.find_one())
        for post in self.coords_collection.find():
            pprint.pprint(post)

    def tear_down(self):
        self.client.close()

    def get_coords(self):
        df = pd.read_csv("https://raw.githubusercontent.com/albertyw/avenews/master/old/data/average-latitude-longitude-countries.csv")
        df = df.drop('ISO 3166 Country Code', axis = 1) #drop extraneous country code
        df.set_index("Country", inplace=True) #set index to country 
        #b = df.loc["Aruba"] #example locate 
	return df



db_setup = DataBaseSetup()
db_setup.setup_client()
db_setup.setup_collections()

date_obj = db_setup.get_recent_date()

def gen_json(df):
    dic = {}

    ctr = 0
    for country_name in date_obj: 
        try:
            #print('thing: ',thing)
            #print(df.loc[thing])
            dic[ctr] = {"name": country_name,"coordinates": [df.loc[country_name][1], 
                    df.loc[country_name][0]], "deaths": date_obj[country_name]}
            #dic["countries"].append(obj)
            ctr+=1
        except Exception:
            print("error, couldn't find: ", country_name)

    with open ('countries.json', 'w') as outfile: 
        json.dump(dic, outfile)



def main():
    db_setup = DataBaseSetup()
    db_setup.setup_client()
    db_setup.setup_collections()
    db_setup.drop_data()
    db_setup.call_loader()
    
    db_setup.create_coord_collection()
    db_setup.print()
    db_setup.tear_down()
    #client.close()


main()
