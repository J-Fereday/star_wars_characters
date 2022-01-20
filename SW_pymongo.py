import pymongo
import requests
import json


def connection(server):
    #connect to MongoDB server
    client = pymongo.MongoClient()
    return client[server]


def getDatafromURL(URL):
    #getData from URL and return
    request = requests.get(URL).text
    return json.loads(request)


def InsertingData(URL,server):
    #open database and retrieve data from URL using existing functions
    db = connection(server)
    data = getDatafromURL(URL)
    for item in data["results"]:
        db.starships.insert_one(item)


def updatingShips():
    #connect to database
    db = connection(server="starwars")
    
    #loop through ships
    for ships in db.starships.find():
        
        #make empty id array
        id_array = []
        
        #loop through pilots
        for pilots in ships["pilots"]:
           #get pilot name from URL in data, use name to search id in database and append to the array
           pilot_name = getDatafromURL(pilots)["name"]
           result = db.characters.find_one({"name":pilot_name})
           id_array.append(result["_id"])
           
        #update database with array
        db.starships.update_one({"name":ships["name"]},{"$set":{"pilots":id_array}})


db = connection(server="starwars")

db.starships.drop()

InsertingData(URL="https://swapi.dev/api/starships",server="starwars")

updatingShips()
