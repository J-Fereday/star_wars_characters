import pymongo
import requests
import json




def connection(server):
    client = pymongo.MongoClient()
    return client[server]


def insertingShips(URL):
    #get mongo server connection
    db = connection(server="starwars")
    
    #get text from URL and convert to json format
    r = requests.get(URL).text
    d = json.loads(r)
    
    #loop over ship types in data set
    for ships in d["results"]:
        db.starships.insert_one(ships)
        
        #update pilot names
        name_array = []
        for pilots in ships["pilots"]:
            name_array.append(replaceURLObjectID(pilots))
        db.starships.update_one({"name":ships["name"]},{"$set":{"pilots":name_array}})
        
    #if next is not empty, load data from that URL
    if d["next"] is not None:
        insertingShips(URL=d["next"])

    

def replace_name_with_ID():
    #get mongo server connection
    db = connection(server="starwars")
    
    #get starships and loop through all ships
    starships = db.starships.find()
    for ships in starships:
        
        #make empty id array
        id_array = []
        
        #loop through all pilots
        for pilots in ships["pilots"]:
            #find id for given pilot name
            result = db.characters.find_one({"name":pilots})
            id_array.append(result["_id"])
            
        #replace names with ids
        db.starships.update_one({"name":ships["name"]},{"$set":{"pilots":id_array}})



def replaceURLObjectID(URL):
    r = requests.get(URL).text
    d = json.loads(r)
    return d["name"]


db = connection(server="starwars")

db.starships.drop()


insertingShips("https://swapi.dev/api/starships")

replace_name_with_ID()












#combining functions
#db.starships.aggregate([{$lookup:{from:"characters",localField:"commander",foreignField:"_id",as:"matched_pilot}}]}
#