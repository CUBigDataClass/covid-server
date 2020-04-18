// import mongoDB from './server/mongoFacade'
const express = require('express')
const app = express()
const port = 3000
const mongo = require('mongodb').MongoClient;

const sleep = (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}

var url ="mongodb+srv://maurawins:coronabigdata@cluster0-ud77s.gcp.mongodb.net/test?retryWrites=true&w=majority"

function jsonify(data) {
    var stringobj = JSON.stringify(data);
    stringobj = stringobj.substring(1, stringobj.length-1)
    return JSON.parse(stringobj)
}

function getTodaysDate() {
    // get todays date
    var today = new Date();
    var dd = String(today.getDate()).padStart(2, '0');
    var mm = String(today.getMonth() + 1).padStart(2, '0'); //January is 0!
    var yyyy = today.getFullYear();
    return yyyy + '-' + mm + '-' + dd;
}


class mongoConnector {
    
    constructor() {
        this.url ="mongodb+srv://maurawins:coronabigdata@cluster0-ud77s.gcp.mongodb.net/test?retryWrites=true&w=majority";
    }

    connect(callback) {
        mongo.connect(this.url,  { useNewUrlParser: true, useUnifiedTopology: true }, function(err, db) {
            if(err) {
                console.log('Sorry unable to connect to MongoDB Error:', err);
                return;
            } 
            console.log("Connected to mongodb");
            
            callback(err, db);
        });    
    }
}

class mongoDB {
    
    constructor(db) {
        this.db = db;
        this.dbName = "corona_virus_data"
        this.instance = this.db.db(this.dbName);
    }

    get(collection, query, callback) {
        this.instance.collection(collection).find(query).toArray()
        .then(data => {
            try {
                data = jsonify(data);
            } catch {
                // this is expected to happen when empty data is returned
            }
            callback(data)
           
        })
        .catch(error => console.error(error))
    }
}

var connector = new mongoConnector();

connector.connect(function(err,db) {

    db = new mongoDB(db);

    // GET daily updates on case totals by country
    app.get('/data', (req, res) => {
        var result = {}

       
        // build query
        var date = getTodaysDate();
        var query = {date: '2020-04-15'};

        // get data on all countries for a given date
        db.get(req.query.type, query, function(recentData) {
            result[0] = {"country": "World", "stat": recentData["World"]}
            var counter = 1;
            for (let country in recentData) {

                // get coordinates for each country
                db.get("coordinates", {_id: country}, function(coords) {
                    try {
                        if (coords._id != undefined) {
                            result[counter] = {"country": coords._id, "coordinates": [coords.latitude,coords.longitude], "stat": recentData[coords._id]}
                            counter++;
                        }
                    } catch {
                        //console.log("Country not found.")
                    }
                })
            }
        });

        // temporary sleep function bc figuring out async java promises can wait til later
        sleep(3000).then(() => {
            res.status(200).send(result);
        })
        
        
    }); 

    app.get('/coords', (req, res) => {
        db.get("total_cases", {date: '2020-04-15'}, function(results) {
            res.status(200).send(results);
        });
       
    });
});

app.get('/', (req, res) => res.send('Hello World!'))

app.listen(port, () => console.log(`Example app listening on port ${port}!`))
