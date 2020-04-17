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

    retrieveDataFor(result, type, date) {
        this.instance.collection(type).find(date).toArray()
        .then(recentData => {
            // TODO: CLEAN ALL THESE VARIABLES UP
            // // make json usable (should probably make this a function)
            recentData = jsonify(recentData);

            var counter = 0;
            console.log(recentData)
            for (let country in recentData) {
                console.log(country)
                this.instance.collection("coordinates").find({_id: country}).toArray()
                .then(coords => {
                    try {
                        // make json usable (should probably make this a function)
                        coords = jsonify(coords)

                        result[counter] = {"country": coords._id, "coordinates": [coords.latitude,coords.longitude], "stat": recentData[coords._id]}
                        counter++;

                    } catch {
                        console.log("Country not found.")
                    }
                })
            }                  
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
        var query = {date: '2020-04-16'};

        // get most recent data
        db.retrieveDataFor(result, req.query.type, query);
        sleep(3000).then(() => {
            res.status(200).send(result);; 
        })
        
        
    }); 
    
    //  // GET coordinates for country
    //  app.get('/coords', (req, res) => {

    //     var result = {}

    //     // build query
    //     var dbo = db.db("corona_virus_data");
        
    //     // get coords from mongo
    //     dbo.collection("coordinates").find({}).toArray()
    //     .then(recentData => {
    //         res.status(200).send(recentData);
    //     })
    //     .catch(error => console.error(error))
    // }); 

});

app.get('/', (req, res) => res.send('Hello World!'))

app.listen(port, () => console.log(`Example app listening on port ${port}!`))
