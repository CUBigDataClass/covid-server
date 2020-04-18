const {mongoConnector, mongoDB} = require('./server/mongoFacade.js')
const express = require('express')
const app = express()
const port = 3001

// allow cross-origin requests
let allowCrossDomain = function(req, res, next) {
    res.header('Access-Control-Allow-Origin', "*");
    res.header('Access-Control-Allow-Headers', "*");
    next();
  }
app.use(allowCrossDomain);

const sleep = (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
}


function getTodaysDate() {
    // get todays date
    var today = new Date();
    var dd = String(today.getDate()).padStart(2, '0');
    var mm = String(today.getMonth() + 1).padStart(2, '0'); //January is 0!
    var yyyy = today.getFullYear();
    return yyyy + '-' + mm + '-' + dd;
}

var connector = new mongoConnector();

connector.connect(function(err,db) {

    db = new mongoDB(db);

    // GET daily updates on case totals by country
    app.get('/data', (req, res) => {
        // build query
        var result = []
        var date = getTodaysDate();
        var query = {date: '2020-04-15'};

        // get data on all countries for a given date
        db.get(req.query.type, query, function(recentData) {
            //result.push({"country": "World", "stat": recentData["World"]})
            
            var counter = 1;
            console.log(recentData)
            for (let country in recentData) {
                // get coordinates for each country
                db.get("coordinates", {_id: country}, function(coords) {
                    try {
                        if (coords._id != undefined) {
                            result.push({"country": coords._id, "coordinates": [coords.latitude,coords.longitude], "stat": recentData[coords._id]})
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
            res.status(200).send({result});
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
