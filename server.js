const {mongoConnector, mongoDB} = require('./server/mongoFacade.js')
const express = require('express')
const app = express()
const port = '35.193.65.75'

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


function getYesterdaysDate() {
    // get todays date
    var today = new Date();
    var yesterday = new Date(today);
    yesterday.setDate(yesterday.getDate() - 1);

    var dd = String(yesterday.getDate()).padStart(2, '0');
    var mm = String(yesterday.getMonth() + 1).padStart(2, '0'); //January is 0!
    var yyyy = yesterday.getFullYear();
    return yyyy + '-' + mm + '-' + dd;
}

var connector = new mongoConnector();

connector.connect(function(err,db) {

    db = new mongoDB(db);
    var date = getYesterdaysDate()
    var query = {date: date};
    app.get('/data_nocoords', (req, res) => {
        db.get(req.query.type, query, function(results) {
            
            res.status(200).send(results);
           
        });
    });

    // GET daily updates on case totals by country
    app.get('/data', (req, res) => {
        // build query
        var result = []
        var date = getYesterdaysDate();
        var query = {date: date};

        // get data on all countries for a given date
        db.get(req.query.type, query, function(recentData) {
            for (let country in recentData) {
                result.push({"country": country, "stat": recentData[country]})
            }

            res.status(200).send(result);
        });        
    }); 

    app.get('/coords', (req, res) => {
        db.get("coordinates", {}, function(results) {
            res.status(200).send(results);
        });
       
    });


    app.get('/timeline', (req, res) => {
        db.get(req.query.type, {}, function(results) {
            res.status(200).send(results);
        });
       
    });
});

app.get('/', (req, res) => res.send('Hello World!'))

app.listen(port, () => console.log(`Example app listening on port ${port}!`))
