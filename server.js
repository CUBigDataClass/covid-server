const express = require('express')
const app = express()
const port = 3000
const mongo = require('mongodb').MongoClient;

var url = "mongodb+srv://maurawins:coronabigdata@cluster0-ud77s.gcp.mongodb.net/test?retryWrites=true&w=majority";

const sleep = (milliseconds) => {
    return new Promise(resolve => setTimeout(resolve, milliseconds))
  }

// Connect to the db
mongo.connect(url,  { useNewUrlParser: true, useUnifiedTopology: true }, function(err, db) {
    if(err) {
        console.log('Sorry unable to connect to MongoDB Error:', err);
    }
    console.log("Connected to mongodb");

    // GET daily updates on case totals by country
    app.get('/data', (req, res) => {

        // get todays date
        var today = new Date();
        var dd = String(today.getDate()).padStart(2, '0');
        var mm = String(today.getMonth() + 1).padStart(2, '0'); //January is 0!
        var yyyy = today.getFullYear();
        today = yyyy + '-' + mm + '-' + dd;

        // build query
        var dbo = db.db("corona_virus_data");
        var query = {date: "2020-04-15"};


        var result = {}
        // get most recent data
        dbo.collection(req.query.type).find(query).toArray()
        .then(recentData => {


            // TODO: CLEAN ALL THESE VARIABLES UP
            // // make json usable (should probably make this a function)
            var stringobj = JSON.stringify(recentData);
            stringobj = stringobj.substring(1, stringobj.length-1)
            recentData = JSON.parse(stringobj)

            var counter = 0;

            for (country in recentData) {
                dbo.collection("coordinates").find({_id: country}).toArray()
                .then(coords => {
                    try {
                        // make json usable (should probably make this a function)
                        var stringobj2 = JSON.stringify(coords);
                        stringobj2 = stringobj2.substring(1, stringobj2.length-1)
                        coords = JSON.parse(stringobj2)

                        result[counter] = {"country": coords._id, "coordinates": [coords.latitude,coords.longitude], "stat": recentData[coords._id]}
                        counter++;
                        
                    } catch {
                        console.log("Country not found.")
                    }
                    
                })
            }

            sleep(2500).then(() => {
                res.status(200).send(result);  
              })
            
                      
        })
        .catch(error => console.error(error))
    }); 
    
     // GET coordinates for country
     app.get('/coords', (req, res) => {

        var result = {}

        // build query
        var dbo = db.db("corona_virus_data");
        
        // get coords from mongo
        dbo.collection("coordinates").find({}).toArray()
        .then(recentData => {
            res.status(200).send(recentData);
        })
        .catch(error => console.error(error))
    }); 
});


app.get('/', (req, res) => res.send('Hello World!'))

app.listen(port, () => console.log(`Example app listening on port ${port}!`))
