const express = require('express')
const app = express()
const port = 3000
const mongo = require('mongodb').MongoClient;

var url = "mongodb+srv://maurawins:coronabigdata@cluster0-ud77s.gcp.mongodb.net/test?retryWrites=true&w=majority";

// Connect to the db
mongo.connect(url,  { useNewUrlParser: true, useUnifiedTopology: true }, function(err, db) {
    if(err) {
        console.log('Sorry unable to connect to MongoDB Error:', err);
    }
    console.log("Connected to mongodb");
    // var dbo = db.db("coron_virus_data");
    // var query = {date: '2020-04-10'};
    // dbo.collection("total_case").find(query).toArray(function(err, result) {
    //     if (err) throw err;
    //     console.log(result);
    //     db.close();
    // });
});


app.get('/', (req, res) => res.send('Hello World!'))


// GET daily updates on case totals by country
app.get('/daily', (req, res) => {

    let countryData = require('./server/countries.json');

    var country = [];

    for (var key in countryData) {
        country.push(countryData[key])
    }
    
    res.status(200).send(data);
    
});

app.listen(port, () => console.log(`Example app listening on port ${port}!`))
