const mongo = require('mongodb').MongoClient;

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

function jsonify(data) {
  var stringobj = JSON.stringify(data);
  stringobj = stringobj.substring(1, stringobj.length-1)
  return JSON.parse(stringobj)
}


exports.mongoConnector = mongoConnector
exports.mongoDB = mongoDB
