// class mongoConnection {
    
//   constructor() {
//       this.url ="mongodb+srv://maurawins:coronabigdata@cluster0-ud77s.gcp.mongodb.net/test?retryWrites=true&w=majority";
//   }

//   connect(callback) {
//       mongo.connect(this.url,  { useNewUrlParser: true, useUnifiedTopology: true }, function(err, db) {
//           if(err) {
//               console.log('Sorry unable to connect to MongoDB Error:', err);
//               return;
//           } 
//           console.log("Connected to mongodb");
          
//           callback(err, db);
//       });    
//   }
// }

class mongoDB {
  
  constructor(db) {
      this.db = db;
      this.dbName = "corona_virus_data"
      this.instance = this.db.db(this.dbName);
  }

  retrieveDataFor(type, query) {

  }
}