var express = require('express');
var cassandra = require('cassandra-driver');
var async = require('async');
var http = require("http");
var bodyParser = require('body-parser');
var sqlite3 = require('sqlite3').verbose();
var db = new sqlite3.Database('url'); 
var url = require('url');
var app = express();
var arra=[];
app.use(bodyParser());

var client = new cassandra.Client( { contactPoints : [ '127.0.0.1' ] } );
client.connect(function(err, result) {
    console.log('Connected.');
});


var insert = 'INSERT INTO simplex.urls (sturl,url) VALUES(?, ?);';
var search = 'SELECT * FROM simplex.urls WHERE id = ?;';



app.get('/:format', function (req, res) {
   var format = req.params.format;
   console.log(typeof(format));
    var link='http://localhost:3000/'+format;
    var route;
  client.execute("SELECT * FROM simplex.urls WHERE sturl = ?;",[link] ,function(err, rs) {
   console.log("rs.rows[0].url");

	if(link==String(rs.rows[0].sturl)){
	res.writeHead(301,
{Location:String(rs.rows[0].url)}
);
res.end();


    }
});

});

app.get('/',function(req,res){


   client.execute("CREATE KEYSPACE IF NOT EXISTS simplex WITH replication " +
                   "= {'class' : 'SimpleStrategy', 'replication_factor' : 3};",
                   afterExecution('Error: ', 'Keyspace created.', res));

 client.execute('CREATE TABLE IF NOT EXISTS simplex.urls (' +
               'id uuid ,' +
                'sturl text,' +
                'url text,' +
		'PRIMARY KEY(id,sturl)'+
                ');',
                function(err,res){console.log("create","res")});
        

    var index1="CREATE INDEX ON simplex.urls (sturl);"
    client.execute(index1, function(err, result) {
                console.log("da index is created","err,result");
    });

    var index2="CREATE INDEX ON simplex.urls (url);"
    client.execute(index2, function(err, result) {
                console.log("da index2 is created","err,result");
    });



  var html = '<form action="/" method="post">' +
               'Enter your url:' +
               '<input type="text" name="url" placeholder="..." />' +
               '<br>' +
               '<button type="submit">Submit</button>' +
            '</form>';
               
  res.send(html);
});

console.log("Server has started.");
 


app.post('/', function(req, res){
  var url =req.body.url;
  var sturl;
  var nn=1,html;
  var mm=0;


   var se=client.execute("SELECT * FROM simplex.urls WHERE url = ?;",[url] ,function(err, rs) {
   if(rs.rows.length>0){
	var html = 'short: ' + rs.rows[0].sturl + '.<br>' +
       '<a href="/">Try again.</a>';

         res.send(html);
   }
   else{

  var sign="+-/=?"
  var code="1q2w3e4r5t6y7u8i9o0p1a2s3d4f5g6h7j8k9l0z1x2c3v4b5n6m";
  var d=String(Date.now());
  var st=d[Math.ceil(Math.random()*(d.length-1))];
  for(var i=0;i<url.length;i++){
	try{
	if(sign.search(url[i])!=-1){
	st+=code[Math.ceil(Math.random()*(code.length-1))];
	}}
	catch(e){console.log(e);}
  }
  sturl = 'http://localhost:3000/'+st;
   var id=null;
   if ( ! req.body.hasOwnProperty('id')) {
                id = cassandra.types.uuid();
    } else {
                id = req.body.id;
    };
  client.execute('INSERT INTO simplex.urls (id,sturl,url) VALUES(?,?, ?);',[id,sturl,url],
	function(err,res){console.log("res")});	

//}
var html = 'short: ' + sturl + '.<br>' +
       '<a href="/">Try again.</a>'; 

 res.send(html);
}
})  
});

function afterExecution(errorMessage, successMessage) {
    return function(err, result) {
        if (err) {
            return console.log(errorMessage);
        } else {
            return console.log("successMessage");
        }
    }
}


app.listen(3000);
