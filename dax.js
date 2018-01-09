const AmazonDaxClient = require('amazon-dax-client');
const http = require("http"),
      url = require("url"),
      path = require("path"),
      port = process.argv[9] || 8085;


// var region = "us-east-1";
// var endpoint = "127.0.0.1:8111"
// var tableName = "aqfer-idsync";
// var pkn = "partition-key"
// var pkv = "cid=c016,spid=mmsho.com,suu=15AB34BS232545VDd7841001";
// var skn = "sort-key"
// var skv = "dpid=1";

var region = process.argv[2]
var endpoint = process.argv[3]
var tableName = process.argv[4]
var pkn = process.argv[5]
var skn = process.argv[6]

var dax = new AmazonDaxClient({endpoints: [endpoint], region: region});

http.createServer(function(request, response) {

  var url_parts = url.parse(request.url, true)
  var uri = url_parts.pathname
  var query = url_parts.query

  console.log(url_parts)

	var params = {
		TableName: tableName,
		Key:{}
	};
	params.Key[pkn] = {S: ""},
	params.Key[skn] = {S: ""}


	dax.getItem(params, function(err, data) {
		if (err) {
			console.log(err)
		} else {
	    response.setHeader('Content-Type', 'application/json');
			response.send(data)
			response.writeHead(200);
			response.end();
			dax.shutdown()
		}
	});  

}).listen(parseInt(port, 10));
