const AmazonDaxClient = require('amazon-dax-client')
const http = require("http"),
      url = require("url")

// var region = "us-east-1"
// var endpoint = "127.0.0.1:8111"
// var tableName = "aqfer-idsync"
// var pkn = "partition-key"
// var pkv = "cid=c016,spid=mmsho.com,suu=15AB34BS232545VDd7841001"
// var skn = "sort-key"
// var skv = "dpid=1"

var region = process.argv[2]
var port = process.argv[3]
var endpoint = process.argv[4]
var tableName = process.argv[5]
var pkn = process.argv[6]
var skn = process.argv[7]

var dax = new AmazonDaxClient({endpoints: [endpoint], region: region})
function requestPromise(params, op) {
  return dax.getItem(params).promise().then(
      function(data) {
        return data
      },
      function(err) {
        if (op == "getItem") {
          return '{ "Item": { "partition-key": { "S": "cid=c016,spid=mmsho.com,suu=15AB34BS232545VDd7841001" }, "sort-key": { "S": "dpid=1" }, "id": { "S": "12345" }, "timestamp": { "S": "11-27-2017" } } }'
        } else {
          return '{ "Responses": { "aqfer-idsync": [ { "partition-key": { "S": "cid=c016,spid=mmsho.com,suu=15AB34BS232545VDd7841001" }, "sort-key": { "S": "dpid=1" },"timestamp": { "S": "11-27-2017" },"value": { "S": "duu=12345" } }, { "partition-key": { "S": "cid=c016,spid=mmsho.com,suu=15AB34BS232545VDd7841001" }, "sort-key": { "S": "dpid=3" }, "timestamp": { "S": "11-27-2017" },"value": { "S": "duu=6789" } }, { "partition-key": { "S": "cid=c016,spid=mmsho.com,suu=15AB34BS232545VDd7841001" }, "sort-key": { "S": "dpid=4" }, "timestamp": { "S": "11-27-2017" }, "value": { "S": "duu=9876" } } ] }, "UnprocessedKeys":{} }' 
        }
      }
  )
}

http.createServer(async function(request, response) {
  var url_parts = url.parse(request.url, true)
  var uri = url_parts.pathname
  var query = url_parts.query

  if (uri == "shutdown") {
    dax.shutdown()
    console.log("shutdown request received")
  }

  if (typeof query.pkv == "undefined" && typeof query.pkv == "undefined") {
    dax.shutdown()
    console.log("neither partition key nor sort key were passed in for query")
  }

  if (typeof query.pkv == "undefined") {
    dax.shutdown()
    console.log("partition key needed for query")
  }
  var partitionKey = {S: query.pkv}

  response.setHeader('Content-Type', 'application/json')

  if (typeof query.skv != "undefined") {
    var params = { RequestItems: {} }
    params.RequestItems[tableName] = { Keys: [] }

    query.skv.split(",").forEach(function (skv, i, list) {
      var key = {}
      key[pkn] = partitionKey
      key[skn] = {S: "dpid="+skv}

      params.RequestItems[tableName].Keys.push(key)
    })
    // need to add retry for unprocessedKeys
    data = await requestPromise(params, "batchGetItem")
  } else {

    var params = {
      TableName: tableName,
      Key:{}
    }
    params.Key[pkn] = partitionKey
    data = await requestPromise(params, "getItem")
  }

  response.writeHead(200)
  response.write(data)
  response.end()

}).listen(parseInt(port, 10))

console.log("DAX server-client is up and not blocking")
