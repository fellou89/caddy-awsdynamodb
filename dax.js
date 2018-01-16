const AmazonDaxClient = require('amazon-dax-client')
const http = require("http"),
      url = require("url")

var region = process.argv[2]
var port = process.argv[3]
var endpoint = process.argv[4]
var tableName = process.argv[5]
var pkn = process.argv[6]
var skn = process.argv[7]

var dax = new AmazonDaxClient({endpoints: [endpoint], region: region})
function requestPromise(params, op) {
  var promise
  if (op == "getItem") {
    promise = dax.getItem(params).promise()
  } else {
    promise = dax.batchGetItem(params).promise()
  }

  return promise.then(
    function(data) {
      // need to add retry for unprocessedKeys
      return JSON.stringify(data)
    },
    function(err) {
      console.log(err)
      if (op == "getItem") {
        return '{ "Item": { "partition-key": { "S": "cid=c016,spid=mmsho.com,suu=15AB34BS232545VDd7841001" }, "sort-key": { "S": "dpid=1" }, "value": { "S": "duu=12345" }, "timestamp": { "S": "11-27-2017" } } }'
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

  // if (uri == "shutdown") {
  //   console.log("shutdown request received")
  //   response.writeHead(200)
  //   response.end()
  // }

  if (request.method != "GET" && request.method != "PUT") {
    console.log("dax server only takes put and get request methods")
    response.writeHead(400)
    response.end()
  }

  if (typeof query.pkv == "undefined" && typeof query.pkv == "undefined") {
    console.log("partition key and sort key are needed for query")
    response.writeHead(400)
    response.end()
  }

  var partitionKey = {S: query.pkv}

  response.setHeader('Content-Type', 'application/json')

  var opName
  var sortKeys = query.skv.split(",")

  if (sortKeys.length > 1) {
    var params = { RequestItems: {} }

    if (request.method == "GET") {
      params.RequestItems[tableName] = { Keys: [] }

      sortKeys.forEach(function (skv, i, list) {
        var key = {}
        key[pkn] = partitionKey
        key[skn] = {S: skv}

        params.RequestItems[tableName].Keys.push(key)
      })
      opName = "batchGetItem"

    } else {

      params.RequestItems[tableName] = []

      sortKeys.forEach(function (skv, i, list) {
        request = {PutRequest: { Item: {}}}
        request.PutRequest.Item[pkv] = partitionKey
        request.PutRequest.Item[skv] = {S: skv}
        // need to add timestamp and value attributes

        params.RequestItems[tableName].push(request)
      })
      opName = "batchWriteItem"
    }

  } else {
    var params = {
      TableName: tableName,
    }
    if (request.method == "GET") {
      params['Key'] = {}
      params.Key[pkn] = partitionKey
      params.Key[skn] = {S: sortKeys[0]}
      opName = "getItem"

    } else {
      params['Item'] = {}
      params.Item[pkn] = partitionKey
      params.Item[skn] = sortKey
      // need to add timestamp and value attributes

      opName = "putItem"
    }
  }

  var data = await requestPromise(params, opName)
  response.writeHead(200)
  response.write(data)
  response.end()

}).listen(parseInt(port, 10))

console.log("DAX server-client is up and not blocking on port: "+port)
