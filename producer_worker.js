/***
* Author: Titcheu Chekam Thierry
*/

"use strict";

var tube_name = 'thierry-tct';

var data = {
      			"from": "HKD",
				"to": "USD",
    		};
function producerWorker() {
	var http = require('http');
	var options = {
  		host: 'challenge.aftership.net',
  		path: '/v1/beanstalkd',
  		port: '9578',
  		method: 'POST',
  		headers: {'content-type': 'application/json;charset=utf-8', 
				'aftership-api-key': 'a6403a2b-af21-47c5-aab5-a2420d20bbec'},
	};

	function beanstalkCallback(response) {
 	 	var str = '';
 	 	response.on('data', function(chunk) {
    		str += chunk;
  		});

		function putJob(err, conn) {
			if (err) {
    			console.log('Error connecting to beanstalkd.');
    			console.log('Make sure that beanstalkd is running.');
  			} else {
				conn.use(tube_name, function(err, id, json) {
					if (err) {
                		console.log('Error using tube.');
              		} else {
						console.log('using tube ' + tube_name);
                		conn.put(0, 0, 1, JSON.stringify(data), function(err, id) {
          					if (err) {
            					console.log('Error putting job.');
          					} else {
            					console.log('Produced Job ' + id);
								conn.end();
          					}
        				});
              		}
      			});
			}
		}

  		response.on('end', function () {
			var bs = require('./node-beanstalkd');
			var client = new bs.Client();
			var host = JSON.parse(str).data.host;
			var port = JSON.parse(str).data.port;
			client.connect(host + ':' + port, putJob); 
  		});
	}

	var req = http.request(options, beanstalkCallback);
	req.end();
}

// Execute Producer
producerWorker();
