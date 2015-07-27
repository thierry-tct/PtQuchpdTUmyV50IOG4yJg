/***
* Author: Titcheu Chekam Thierry
* 
* This program gets the exchange rates from Yahoo Finance which is free.
* xe.com licence don't allow to freely and automatically get the exchange rates,
* from xe.com page source, it says: 
* <!-- WARNING: Automated extraction of rates is prohibited under the Terms of Use. -->
*/

"use strict";

var tube_name = 'thierry-tct';

//number of results: integer >0
var NUM_RESULTS = 10;	

//value in milliseconds
var ITERVAL_MS = 60 * 1000;	

//number of retries to get the rate	
var ATTEPTS = 3;	

//value in miliseconds
var RETRY_DELAY = 3000;		

/**
	* Mongolab insertion into database for thierry-tct
	*
	* @param {object} json - JSON representation of the document to be inserted
	*/
function insertToMongolab(json) {
	var https = require('https');
	var my_db = 'currency_conversion_rate';
	var my_coll = 'currency_conversion_rate_coll';
	var my_api_key = 'JbZiHSs9zVVkrKHbKUGQC82a3LTwmoIW';
	var options_path = '/api/1/databases/' + my_db + '/collections/' + my_coll + '?apiKey=' + my_api_key;
	var options = {
		host: 'api.mongolab.com',
		path: options_path,
		
		//https default port 443
		port: '443',	
		method: 'POST',
		headers: {'content-type': 'application/json;charset=utf-8'},
	};

	function mongoCallback(response) {
		var str = '';
		response.on('data', function(chunk) {
			str += chunk;
		});
		response.on('end', function() {
			console.log(str);
		});
	}

	//make an http POST request to mongolab to insert the document
	var req = https.request(options, mongoCallback);
	req.on('error', function(e) {
		console.log('problem with request: ' + e.message);
	});
	req.write(JSON.stringify(json));
	req.end();
}

/**
	* Processor of the exchange rate obtained (store into mongolab)
	*
	* @param {String} currency_from - currency to be converted from
	* @param {String} currency_to - currency to be converted to
	* @param {String} rate - conversion rate between currency_from and currency_to
	*/
function useExchangeRate(currency_from, currency_to, rate) {	
	var data = {
		"from": currency_from,
		"to": currency_to,
		"created_at": (new Date()).toString(),

		//round rate to 2 decimals
		"rate": parseFloat(rate).toFixed(2).toString(),
	};
	insertToMongolab(data);
};

/**
	* obtain the conversion rate between two currencies from Yahoo Finance
	*
	* @param {String} currency_from - currency to be converted from
	* @param {String} currency_to - currency to be converted to
	* @param {Number} num_runs_left - number of exchange rate values to obtain 
	* @param {Number} attemps_left - number of attepts to retry in case of failure  
	*/
function getUseExchangeRate(currency_from, currency_to, num_runs_left, attemps_left) {
	var http = require('http');
	var options_path = '/d/quotes.csv?s=' + currency_from + currency_to + '=X&f=l1&e=.csv';

	//using Yahoo finance API
	var options = {
		host: 'download.finance.yahoo.com',
		path: options_path,
		port: '80',
		method: 'GET',
	};

	function yahooCallback(response) {
		var str = '';
		response.on('data', function(chunk) {
			str += chunk;
		});

		response.on('end', function() {
			useExchangeRate(currency_from, currency_to, str);
			if (num_runs_left > 1)
				setTimeout(function() {
					getUseExchangeRate(currency_from , currency_to, num_runs_left - 1, ATTEPTS);}, ITERVAL_MS);
		});
	}

	//make an http POST request to yahoo finace and call yahooCallback to process response
	var req = http.request(options, yahooCallback);

	req.on('error', function(e) {
		console.log('problem with request: ' + e.message + '\nretrying...');
		if (attemps_left > 0)
			setTimeout(function() {
				getUseExchangeRate(currency_from , currency_to, num_runs_left, attemps_left - 1);}, RETRY_DELAY);
	});

	req.end();
}

/**
	* Worker main function: get a job from beanstalkd server through node-beanstalkd client and work.   
	*/
function consumerWorker() {
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
		function getJobAndWork(err, conn) {
			if (err) {
				console.log('Error connecting to beanstalkd.');
				console.log('Make sure that beanstalkd is running.');
			} else {

				//select tube
				conn.watch(tube_name, function(err) {
					if (err) {
						console.log('Error watching tube.');
					} else {
						console.log('watching tube ' + tube_name);

						//pick a job from queue's tube 
						conn.reserve(function(err, id, json) {
							if (err) {
								console.log('Error reserving job.');
							} else {
								var currency_from = JSON.parse(json).from;
								var currency_to = JSON.parse(json).to;
								console.log('Consumed Job ' + id);
								console.log('Rate from ' + currency_from + ' to ' + currency_to);

								//process the Job
								getUseExchangeRate(currency_from , currency_to, NUM_RESULTS, ATTEPTS);
	
								//Wheather Fail or succeed work, the job is reput to the tube
								conn.release(id, 0, 0, function(err) {	
									if (err) {
										console.log('Error releasing job.');
									} else {
										console.log('Job released back to tube');
										conn.end();
									}
								});
							}
						});
					}
				});
			}
		}

		response.on('end', function() {
			//Use beanstalkd client to get a job for work
			var bs = require('./node-beanstalkd');
			var client = new bs.Client();
			var host = JSON.parse(str).data.host;
			var port = JSON.parse(str).data.port;
			client.connect(host + ':' + port, getJobAndWork); 
		});
	}

	//make an http POST request to the beanstakd server, 
	//to obtain information to connect node-beanstalkd client
	var req = http.request(options, beanstalkCallback);
	req.on('error', function(e) {
		console.log('problem with request: ' + e.message);
	});
	req.end();
}

// Execute Consumer
consumerWorker();
