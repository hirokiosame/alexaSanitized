module.exports = (function(){
	var async = require("async"),
		url = require("url"),
		protocols = {
			http: require('http'),
			https: require('https')
		};

		protocols.http.globalAgent.maxSockets = Infinity;
		protocols.https.globalAgent.maxSockets = Infinity;


	require('ssl-root-cas').inject();


	function createError(msg, attrs){
		var err = new Error(msg);

		if( attrs && typeof attrs === "object" ){ for( var attr in attrs ){ err[attr] = attrs[attr]; } }
		return err;
	}

	function ping(uri, callback, count){

		// Validation: callback
		if( typeof callback !== "function" ){ throw new Error("Callback not a function"); }

		// Validation: count
		count = count || 0;
		if( typeof count !== "number"){ return callback(createError("count must be a number", { input: count })); }

		// Validation: uri
		if( !uri || typeof uri !== "object" ){ return callback(createError("Invalid uri", { input: uri })); }

		// Validation: protocol
		var  protocol = uri.protocol.slice(0, -1);
		if( !protocols.hasOwnProperty(protocol) ){ return callback(createError("Invalid protocol", { input: protocol })); }


		var triggered = 0,
			req = protocols[protocol]
					.request({
						hostname: uri.hostname,
						path: uri.path || "/",
						method: 'GET',
						headers: {
							"Accept": "*/*",
							"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.104 Safari/537.36",
							"Connection": "close"
						},
						agent: false
					}, function(res){

						// Callback not yet triggered
						if( !triggered ){

							// Redirect
							if( res.headers.location ){

								// If too many redirects
								if( count === 10 ){
									callback(createError("Exceeded 10 redirects"));
								}else{
									ping( url.parse(url.resolve(url.format(uri), res.headers.location)), callback, ++count);
								}
							}
							else{
								res.uri = uri;
								callback(null, res);
							}
							triggered++;
						}

						// Close socket
						res.destroy();
						req.destroy();
					})
					.on("error", function(err){

						// Trigger Callback
						if( !triggered ){ callback(err); triggered++; }

						req.destroy();
					});

			// Set timeout of 10 sec
			req.setTimeout(10000, function(){

				// Trigger Callback
				if( !triggered ){ callback(new Error("ping timeout")); triggered++; }

				req.destroy();
			});

		req.end();
	}

	function pingHttps(host, callback, www){

		// Append www
		if( www ){ host = "www." + host; }

		ping({
			protocol: 'https:',
			host: host,
			hostname: host
		}, function(err, res){

			if( err ){

				// DNS can't be resolved
				// if( err.message === "getaddrinfo ENOTFOUND" || err.message === "getaddrinfo EIO" ){
				if( err.syscall === "getaddrinfo" ){
					// Try Adding www
					if( !www ){ return pingHttps(host, callback, 1); }

					// Could not resolve host
					else{ return callback(new Error("Cannot resolve host")); }
				}

				// Try HTTP
				return pingHttp(host, callback);
			}

			// Success
			callback(null, res);
		});
	}


	function pingHttp(host, callback){
		// If http redirects back to https, cert errors can reoccur
		ping({
			protocol: 'http:',
			host: host,
			hostname: host
		}, callback);
	}

	function detProtocol(host, callback){

		// Validate callback
		if( typeof callback !== "function" ){ throw new Error("Callback must be a function"); }

		// Validate host
		if( typeof host !== "string" ){ return callback( createError("Host must be a string", { input: host }) ); }


		pingHttps(host, callback);
	}

	var errCodes = require("./errorCodes");

	return {
		processRows: function(rows, callback){
			var hosts = [];
			async.eachLimit(
				rows, 10,
				function perRow(row, nextRow){
					detProtocol(row.host, function(err, res){

						if( err ){
							// console.log("Error", row);
							if( errCodes.hasOwnProperty(err.message) ){

								row.status = errCodes[err.message];	
							}else{
								console.log("Uncaught Error:", err.message, errCodes, row);
							}
						}else{
							["search", "query", "path", "pathname", "href"].forEach(function(p){
								delete res.uri[p];
							});
							row.resolvedTo = url.format(res.uri);
							row.status = 1;
						}
						hosts.push(row);
						
						nextRow();
					});
				},
				function(){
					callback(hosts);
				}
			);
		}
	};
})();