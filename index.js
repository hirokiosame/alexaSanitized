// Set up
(function(){

	var sqlConnect = require("./sqlConnect");

	function main(credentials, importCount, doneCallback){

		// Input Validation
		if( typeof doneCallback !== "function" ){
			throw new Error("Callback is not function");
		}

		// Fetch rows to check how many have already been imported
		function checkAlreadyImported(connection, table, cb){
			connection.query("SELECT COUNT(*) FROM `" + table + "`;", function(err, result){

				if( err ){ cb(err); return; }

				cb(null, result[0]['COUNT(*)']);
			});
		}

		// Make SQL connection and establish tables
		sqlConnect(
			credentials,
			function(err, connection){

				// On error
				if( err ){ doneCallback(err); return; }

				// Failed connection
				if( !connection ){ doneCallback(new Error("Connection failed to establish")); return; }

				checkAlreadyImported(connection, credentials.table, function(err, count){

					if( err ){ doneCallback(err); return; }

					// Calculate how many more
					importCount -= count;

					// Proceed to import
					var importAlexa = require("./importAlexa");

					importAlexa(connection, credentials.table, importCount, function(){

						var resolveUrls = require("./resolveUrls")(connection, credentials.table);

						// Start resolving URLs
						resolveUrls(doneCallback);
					});
				});
			}
		);
	}

	// Direct
	if( require.main === module ){

		var read = require("read");

		function check( prompt, value, callback ){
			if( value === undefined ){
				read({ prompt: prompt+": " }, function(err, answer){
					if( err ){ return callback(err); }
					else{ return callback(null, answer); }
				});
			}else{
				// Return
				console.log(prompt+":", value);
				callback(null, value);
			}
		}

		// Get commandline arguments
		var args = process.argv.slice(2);

		// Else, prompt user
		check("Host", args[0], function(err, host){

		if( err ){ throw err; }
		check("Username", args[1], function(err, username){

		if( err ){ throw err; }
		check("Password", args[2], function(err, password){

		if( err ){ throw err; }
		check("Database", args[3], function(err, database){

		if( err ){ throw err; }
		check("Table", args[4], function(err, table){

		if( err ){ throw err; }
		check("Number of hosts to import", args[5], function(err, importCount){

		if( err ){ throw err; }

			main({
				host: host,
				user: username,
				password: password,
				database: database,
				table: table
			}, importCount, function(err){
				if( err ){ throw err; }
				else{
					console.log("Done importing and resolving URLs!");
				}

				process.exit();
			});

		}); }); }); }); }); });
	}else{
		module.exports = main;
	}
})();