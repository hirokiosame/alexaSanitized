// Set up
(function(){

	var sqlConnect = require("./sqlConnect");

	function exec(host, username, password, database, table){
		sqlConnect(host, username, password, database, table, function(err, connection){

			if( err ){ throw err; }
			if( !connection ){ throw new Error("Connection failed to establish"); }

			console.log("Ready to import");


			var importNum = 100000;

			connection.query("SELECT COUNT(*) FROM `" + table + "`;", function(err, result){
				if( err ){ throw err; }

				// Calculate how many more
				importNum -= result[0]['COUNT(*)'];

				// Proceed to import
				var importAlexa = require("./importDb");

				importAlexa(connection, table, function(){
					console.log("Successfully imported... resolving urls", importNum);

					var resolveUrls = require("./resolveUrls")(connection, table);

					resolveUrls(function(){

						console.log("Done resolving URLS. Exiting.");
						process.exit();
					});
				}, importNum);
			});
		});
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

			exec(host, username, password, database, table);

		}); }); }); }); });
	}else{

		// dbCredentials.host = '127.0.0.1';
		// dbCredentials.user = 'crawler';
		// dbCredentials.password = 'crawling0for0directed0study';
		// dbCredentials.database = 'directedStudy';
		module.exports = exec;
	}
})();