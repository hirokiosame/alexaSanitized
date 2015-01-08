// Set up
(function(){

	var sqlConnect = require("./sqlConnect");

	function exec(host, username, password, database, table){
		sqlConnect(host, username, password, database, table, function(err, connection){

			if( err ){ throw err; }
			if( !connection ){ throw new Error("Connection failed to establish"); }
			// Error occurred
			// connection.on('error', function(){
			// 	console.log("Error", arguments);
			// });

			console.log("Ready to import");

			var importAlexa = require("./importDb");

			var importNum = 1000;
			importAlexa(connection, table, function(){
				console.log("Successfully imported", importNum);

				var resolveUrls = require("./resolveUrls")(connection, table);

				resolveUrls(function(){

					console.log("Done");
				});

				// process.exit();
			}, importNum);
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

// dbSetUp = require("./dbSetUp"),
// importAlexa = require("./modules/importAlexa");

/*	read({

	})
	read({ prompt: "Connecting to database:" + require("./dbCredentials").database + "\nContinue?" }, function(err, answer){
		if( answer.toLowerCase() !== 'y' ){ return process.exit(); }

		// Drop all tables
		dbSetUp.dropTables(["inputs", "links", "resHeaders", "pages", "websites"], function(err, rows){

			if( err ){
				return console.log("Error dropping tables", err);
			}

			console.log("Tables dropped");

			dbSetUp.createTables(["websites", "pages", "resHeaders", "links", "inputs"], function(){
				console.log("Tables created");

				console.log("Importing Alexa...");
				importAlexa(function(){
					process.exit();
				});
			});
		});
	});*/
})();