
// Create table
function createStatusTable(connection, table, callback){
	connection.query('CREATE TABLE IF NOT EXISTS `' + table + '_statuses`(' +
		'statusId INT NOT NULL AUTO_INCREMENT,' +
		'PRIMARY KEY (statusId),' +
		'statusMessage VARCHAR(100) NOT NULL,' +
		'UNIQUE (`statusMessage`)' +
	');', function(err, rows){
		if( err ){
			console.log(err, this.sql);
			return callback(err);
		}

		console.log("Successfully error table.");
		callback(null, rows);
	});	
}

// Create table
function createTable(connection, table, callback){
	connection.query('CREATE TABLE IF NOT EXISTS `' + table + '`(' +
		'id INT NOT NULL AUTO_INCREMENT,' +
		'PRIMARY KEY (id),' +
		'host VARCHAR(100) NOT NULL,' +
		'UNIQUE (`host`),' +
		'resolvedTo VARCHAR(100) DEFAULT NULL,' +
		'UNIQUE (`resolvedTo`),' +
		'status INT DEFAULT NULL,' +
		'FOREIGN KEY(status) REFERENCES ' + table + '_statuses(statusId)' +
	');', function(err, rows){
		if( err ){
			console.log(err, this.sql);
			return callback(err);
		}

		console.log("Successfully created table.");
		callback(null, rows);
	});	
}

module.exports = function(credentials, callback){

	var mysql = require("mysql");

	// Validate credentials
	if( typeof credentials !== "object" || !(credentials instanceof Object) ){
		callback(new Error("Credentials is not a valid object")); return;
	}

	if( !["host", "user", "password", "database", "table"].every(function(property){
		return typeof credentials[property] === "string" && credentials[property].length > 0;
	}) ){
		callback(new Error("Invalid credential properties"));
		return;
	}

	credentials.multipleStatements = true;


	var connection = mysql.createConnection(credentials);

	// Connect
	connection.connect(function(err){

		if( err ){ return callback(err); }

		// Catches ctrl+c event to exit properly
		process
		.on('SIGINT', process.exit)

		// Cleanup before exit
		.on('exit', function(){

			// Kill MySql connection
			connection.end();
		});


		// Check if table exists and get stats if does
		connection.query(
			"SELECT * FROM information_schema.tables WHERE table_schema = '" + credentials.database + "' AND table_name = '" + credentials.table + "' LIMIT 1;",
			function(err, rows){
				if( err ){ return callback(err); }

				// If tables don't exist, Create
				if( rows.length === 0 ){
					createStatusTable(connection, credentials.table, function(err, fields){
						if( err ){ return callback(err); }

						createTable(connection, credentials.table, function(err, fields){
							if( err ){ return callback(err); }

							// Success
							callback(null, connection);
						});
					});
				}

				// Already Exists - Success
				else{ callback(null, connection); }

				/*
				// Delete
				else{
					console.log("Table already exists.");

					// connection.query("DROP TABLE `" + table + "`;", function(err, row){
					// 	if( err ){ throw err; }
						// console.log("drop table", row);
						createTable(table, function(err, fields){
							if(err){ return console.log(err); }
							console.log(fields);
							callback(null, connection);
						});
					// });
				}*/
			}
		);
	});
};