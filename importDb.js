module.exports = function(connection, tableName, doneImporting, max){
	
	if( max <= 0 ){ return doneImporting(); }

	var readLine = require("./readLine"),
		async = require("async");


	var imported = 0;
	readLine(
		"./top-1m.csv",
		function(progress, rows, nextChunk){

			console.log("Block", progress, rows.length);

			// Sanitize and Escape
			var hosts = rows.map(function(row){
				return connection.escape(row.split(",")[1]);
			});


			async.whilst(
				function () { return hosts.length; },
				function (callback) {

					// Get Difference
					var diff = (imported + hosts.length) - max;

					// console.log(imported, hosts.length, max, diff);

					// Remove extraneous
					var impHosts;
					if( diff > 0 ){ impHosts = hosts.splice(0, hosts.length-diff); }
					else{ impHosts = hosts.splice(0); }


					// Insert
					// console.log("Importing", impHosts, impHosts.length);
					connection.query(
						"INSERT IGNORE INTO `" + tableName + "` (host) VALUES (" + impHosts.join("),(") + ");",
						function(err, row, status){
							if( err ){ return console.log("MariaDB Error", err, this.sql); }

							// Keep track
							imported += row.affectedRows;

							// console.log("Imported:", row.affectedRows, imported);

							// If reached max -- terminate
							if( imported === max ){ hosts = []; }

							// Next
							callback();
						}
					);
				},
				function (err) {

					console.log("Done Importing!");
					// Determine whether to continue
					// More to import
					if( imported < max ){ console.log(max-imported, "Left"); nextChunk(); }

					// Done importing
					else{
						doneImporting();
					}
				}
			);
		},
		function(){
			console.log("Reached end of file. Imported:", imported);
			doneImporting();
		}
	)
};