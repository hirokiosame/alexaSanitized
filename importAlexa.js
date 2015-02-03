module.exports = function(connection, tableName, importCount, doneImporting){
	
	// Validation
	if( typeof doneImporting !== "function" ){ throw new Error("Callback must be a function"); }

	if( typeof connection !== "object" || !(connection instanceof Object) ){ doneImporting(new Error("Invalid MySQL connection")); return; }
	if( connection.state !== "authenticated" ){ doneImporting(new Error("Unauthenticated MySQL connection")); return; }

	if( typeof tableName !== "string" || tableName.length === 0 ){ doneImporting(new Error("Invalid table name")); return; }

	if( typeof importCount !== "number" ){
		doneCallback(new Error("Invalid import count")); return;
	}

	if( importCount <= 0 ){ doneImporting(); return; }


	var readLine = require("./readLine"),
		async = require("async");


	var cbTriggered = false,
		imported = 0;
		
	readLine(
		"./top-1m.csv",
		function(progress, rows, nextChunk){

			// console.log("Block", progress, rows.length);

			// Sanitize and Escape
			var hosts = rows.map(function(row){
				return connection.escape(row.split(",")[1]);
			});

			async.whilst(
				function () { return hosts.length; },
				function (callback) {

					// Get Difference
					var diff = (imported + hosts.length) - importCount;

					// Remove extraneous
					var impHosts;
					if( diff > 0 ){ impHosts = hosts.splice(0, hosts.length-diff); }
					else{ impHosts = hosts.splice(0); }


					// Insert
					connection.query(
						"INSERT IGNORE INTO `" + tableName + "` (host) VALUES (" + impHosts.join("),(") + ");",
						function(err, row, status){
							if( err ){
								// console.log("MariaDB Error", err, this.sql);
								return;
							}

							// Keep track
							imported += row.affectedRows;

							// If reached importCount -- terminate
							if( imported === importCount ){ hosts = []; }

							// Next
							callback();
						}
					);
				},
				function (err) {

					// Determine whether to continue
					// More to import
					if( imported < importCount ){
						// console.log(importCount-imported, "Left");
						nextChunk();
					}

					// Done importing
					else{
						!cbTriggered && doneImporting() && (cbTriggered = true);
					}
				}
			);
		},
		function(){
			// console.log("Reached end of file. Imported:", imported);
			!cbTriggered && doneImporting() && (cbTriggered = true);
		}
	)
};