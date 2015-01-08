var fs = require('fs');
module.exports = function readLine(file, chunkCB, endCB, from){

	var progress = from || 0,
		temp = "",
		stream,
		ended = 0;

	return ( stream = fs.createReadStream(file, { start: from, autoClose: true }) )
		.on("data", function(chunk){
			stream.pause();

			temp += chunk.toString();

			// Split by rows
			var rows = temp.split("\n");

			// Reset temp
			temp = rows.pop();

			// CB
			chunkCB(progress, rows, function nextChunk(){

				// Save progress
				progress += chunk.length - temp.length;

				// End
				if( ended ){ return endCB(); }

				// Resume
				stream.resume();
			});

		})
		.on("end", function(){
			ended = 1;
		});
};