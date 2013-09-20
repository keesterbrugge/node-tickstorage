/*
Market tick storage for Node.js
=============

Stores and reads market ticks data in very condensed format and does it very fast.

Writing ticks
--------------
Example usage:

```javascript
var Writer = require('tickstorage').Writer;

writer = new Writer("/tmp/test.ticks");

writer.addTick({
	unixtime: Date.now(),
	volume: 100,
	price: 50000,
	bid: 50100,
	ask: 49900,
	bidSize: 100000,
	askSize: 100000,
	isMarket: true
});

writer.save(fucntion(err) {
	if (err) return console.log(err);

	console.log("Ticks saved successfully");
});
```

See [Writer documentation](lib/Writer.html) for implementation details.


Reading ticks
----------
Example usage:

```javascript
var Reader = require('tickstorage').Reader;

reader = new Reader("/tmp/test.ticks");

reader.load(function(err) {
	if (err) return console.error(err);

	console.log("Ticks count: ", reader.length);

	var tick, totalVolume = 0;
	while(tick = reader.nextTick()) {
		if (tick.isMarket) {
			totalVolume += tick.volume;
		}
	}

	console.log("Total volume: ", totalVolume);
});
```

Reader supports tick files created by [stock module](http://github.com/egorFiNE/node-stock).

See [Reader documentation](lib/Reader.html) for implementation details.

*/
var
	Reader = require('./lib/Reader'),
	Writer = require('./lib/Writer');

module.exports = {
	Reader: Reader,
	Writer: Writer
};
