var
	chai = require('chai'),
	assert = chai.assert,
	fs = require('fs'),
	TickStorage = require('../index.js'),
	Reader = TickStorage.Reader,
	Writer = TickStorage.Writer;

function unixtime() {
	return parseInt(Date.now()/1000, 10);
}

describe("TickStorage/Read old storage", function() {

	it("should create directories and file", function() {
		var path = __dirname + "/data/tmp/somedir/createThis.ticks";
		var writer = new Writer(path);
		writer.save();
		assert.ok( fs.existsSync(path));
	});

	it("should overwrite existant file", function() {
		var path = __dirname + '/data/tmp/rewrite.ticks';
		fs.writeFileSync(path, "test content");
		assert.equal(fs.readFileSync(path, "utf8"), "test content");

		var writer = new Writer(path);
		writer.save();
		assert.ok( fs.existsSync(path) );
		assert.notEqual(fs.readFileSync(path, "utf8"), "test content");
	});

	it("should read what it wrote", function() {
		var path = __dirname + '/data/tmp/readWhatItWrote.ticks';
		var writer = new Writer(path);
		var ticks = [
			{
				unixtime: unixtime(),
				msec: 123,
				volume: 100,
				price: 14134,
				bid: 14145,
				ask: 14245,
				bidSize: 35200,
				askSize: 1000,
				isMarket: true
			},
			{
				unixtime: unixtime(),
				msec: 234,
				volume: 100,
				price: 14134,
				bid: 123,
				ask: 124,
				bidSize: 125,
				askSize: 126,
				isMarket: false,
			},
			{
				unixtime: unixtime(),
				msec: 345,
				volume: 99,
				price: 1,
				bid: 123,
				ask: 124,
				bidSize: 125,
				askSize: 126,
				isMarket: false
			}
		];
		ticks.forEach(function(tick) {
			writer.addTick(tick);
		});
		writer.save();
		var readTicks = readAllTicks(path);
		assert.deepEqual(ticks, readTicks);
	});

	it("should support large datasets", function() {
		var path = __dirname + '/data/tmp/largeDataSet.ticks';
		var writer = new Writer(path);
		var ticksCount = 1000000;

		var tick = {
			unixtime: unixtime(),
			msec: 556,
			price: 100,
			volume: 100
		}
		for(var i = 0; i < ticksCount; ++i) {
			writer.addTick(tick);
		}

		writer.save();

		var reader = new Reader(path);
		reader.load();
		assert.equal(reader.length, ticksCount);

		fs.unlinkSync(path);
	});

	it("should handle non-int values tick values", function() {
		var path = __dirname + '/data/tmp/acceptInvalidValues.ticks';
		var writer = new Writer(path);
		writer.addTick({
				unixtime: 0,
				msec: 0,
				volume: "100",
				price: "150.2",
				bid: "bid",
				ask: "ask",
				bidSize: "0"
		});
		writer.save();
		var readTicks = readAllTicks(path);
		var tick = readTicks[0];
		assert.deepEqual(tick, {
			unixtime: 0,
			msec: 0,
			volume: 100,
			price: 150,
			bid: 0,
			ask: 0,
			bidSize: 0,
			askSize: 0,
			isMarket: false
		});
	});

	function readAllTicks(path) {
		var reader = new Reader(path);
		reader.load();
		var ticks = [];
		var tick;
		while (tick = reader.nextTick()) {
			ticks.push(tick);
		}
		return ticks;
	}

});
