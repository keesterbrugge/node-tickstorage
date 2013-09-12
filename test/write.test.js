var
	chai = require('chai'),
	assert = chai.assert,
	fs = require('fs'),
	TickStorage = require('../index.js'),
	Reader = TickStorage.Reader,
	Writer = TickStorage.Writer;

function unixtimeMsec() {
	return parseInt(Date.now()/1000, 10) * 1000;
}

describe("TickStorage/Read old storage", function() {

	it("should create directories and file", function(done) {
		var path = __dirname + "/data/tmp/somedir/createThis.ticks";
		try { 
			fs.unlinkSync(path);
		} catch(e) {
		}

		var writer = new Writer(path);
		writer.save(function(err) {
			assert.ok(!err);
			assert.ok(fs.existsSync(path));
			done();
		});
	});

	it("should overwrite existing file", function(done) {
		var path = __dirname + '/data/tmp/rewrite.ticks';
		fs.writeFileSync(path, "test content");
		assert.equal(fs.readFileSync(path, "utf8"), "test content");

		var writer = new Writer(path);
		writer.save(function(err) {
			assert.ok(!err);
			assert.ok(fs.existsSync(path));
			assert.notEqual(fs.readFileSync(path, "utf8"), "test content");
			done();
		});
	});

	it("should read what it wrote", function(done) {
		var path = __dirname + '/data/tmp/readWhatItWrote.ticks';
		var writer = new Writer(path);
		var ticks = [
			{
				unixtime: unixtimeMsec() + 123,
				volume: 100,
				price: 14134,
				bid: 14145,
				ask: 14245,
				bidSize: 35200,
				askSize: 1000,
				isMarket: true
			},
			{
				unixtime: unixtimeMsec() + 234,
				volume: 100,
				price: 14134,
				bid: 123,
				ask: 124,
				bidSize: 125,
				askSize: 126,
				isMarket: false,
			},
			{
				unixtime: unixtimeMsec() + 345,
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
		writer.save(function(err) {
			assert.ok(!err);
			readAllTicks(path, function(err, readTicks) {
				assert.ok(!err);
				assert.deepEqual(ticks, readTicks);
				done();
			});
		});
	});

	it("should support large datasets", function(done) {
		var path = __dirname + '/data/tmp/largeDataSet.ticks';
		var writer = new Writer(path);
		var ticksCount = 1000000;

		var tick = {
			unixtime: unixtimeMsec() + 556,
			price: 100,
			volume: 100
		}

		for(var i = 0; i < ticksCount; ++i) {
			writer.addTick(tick);
		}

		writer.save(function(err) {
			assert.ok(!err);
			var reader = new Reader(path);
			reader.load(function(err) {
				assert.ok(!err);
				assert.equal(reader.length, ticksCount);
				fs.unlinkSync(path);
				done();
			});
		});
	});

	it("should handle non-int values tick values", function(done) {
		var path = __dirname + '/data/tmp/acceptInvalidValues.ticks';
		var writer = new Writer(path);
		writer.addTick({
				unixtime: 0,
				volume: "100",
				price: "150.2",
				bid: "bid",
				ask: "ask",
				bidSize: "0"
		});

		writer.save(function(err) {
			assert.ok(!err);
			readAllTicks(path, function(err, readTicks) {
				assert.ok(!err);
				var tick = readTicks[0];
				assert.deepEqual(tick, {
					unixtime: 0,
					volume: 100,
					price: 150,
					bid: 0,
					ask: 0,
					bidSize: 0,
					askSize: 0,
					isMarket: false
				});
				done();
			});
		});
	});

	function readAllTicks(path, callback) {
		var reader = new Reader(path);
		reader.load(function(err) {
			var ticks = [];
			var tick;
			while (tick = reader.nextTick()) {
				ticks.push(tick);
			}
			callback(err, ticks);
		});
	}
});
