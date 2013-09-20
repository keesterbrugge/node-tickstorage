/*
Tick database reader
=============
*/
var
	fs = require('fs'),
	zlib = require('zlib'),
	common = require('./common'),
	uncompress = require('compress-buffer').uncompress;

/*
Initialize reader.

You should call `load` before using any other methods or properties.

@param {String} path path to tick file.
*/
function Reader(path) {
	this._path = path;

	this.length = 0;

	this._ticksBuffer = null;
	this._position = 0;
	this._loaded = false;

	// Specific implementation of `_readTick` will be determined during `load`.
	this._readTick = function() { throw new Error("Ticks not loaded properly."); };
}

/*
Load ticks file.

It will return error to callback if file doesn't exist, has invalid format or some other problem occured.

After this method is complete, you can call `nextTick` and/or `getBuffer`.
*/
Reader.prototype.load = function(callback) {
	var _this=this;
	if (!fs.existsSync(this._path)) {
		return callback(new Error("File does not exists:" + this._path));
	}

	fs.open(this._path, "r", function(err, fd) {
		if (err) {
			return callback(err);
		}

		fs.fstat(fd, function(err, stats) {
			//} if fs.open succeed, then fs.stats should not fail, but, you know...
			if (err) {
				return callback(err);
			}

			_this._loadHeader(fd, function(err, headerValues) {
				if (err) {
					return callback(err);
				}

				if (headerValues.version == 1) {
					_this._loadV1(fd, stats, headerValues, callback);
				} else {
					_this._loadV2(fd, stats, headerValues, callback);
				}
			});
		});
	});
};

/*
Returns tick at a given position (starting with 0).

@param {Number} position position of tick you want to read (should be from 0 to reader.length).
*/
Reader.prototype.tickAtPosition = function(position) {
	if (!this._loaded) {
		throw new Error("You should call `load` before `tickAtPosition`");
	}

	if (position < this.length) {
		var offset = position * this._tickSize;
		return this._readTick(offset);
	} else {
		return null;
	}
};

/*
Reads adn returns next tick.
Returns null if there is no more ticks.
*/
Reader.prototype.nextTick = function() {
	if (!this._loaded) {
		throw new Error("You should call `load` before `nextTick`");
	}

	if (this._position < this.length) {
		var offset = this._position * this._tickSize;
		var tick = this._readTick(offset);
		this._position++;
		return tick;
	} else {
		return null;
	}
};

/*
Returns raw buffer of ticks.

Method may be useful if you, for example, want to pass all ticks to native addon.

It's plain array of tick structures, each of which has following format:

- 4 bytes - unixtime (seconds) of tick
- 2 bytes - milliseconds of tick unixtime
- 4 bytes - tick volume
- 4 bytes - deal price
- 4 bytes - bid price
- 4 bytes - ask price
- 4 bytes - bid size
- 4 bytes - ask size
- 1 byte - flags. Currently there is only 1 flag and 2 possible values: 1 - it's market tick, 0 - it isn't market tick

All multibyte numbers are little-endian.

All prices (price, bid, ask) are multiplied by 10000 (e.g. $0.01 will be 100, $1 will be 10000).

@return {Buffer} buffer of ticks
*/
Reader.prototype.getBuffer = function() {
	return this._ticksBuffer;
};

/*
Read tick from old storage (version 1)

@param {Number} offset offset in bytes from the start of `_ticksBuffer` where tick data is located.
*/
Reader.prototype._readTickV1AtOffset = function(offset) {
	return {
		unixtime: this._ticksBuffer.readUInt32LE(offset, false) * 1000,
		volume: this._ticksBuffer.readUInt32LE(offset + 4, false),
		price: this._ticksBuffer.readUInt32LE(offset + 8, false),
		isMarket: this._ticksBuffer.readUInt8(offset + 12, false) == 1
	};
};

/*
Read tick from new storage (version 2)
*/
Reader.prototype._readTickV2AtOffset = function(offset) {
	return {
		unixtime: this._ticksBuffer.readUInt32LE(offset, false) * 1000 + this._ticksBuffer.readUInt16LE(offset + 4, false),
		volume: this._ticksBuffer.readUInt32LE(offset + 6, false),
		price: this._ticksBuffer.readUInt32LE(offset + 10, false),
		bid: this._ticksBuffer.readUInt32LE(offset + 14, false),
		ask: this._ticksBuffer.readUInt32LE(offset + 18, false),
		bidSize: this._ticksBuffer.readUInt32LE(offset + 22, false),
		askSize: this._ticksBuffer.readUInt32LE(offset + 26, false),
		isMarket: this._ticksBuffer.readUInt8(offset + 30, false) == 1
	};
};

/*
Loads old storage file (version 1)
Creates and fills `_tickBuffer`.
Fills `length` property.

@param {Number} fd file descriptor to read from.
@param {Object} stats file stats returned by fs.stats
@param {Object} headerValues values read from file header
@param {Function} callback
*/
Reader.prototype._loadV1 = function(fd, stats, headerValues, callback) {
	var _this=this;

	var compressedBuffer = new Buffer(stats.size - common.HEADER_SIZE - headerValues.minuteIndexSize);

	fs.read(fd, compressedBuffer, 0, compressedBuffer.length, common.HEADER_SIZE + headerValues.minuteIndexSize, function(err) {
		if (err) {
			return callback(err);
		}

		try {
			_this._ticksBuffer = uncompress(compressedBuffer);
		} catch (e) {
			return callback(e);
		}

		_this._tickSize = 13;
		_this.length = _this._ticksBuffer.length / _this._tickSize;

		_this._readTick = _this._readTickV1AtOffset;

		_this._loaded = true;

		callback();
	});
};

/*
Load new storage file (version 2)

Arguments are the same as for _loadV1
*/
Reader.prototype._loadV2 = function(fd, stats, headerValues, callback) {
	var _this=this;

	var compressedBuffer = new Buffer(stats.size - common.HEADER_SIZE);

	fs.read(fd, compressedBuffer, 0, compressedBuffer.length, common.HEADER_SIZE, function(err) {
		if (err) {
			return callback(err);
		}

		zlib.inflate(compressedBuffer, function(err, _ticksBuffer) {
			if (err) {
				return callback(err);
			}

			_this._ticksBuffer = _ticksBuffer;
			_this._tickSize = 31;
			_this.length = _this._ticksBuffer.length / _this._tickSize;

			_this._readTick = _this._readTickV2AtOffset;

			_this._loaded = true;

			callback();
		});
	});
};

/*
Every storage file has a fixed-sized (common.HEADER_SIZE) header.
It contains storage version and can contain other metadata in future.
*/
Reader.prototype._loadHeader = function(fd, callback) {
	var headerBuffer = new Buffer(common.HEADER_SIZE);

	fs.read(fd, headerBuffer, 0, common.HEADER_SIZE, 0, function(err) {
		if (err) {
			return callback(err);
		}

		var headerString = headerBuffer.toString().split("\n")[0];
		var json = null;
		try {
			json = JSON.parse(headerString);
		} catch(e) {
			return callback(e);
		}

		callback(null, json);
	});
};

module.exports = Reader;
