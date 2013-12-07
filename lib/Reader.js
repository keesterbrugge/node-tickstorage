/*
Tick database reader
=============
*/
var
	fs = require('fs'),
	zlib = require('zlib'),
	common = require('./common'),
	Readable = require('stream').Readable;

/*
Initialize reader.

You should call `load` before using any other methods or properties.

@param {String or Readable} where to read result from. Could be either readable stream or path of file to read.
*/
function Reader(input) {
	this._input = input;

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
	var self = this;

	this._readInputBuffer(function(err) {
		if (err) return callback(err);

		self._loadHeader(function(err, headerValues) {
			if (err) {
				return callback(err);
			}

			if (headerValues.version == 1) {
				self._loadV1(headerValues, callback);
			} else {
				self._loadV2(headerValues, callback);
			}
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
Read input stream (or file) and set `_inputBuffer` varialbe to the Buffer containing all the input data.
*/
Reader.prototype._readInputBuffer = function(callback) {
	var self = this;

	var dataChunks = [];

	var inputStream = null;
	if (this._input instanceof Readable) {
		inputStream = this._input;
	} else {
		inputStream = fs.createReadStream(this._input);
	}

	inputStream.on('error', function(err) {
		return callback(err);
	});
	inputStream.on('end', function() {
		self._inputBuffer = Buffer.concat(dataChunks);
		return callback();
	});
	inputStream.on('data', function(data) {
		dataChunks.push(new Buffer(data));
	});
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
Reader.prototype._loadV1 = function(headerValues, callback) {
	var self = this;

	var compressedBuffer = this._inputBuffer.slice(common.HEADER_SIZE + headerValues.minuteIndexSize);

	zlib.gunzip(compressedBuffer, function(err, buffer) {
		if (err) return callback(err);

		self._ticksBuffer = buffer;

		self._tickSize = 13;
		self.length = self._ticksBuffer.length / self._tickSize;

		self._readTick = self._readTickV1AtOffset;

		self._loaded = true;

		callback();
	});
};

/*
Load new storage file (version 2)

Arguments are the same as for _loadV1
*/
Reader.prototype._loadV2 = function(headerValues, callback) {
	var self=this;

	var compressedBuffer = this._inputBuffer.slice(common.HEADER_SIZE);

	zlib.inflate(compressedBuffer, function(err, _ticksBuffer) {
		if (err) {
			return callback(err);
		}

		self._ticksBuffer = _ticksBuffer;
		self._tickSize = 31;
		self.length = self._ticksBuffer.length / self._tickSize;

		self._readTick = self._readTickV2AtOffset;

		self._loaded = true;

		callback();
	});
};

/*
Every storage file has a fixed-sized (common.HEADER_SIZE) header.
It contains storage version and can contain other metadata in future.
*/
Reader.prototype._loadHeader = function(callback) {
	var headerBuffer = this._inputBuffer.slice(0, common.HEADER_SIZE);
	var headerString = headerBuffer.toString().split("\n")[0];

	var json = null;
	try {
		json = JSON.parse(headerString);
	} catch(e) {
		return callback(e);
	}

	callback(null, json);
};

module.exports = Reader;
