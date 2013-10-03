/*
Tick storage writer
===================
*/
var
	common = require('./common'),
	fs = require('fs'),
	zlib = require('zlib'),
	Writable = require('stream').Writable,
	mkdirp = require('mkdirp');

/*
Initialize writer.

@param {String or Writable} writable stream or path to tick database file. If it's path then all necessary directories will be created automatically (if possible). If there is file already it will be rewritten.
*/
function Writer(output) {
	this._output = output;
	this._position = 0;
	this._buffer = new Buffer(1024*1024*10);
}

/**

Add tick to storage.

@param {Tick} tick tick object with following fields:
	- unixtime - unixtime in milliseconds, Integer
	- volume - volume, Integer (stored as unsigned, 32 bit)
	- price - price, Integer (stored as unsigned, 32 bit)
	- bid - bid price, Integer (stored as unsigned, 32 bit)
	- ask - ask price, Integer (stored as unsigned, 32 bit)
	- bidSize - bid size, Integer (stored as unsigned, 32 bit)
	- askSize - askSize, Integer (stored as unsigned, 32 bit)
	- isMarket - true for market ticks, false for other ticks, Boolean

Ticks are written to `_buffer`, which is resized when filled up.
*/
var TICK_SIZE = 31;
Writer.prototype.addTick = function(tick) {
	if (tick.volume < 0 || tick.price < 0) {
		// Ignore ticks with non-positive and non-zero volume or price
		return;
	}
	var offset = this._position * TICK_SIZE;

	this._ensureBufferSize(offset + TICK_SIZE);

	this._buffer.writeUInt32LE(parseInt(tick.unixtime/1000, 10) || 0, offset, false);
	this._buffer.writeUInt16LE((tick.unixtime%1000) || 0, offset + 4, false);
	this._buffer.writeUInt32LE(parseInt(tick.volume, 10) || 0, offset + 6, false);
	this._buffer.writeUInt32LE(parseInt(tick.price, 10) || 0, offset + 10, false);
	this._buffer.writeUInt32LE(parseInt(tick.bid, 10) || 0, offset + 14, false);
	this._buffer.writeUInt32LE(parseInt(tick.ask, 10) || 0, offset + 18, false);
	this._buffer.writeUInt32LE(parseInt(tick.bidSize, 10) || 0, offset + 22, false);
	this._buffer.writeUInt32LE(parseInt(tick.askSize, 10) || 0, offset + 26, false);
	this._buffer.writeUInt8(tick.isMarket ? 1 : 0, offset + 30, false);

	this._position++;
};

/*
Store added ticks to file.

Creates necessary directories if needed.
Overwrites existing file.
*/
Writer.prototype.save = function(callback) {
	var _this=this;

	var headerBuffer = this._generateHeader();
	this._compressData(function(err, compressedBuffer) {
		if (err) return callback(err);

		var resultBuffer = Buffer.concat([headerBuffer, compressedBuffer]);

		_this._writeOutput(resultBuffer, callback);
	});
};

/*
Check if `_buffer` is large enough to write one more tick.
If not - create new Buffer twice the size and replace `_buffer` with it.
*/
Writer.prototype._ensureBufferSize = function(size) {
	if (this._buffer.length < size) {
		var newBuffer = new Buffer(this._buffer.length * 2);
		this._buffer.copy(newBuffer, 0, 0, this._buffer.length);
		this._buffer = newBuffer;
	}
};

/*
Prepare buffer with header data to be written into tick database file before actual ticks.
Saves only storage version for now.
*/
Writer.prototype._generateHeader = function() {
	var headerData = {
		version: common.STORAGE_VERSION
	};
	var buffer = new Buffer(common.HEADER_SIZE);
	buffer.fill(0);
	buffer.write(JSON.stringify(headerData)+"\n", 0 , 'ascii');
	return buffer;
};

/*
Compress tick data to occupy even less space.
*/
Writer.prototype._compressData = function(callback) {
	var realDataLength = this._position * TICK_SIZE;
	zlib.deflate(this._buffer.slice(0, realDataLength), callback);
};

/*
Write result buffer to the output
*/
Writer.prototype._writeOutput = function(buffer, callback) {
	this._getOutputStream(function(err, outputStream) {
		if (err) return callback(err);

		outputStream.on('error', callback);

		outputStream.write(buffer);

		outputStream.end(null, null, callback);
	});
};

/*
Creates output stream if `_output` is a file path, returns `_output` if it's already a stream.
*/
Writer.prototype._getOutputStream = function(callback) {
	var self = this;

	if (this._output.write) {
		return callback(null, this._output);
	} else {
		var dirPath = require('path').dirname(self._path);
		mkdirp(dirPath, function(err) {
			if (err) return callback(err);

			var stream = fs.createWriteStream(self._output, "w");

			return callback(null, stream);
		});
	}
};

module.exports = Writer;
