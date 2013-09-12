var
	fs = require('fs'),
	common = require('./common');

function Reader(path) {
	this._path = path;

	this.length = 0;

	this._ticksBuffer = null;
	this._position = 0;
	this._loaded = false;
}

Reader.prototype.load = function() {
	if( !fs.existsSync(this._path) ) {
		throw new Error("File does not exists:" + this._path);
	}

	var fd = fs.openSync(this._path, "r");

	var stats = fs.fstatSync(fd);

	var headerValues = this._loadHeader(fd);

	if (headerValues.version == 1) {
		this._loadV1(fd, stats, headerValues);
	} else {
		this._loadV2(fd, stats, headerValues);
	}
	this._loaded = true;
};

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
}

Reader.prototype._readTickV1 = function(offset) {
	return {
		unixtime: this._ticksBuffer.readUInt32LE(offset, false),
		volume: this._ticksBuffer.readUInt32LE(offset + 4, false),
		price: this._ticksBuffer.readUInt32LE(offset + 8, false),
		isMarket: this._ticksBuffer.readUInt8(offset + 12, false) == 1
	}
}

Reader.prototype._readTickV2 = function(offset) {
	return {
		unixtime: this._ticksBuffer.readUInt32LE(offset, false) * 1000 + this._ticksBuffer.readUInt16LE(offset + 4, false),
		volume: this._ticksBuffer.readUInt32LE(offset + 6, false),
		price: this._ticksBuffer.readUInt32LE(offset + 10, false),
		bid: this._ticksBuffer.readUInt32LE(offset + 14, false),
		ask: this._ticksBuffer.readUInt32LE(offset + 18, false),
		bidSize: this._ticksBuffer.readUInt32LE(offset + 22, false),
		askSize: this._ticksBuffer.readUInt32LE(offset + 26, false),
		isMarket: this._ticksBuffer.readUInt8(offset + 30, false) == 1
	}
}

Reader.prototype._loadV1 = function(fd, stats, headerValues) {
	var compressedBuffer = new Buffer(stats.size - common.HEADER_SIZE - headerValues.minuteIndexSize);

	fs.readSync(fd, compressedBuffer, 0, compressedBuffer.length, common.HEADER_SIZE + headerValues.minuteIndexSize);

	this._ticksBuffer = require('compress-buffer').uncompress(compressedBuffer);
	this._tickSize = 13;
	this.length = this._ticksBuffer.length / this._tickSize;
	this._readTick = this._readTickV1;
}

Reader.prototype._loadV2 = function(fd, stats, headerValues) {
	var compressedBuffer = new Buffer(stats.size - common.HEADER_SIZE);

	fs.readSync(fd, compressedBuffer, 0, compressedBuffer.length, common.HEADER_SIZE);

	this._ticksBuffer = require('compress-buffer').uncompress(compressedBuffer);
	this._tickSize = 31;
	this.length = this._ticksBuffer.length / this._tickSize;
	this._readTick = this._readTickV2;
}

Reader.prototype._loadHeader = function(fd) {
	var headerBuffer = new Buffer(common.HEADER_SIZE);

	fs.readSync(fd, headerBuffer, 0, common.HEADER_SIZE, 0);

	var headerString = headerBuffer.toString().split("\n")[0];

	return JSON.parse(headerString);
};


module.exports = Reader;
