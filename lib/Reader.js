var
	fs = require('fs'),
	zlib = require('zlib'),
	common = require('./common'),
	uncompress = require('compress-buffer').uncompress;

function Reader(path) {
	this._path = path;

	this.length = 0;

	this._ticksBuffer = null;
	this._position = 0;
	this._loaded = false;

	this._readTick = function() { throw new Error("Ticks not loaded properly."); };
}

Reader.prototype.load = function(callback) {
	var self=this;
	if (!fs.existsSync(this._path)) {
		return callback(new Error("File does not exists:" + this._path));
	}

	fs.open(this._path, "r", function(err, fd) {
		if (err) {
			return callback(err);
		}

		fs.fstat(fd, function(err, stats) {
			// if fs.open succeed, then fs.stats should not fail, but, you know...
			if (err) {
				return callback(err);
			}

			self._loadHeader(fd, function(err, headerValues) {
				if (err) {
					return callback(err);
				}

				if (headerValues.version == 1) {
					self._loadV1(fd, stats, headerValues, callback);
				} else {
					self._loadV2(fd, stats, headerValues, callback);
				}
			});
		});
	});
};

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
}

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

Reader.prototype._readTickV1AtOffset = function(offset) {
	return {
		unixtime: this._ticksBuffer.readUInt32LE(offset, false),
		volume: this._ticksBuffer.readUInt32LE(offset + 4, false),
		price: this._ticksBuffer.readUInt32LE(offset + 8, false),
		isMarket: this._ticksBuffer.readUInt8(offset + 12, false) == 1
	}
}

Reader.prototype._readTickV2AtOffset = function(offset) {
	return {
		unixtime: this._ticksBuffer.readUInt32LE(offset, false),
		msec: this._ticksBuffer.readUInt16LE(offset + 4, false),
		volume: this._ticksBuffer.readUInt32LE(offset + 6, false),
		price: this._ticksBuffer.readUInt32LE(offset + 10, false),
		bid: this._ticksBuffer.readUInt32LE(offset + 14, false),
		ask: this._ticksBuffer.readUInt32LE(offset + 18, false),
		bidSize: this._ticksBuffer.readUInt32LE(offset + 22, false),
		askSize: this._ticksBuffer.readUInt32LE(offset + 26, false),
		isMarket: this._ticksBuffer.readUInt8(offset + 30, false) == 1
	}
}

Reader.prototype._loadV1 = function(fd, stats, headerValues, callback) {
	var self=this;

	var compressedBuffer = new Buffer(stats.size - common.HEADER_SIZE - headerValues.minuteIndexSize);

	fs.read(fd, compressedBuffer, 0, compressedBuffer.length, common.HEADER_SIZE + headerValues.minuteIndexSize, function(err) {
		if (err) {
			return callback(err);
		}

		try { 
			self._ticksBuffer = uncompress(compressedBuffer);
		} catch (e) {
			return callback(e);
		}

		self._tickSize = 13;
		self.length = self._ticksBuffer.length / self._tickSize;

		self._readTick = self._readTickV1AtOffset;

		self._loaded = true;

		callback();
	});
}

Reader.prototype._loadV2 = function(fd, stats, headerValues, callback) {
	var self=this;

	var compressedBuffer = new Buffer(stats.size - common.HEADER_SIZE);

	fs.read(fd, compressedBuffer, 0, compressedBuffer.length, common.HEADER_SIZE, function(err) {
		if (err) {
			return callback(err);
		}

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
	});
}

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
