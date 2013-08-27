var
    common = require('./common')
    compressBuffer = require('compress-buffer'),
    fs = require('fs');

function Writer(path) {
    this._path = path;
    this._position = 0;
    this._buffer = new Buffer(1024*1024*10);
}

/**

Add tick to storage.

@param {Tick} tick tick object with following fields:
    * unixtime - unix time (in milliseconds), Integer
    * volume - volume, Integer (stored as unsigned, 32 bit)
    * price - price, Integer (stored as unsigned, 32 bit)
    * bid - bid price, Integer (stored as unsigned, 32 bit)
    * ask - ask price, Integer (stored as unsigned, 32 bit)
    * bidSize - bid size, Integer (stored as unsigned, 32 bit)
    * askSize - askSize, Integer (stored as unsigned, 32 bit)
    * isMarket - true for market ticks, false for other ticks, Boolean

*/
var TICK_SIZE = 31;
Writer.prototype.addTick = function(tick) {
    var offset = this._position * TICK_SIZE;

    this._ensureBufferSize(offset + TICK_SIZE);

    this._buffer.writeUInt32LE(parseInt(tick.unixtime / 1000) || 0, offset, false);
    this._buffer.writeUInt16LE((tick.unixtime % 1000) || 0, offset + 4, false);
    this._buffer.writeUInt32LE(parseInt(tick.volume) || 0, offset + 6, false);
    this._buffer.writeUInt32LE(parseInt(tick.price) || 0, offset + 10, false);
    this._buffer.writeUInt32LE(parseInt(tick.bid) || 0, offset + 14, false);
    this._buffer.writeUInt32LE(parseInt(tick.ask) || 0, offset + 18, false);
    this._buffer.writeUInt32LE(parseInt(tick.bidSize) || 0, offset + 22, false);
    this._buffer.writeUInt32LE(parseInt(tick.askSize) || 0, offset + 26, false);
    this._buffer.writeUInt8(tick.isMarket ? 1 : 0, offset + 30, false);

    this._position++;
}

Writer.prototype.save = function() {
    var headerBuffer = this._generateHeader();
    var compressedBuffer = this._compressData();

    var resultBuffer = Buffer.concat([headerBuffer, compressedBuffer]);

    this._writeFile(resultBuffer);
}

Writer.prototype._ensureBufferSize = function(size) {
    if (this._buffer.length < size) {
        var newBuffer = new Buffer(this._buffer.length * 2);
        this._buffer.copy(newBuffer, 0, 0, this._buffer.length);
        this._buffer = newBuffer;
    }
}

Writer.prototype._generateHeader = function() {
    var headerData = {
        version: common.STORAGE_VERSION
    };
    var buffer = new Buffer(common.HEADER_SIZE);
    buffer.fill(0);
    buffer.write(JSON.stringify(headerData)+"\n", 0 , 'ascii');
    return buffer;
}

Writer.prototype._compressData = function() {
    var realDataLength = this._position * TICK_SIZE;
    return require('compress-buffer').compress(this._buffer.slice(0, realDataLength));
}

Writer.prototype._writeFile = function(buffer) {
    var dirPath = require('path').dirname(this._path);
    require('mkdirp').sync(dirPath);

    var tmpPath = this._path + ".tmp";
    var fd = fs.openSync(tmpPath, "w");

    fs.writeSync(fd, buffer, 0, buffer.length, 0);

    fs.closeSync(fd);

    if (fs.existsSync(this._path) ) {
        fs.unlinkSync(this._path)
    }

    fs.renameSync(tmpPath, this._path);
}

module.exports = Writer;
