var
    chai = require('chai'),
    assert = chai.assert,
    Reader = require('../index.js').Reader;

describe("TickStorage/Reader", function() {
    it("should read storage version 1 correctly", function() {
        return
        var reader = new Reader(__dirname + '/data/ticks-v1/LVS/20110104.ticks');
        reader.load();

        var tick;
        var ticksAmount = 0,
            totalVolume = 0,
            totalPrice = 0,
            totalMarketTicks = 0;

        while(tick = reader.nextTick()) {
            totalVolume += tick.volume;
            totalPrice += tick.price;
            ticksAmount++;
            totalMarketTicks += tick.isMarket ? 1 : 0;
        }

        assert.equal(totalVolume, 39222254, 'total volume');
        assert.equal(totalPrice, 55302035684, 'total price');
        assert.equal(ticksAmount, 118003, 'total count');
        assert.equal(totalMarketTicks, 116927, 'total market count');
    });

    it("should read storage correctly", function() {
        var path = __dirname + '/data/ticks/LVS/20110104.ticks';
        var reader = new Reader(path);
        reader.load();

        var tick;
        var ticksAmount = 0,
            totalVolume = 0,
            totalPrice = 0,
            totalBid = 0,
            totalAsk = 0,
            totalBidSize = 0,
            totalAskSize = 0,
            totalMarketTicks = 0;

        while(tick = reader.nextTick()) {
            totalVolume += tick.volume;
            totalPrice += tick.price;
            totalBid += tick.bid;
            totalAsk += tick.ask;
            totalBidSize += tick.bidSize;
            totalAskSize += tick.askSize;
            totalMarketTicks += tick.isMarket ? 1 : 0;
            ticksAmount++;
        }

        assert.equal(totalVolume, 39222254, 'total volume');
        assert.equal(totalPrice, 55302035684, 'total price');
        assert.equal(ticksAmount, 118003, 'total count');
        assert.equal(totalBid, 590390483);
        assert.equal(totalAsk, 589294627);
        assert.equal(totalBidSize, 591026248);
        assert.equal(totalAskSize, 590516449);
        assert.equal(totalMarketTicks, 116927);
    });

    it("should read first ticks correctly", function() {
        var path = __dirname + '/data/ticks/LVS/20110104.ticks';
        var reader = new Reader(path);
        reader.load();

        assert.deepEqual(reader.nextTick(), {
            "unixtime": 1294134747000,
            "volume": 100,
            "price": 465000,
            "bid": 8210,
            "ask": 7971,
            "bidSize": 1659,
            "askSize": 7291,
            "isMarket": false
        });

        assert.deepEqual(reader.nextTick(), {
            "unixtime": 1294143456000,
            "volume": 100,
            "price": 458300,
            "bid": 1119,
            "ask": 1821,
            "bidSize": 1294,
            "askSize": 9690,
            "isMarket": false
        });

        assert.deepEqual(reader.nextTick(), {
            "unixtime": 1294143481000,
            "volume": 100,
            "price": 458300,
            "bid": 5046,
            "ask": 7206,
            "bidSize": 3427,
            "askSize": 3602,
            "isMarket": false
        });
    });

    it("should throw error if not loaded", function() {
        var path = __dirname + '/data/ticks/non-existant.ticks';
        var reader = new Reader(path);

        try {
            reader.nextTick();
        } catch(e) {
            return;
        }
        assert.fail("Should throw error");
    });

    it("should throw error if file doesn't exist", function() {
        var path = __dirname + '/data/ticks/non-existant.ticks';
        var reader = new Reader(path);

        try {
            reader.load();
        } catch(e) {
            return;
        }
        assert.fail("Should throw error");
    });

    it("should return error on invalid file format", function() {
        var path = __dirname + '/data/ticks/invalid.ticks';
        var reader = new Reader(path);

        try {
            reader.load();
        } catch(e) {
            return;
        }
        assert.fail("Should throw error");
    });
});
