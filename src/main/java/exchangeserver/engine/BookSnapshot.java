package exchangeserver.engine;

import exchangeserver.book.LevelUpdate;

import java.util.List;

/*

The book as of a command: the state a market-data subscriber starts from before applying level updates.
sequenceNumber is the last command the engine processed (0 for an empty history); lastTradePrice 0 = none yet.
Levels are best first.

 */
public record BookSnapshot(long sequenceNumber, long lastTradePrice, List<LevelUpdate> bids, List<LevelUpdate> asks) {

    public BookSnapshot {
        bids = List.copyOf(bids);
        asks = List.copyOf(asks);
    }
}
