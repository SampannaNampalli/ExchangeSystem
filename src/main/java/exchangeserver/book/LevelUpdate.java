package exchangeserver.book;

import exchangeserver.domain.Side;

/*

The state of one price level after a command: level-2 market data. volume == 0 means the level is gone.

 */
public record LevelUpdate(Side side, long price, long volume, int orderCount) {
}
