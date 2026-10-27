package exchangeserver.domain;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RulesTest {

    @Test
    void bandIsInclusiveAtBothEdges() {
        // reference 100, band 10%: 90..110
        assertTrue(Rules.withinBand(110, 100));
        assertTrue(Rules.withinBand(90, 100));
        assertTrue(Rules.withinBand(100, 100));
        assertFalse(Rules.withinBand(111, 100));
        assertFalse(Rules.withinBand(89, 100));
    }

    @Test
    void bandComparesExactlyWithoutRounding() {
        // reference 101: 90.9 .. 111.1 -> 91..111
        assertTrue(Rules.withinBand(111, 101));
        assertFalse(Rules.withinBand(112, 101));
        assertTrue(Rules.withinBand(91, 101));
        assertFalse(Rules.withinBand(90, 101));
    }

    @Test
    void bandDoesNotOverflowAtTheMaximumPrice() {
        assertTrue(Rules.withinBand(Rules.MAX_PRICE, Rules.MAX_PRICE));
        assertFalse(Rules.withinBand(Rules.MAX_PRICE, 1));
    }

    @Test
    void maxPriceTimesMaxQuantityFitsInALong() {
        assertTrue(Math.multiplyHigh(Rules.MAX_PRICE, Rules.MAX_ORDER_QUANTITY) == 0
                && Rules.MAX_PRICE * Rules.MAX_ORDER_QUANTITY > 0);
    }

    @Test
    void oppositeSide() {
        assertEquals(Side.SELL, Side.BUY.opposite());
        assertEquals(Side.BUY, Side.SELL.opposite());
    }

    @Test
    void onlyLimitAndPostOnlyRest() {
        for (OrderType type : OrderType.values()) {
            assertEquals(type == OrderType.LIMIT || type == OrderType.POST_ONLY, type.canRest(), type.name());
        }
    }
}
