package exchangeserver.domain;

import java.util.regex.Pattern;

/*

Every rule that replay depends on, in one place.

Replay feeds old commands through today's code. If a rule here changes, an old valid command can be rejected
(or accepted) differently on replay, and the rebuilt book silently differs from the one clients saw (D20).
So: change a rule => bump VERSION. The journal stores the version it was written under and refuses to replay
under a different one (D46); the way across a rule change is a snapshot plus a fresh journal.

Limits are inclusive.

 */
public final class Rules {

    public static final int VERSION = 1;

    // Stateless (checked by OrderRequest, i.e. at the gateway).
    public static final long MAX_ORDER_QUANTITY = 1_000_000_000L;
    // Notional (price x quantity) can never overflow a long.
    public static final long MAX_PRICE = Long.MAX_VALUE / MAX_ORDER_QUANTITY;
    // No ',' or whitespace: they separate fields in the journal and in the wire protocol.
    public static final Pattern PARTICIPANT = Pattern.compile("[A-Za-z0-9_]{1,16}");
    public static final Pattern CLIENT_ORDER_ID = Pattern.compile("[A-Za-z0-9_.-]{1,32}");

    // State-dependent (checked by the engine).
    // A priced order must be within this many percent of the last trade price; a market order trades only
    // inside the same band (the collar). No band until the first trade.
    public static final int PRICE_BAND_PERCENT = 10;
    public static final int MAX_OPEN_ORDERS_PER_PARTICIPANT = 10_000;

    private Rules() {
    }

    // Exact integer comparison, no rounding: price * 100 vs reference * (100 +- band).
    // Cannot overflow: both prices are <= MAX_PRICE (~9.2e9), times at most 110.
    public static boolean withinBand(long price, long referencePrice) {
        return price * 100 <= referencePrice * (100 + PRICE_BAND_PERCENT)
                && price * 100 >= referencePrice * (100 - PRICE_BAND_PERCENT);
    }
}
