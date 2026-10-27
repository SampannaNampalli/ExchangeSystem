package exchangeserver.gateway;

import exchangeserver.book.CancelResult;
import exchangeserver.book.CancelStatus;
import exchangeserver.book.LevelUpdate;
import exchangeserver.core.PrivateEvent;
import exchangeserver.domain.OrderType;
import exchangeserver.domain.Side;
import exchangeserver.engine.BookSnapshot;
import exchangeserver.engine.Execution;
import exchangeserver.engine.Response;
import exchangeserver.engine.Trade;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/*

The wire protocol: ASCII lines ending in '\n' ("\r\n" accepted), tokens separated by single spaces (D53).

Client -> server
    LOGIN <participant> <secret> [COD]          COD: cancel my resting orders when my last session closes (D72)
    NEW <clOrdId> <BUY|SELL> <LIMIT|IOC|FOK|POST_ONLY> <quantity> <price>
    NEW <clOrdId> <BUY|SELL> MARKET <quantity>
    CANCEL <orderId>
    RESEND <oseq>                               private events from oseq on, as far as still retained (D73)
    SUBSCRIBE | UNSUBSCRIBE | PING | QUIT

Server -> client (key=value after the message type, so fields can be added without breaking parsers)
  Private stream, numbered per participant (oseq), sent to every session of that participant (D73):
    FILL oseq= orderId= clOrdId= side= price= qty= leaves= liquidity=TAKER|MAKER   (one per trade, both sides)
    ORDER oseq= orderId= clOrdId= status= filled= remaining= [reason=]           (the outcome of a NEW)
    CANCELLED oseq= orderId= qty= [reason=CANCEL_ALL]   CANCEL_REJECT oseq= orderId= reason=UNKNOWN_ORDER
    (a line repeated by RESEND ends with resend=1)
  Session:
    WELCOME participant=<p> oseq=<latest>       RESENT last=<latest>      ERROR RESEND_GAP first=<oldest retained>
    BOOK seq= last= bids=<price>x<qty>,... asks=...      (best SNAPSHOT_DEPTH levels; "-" for an empty side)
    TRADE seq= price= qty= aggressor=          LEVEL seq= side= price= qty= orders=   (qty=0: level gone)
    SUBSCRIBED  UNSUBSCRIBED  PONG  BYE
    ERROR <text>

For a NEW, the submitter gets its FILL lines, then ORDER; makers' fills and market data for the same command
follow. QUIT: every request sent before it is answered first, then BYE, then the server closes the connection.

 */
final class Protocol {

    // Not counting the line terminator ("\n" or "\r\n").
    static final int MAX_LINE_LENGTH = 256;
    // Levels per side in the BOOK snapshot. Bounded so a SUBSCRIBE costs the writer thread a fixed amount of
    // work however deep the book is (D62). LEVEL updates carry absolute quantities, so a deeper level appears in a
    // subscriber's view the first time it changes.
    static final int SNAPSHOT_DEPTH = 100;
    static final String CANCEL_ON_DISCONNECT = "COD";

    private Protocol() {
    }

    static ClientMessage parse(String line) {
        if (line.isEmpty()) {
            throw new ProtocolException("empty line");
        }
        String[] t = line.split(" ", -1);
        return switch (t[0]) {
            case "LOGIN" -> {
                if (t.length == 4 && t[3].equals(CANCEL_ON_DISCONNECT)) {
                    yield new ClientMessage.Login(t[1], t[2], true);
                }
                arity(t, 3);
                yield new ClientMessage.Login(t[1], t[2], false);
            }
            case "NEW" -> parseNew(t);
            case "CANCEL" -> {
                arity(t, 2);
                yield new ClientMessage.Cancel(number(t[1], "order id"));
            }
            case "RESEND" -> {
                arity(t, 2);
                yield new ClientMessage.Resend(number(t[1], "oseq"));
            }
            case "SUBSCRIBE" -> {
                arity(t, 1);
                yield new ClientMessage.Subscribe();
            }
            case "UNSUBSCRIBE" -> {
                arity(t, 1);
                yield new ClientMessage.Unsubscribe();
            }
            case "PING" -> {
                arity(t, 1);
                yield new ClientMessage.Ping();
            }
            case "QUIT" -> {
                arity(t, 1);
                yield new ClientMessage.Quit();
            }
            default -> throw new ProtocolException("unknown message " + t[0]);
        };
    }

    private static ClientMessage parseNew(String[] t) {
        if (t.length < 5) {
            throw new ProtocolException("NEW needs at least 4 fields");
        }
        Side side = enumValue(Side.class, t[2], "side");
        OrderType type = enumValue(OrderType.class, t[3], "order type");
        if (type == OrderType.MARKET) {
            arity(t, 5);
            return new ClientMessage.NewOrder(t[1], side, type, number(t[4], "quantity"), OptionalLong.empty());
        }
        arity(t, 6);
        return new ClientMessage.NewOrder(t[1], side, type, number(t[4], "quantity"), OptionalLong.of(number(t[5], "price")));
    }

    private static void arity(String[] t, int expected) {
        if (t.length != expected) {
            throw new ProtocolException(t[0] + " takes " + (expected - 1) + " field(s), got " + (t.length - 1));
        }
    }

    private static long number(String s, String what) {
        try {
            return Long.parseLong(s);
        } catch (NumberFormatException e) {
            throw new ProtocolException("bad " + what + ": " + s);
        }
    }

    private static <E extends Enum<E>> E enumValue(Class<E> type, String s, String what) {
        try {
            return Enum.valueOf(type, s);
        } catch (IllegalArgumentException e) {
            throw new ProtocolException("bad " + what + ": " + s);
        }
    }

    // ---------- Formatting ----------

    static String welcome(String participant, long lastOseq) {
        return "WELCOME participant=" + participant + " oseq=" + lastOseq;
    }

    // The lines of one private event; resent ones are marked.
    static List<String> lines(PrivateEvent event, boolean resent) {
        List<String> lines = new ArrayList<>(2);
        String o = "oseq=" + event.oseq() + " ";
        if (event instanceof PrivateEvent.OrderReply r) {
            Response response = r.response();
            long leaves = r.quantity();
            for (Trade t : response.trades()) {
                leaves -= t.quantity();
                lines.add(fill(o, response.orderId(), response.clientOrderId(), r.side(), t.price(), t.quantity(), leaves, "TAKER"));
            }
            lines.add(order(o, response));
        } else if (event instanceof PrivateEvent.CancelReply c) {
            lines.add(cancel(o, c.result(), c.byCancelAll()));
        } else if (event instanceof PrivateEvent.MakerFill m) {
            Execution e = m.execution();
            lines.add(fill(o, e.orderId(), e.clientOrderId(), e.side(), e.price(), e.quantity(), e.leavesQuantity(), "MAKER"));
        }
        if (resent) {
            lines.replaceAll(line -> line + " resend=1");
        }
        return lines;
    }

    private static String fill(String o, long orderId, String clientOrderId, Side side, long price, long qty, long leaves,
                               String liquidity) {
        return "FILL " + o + "orderId=" + orderId + " clOrdId=" + clientOrderId + " side=" + side + " price=" + price
                + " qty=" + qty + " leaves=" + leaves + " liquidity=" + liquidity;
    }

    private static String order(String o, Response r) {
        return "ORDER " + o + "orderId=" + r.orderId() + " clOrdId=" + r.clientOrderId() + " status=" + r.status()
                + " filled=" + r.filledQuantity() + " remaining=" + r.remainingQuantity()
                + (r.reason() == null ? "" : " reason=" + r.reason());
    }

    private static String cancel(String o, CancelResult r, boolean byCancelAll) {
        return r.status() == CancelStatus.CANCELLED
                ? "CANCELLED " + o + "orderId=" + r.orderId() + " qty=" + r.cancelledQuantity() + (byCancelAll ? " reason=CANCEL_ALL" : "")
                : "CANCEL_REJECT " + o + "orderId=" + r.orderId() + " reason=" + r.status();
    }

    static String book(BookSnapshot s) {
        return "BOOK seq=" + s.sequenceNumber() + " last=" + s.lastTradePrice()
                + " bids=" + levels(s.bids()) + " asks=" + levels(s.asks());
    }

    private static String levels(List<LevelUpdate> levels) {
        if (levels.isEmpty()) {
            return "-";
        }
        StringBuilder sb = new StringBuilder();
        for (LevelUpdate l : levels) {
            if (sb.length() > 0) {
                sb.append(',');
            }
            sb.append(l.price()).append('x').append(l.volume());
        }
        return sb.toString();
    }

    static String trade(long seq, Trade t) {
        return "TRADE seq=" + seq + " price=" + t.price() + " qty=" + t.quantity() + " aggressor=" + t.aggressorSide();
    }

    static String level(long seq, LevelUpdate l) {
        return "LEVEL seq=" + seq + " side=" + l.side() + " price=" + l.price() + " qty=" + l.volume() + " orders=" + l.orderCount();
    }

    static String error(String message) {
        // One line, whatever the message contains.
        return "ERROR " + message.replace('\n', ' ').replace('\r', ' ');
    }
}
