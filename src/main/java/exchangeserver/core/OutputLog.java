package exchangeserver.core;

import exchangeserver.book.CancelResult;
import exchangeserver.command.CancelAllCommand;
import exchangeserver.command.CancelOrderCommand;
import exchangeserver.command.Command;
import exchangeserver.command.NewOrderCommand;
import exchangeserver.engine.CancelAllOutput;
import exchangeserver.engine.CancelOutput;
import exchangeserver.engine.EngineOutput;
import exchangeserver.engine.Execution;
import exchangeserver.engine.OrderOutput;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/*

Numbers each participant's private events and keeps the most recent `retention` of them for RESEND (D73).

Deterministic: the events and their numbers follow from the engine outputs alone, so replay rebuilds the same
stream (the map is only looked up; the snapshot export sorts it). Memory: `retention` events per participant.
A snapshot keeps each participant's last number but not the events, so a RESEND reaches back at most to the
last compaction.

 */
final class OutputLog {


    private static final class Stream {
        long lastOseq;
        final ArrayDeque<PrivateEvent> recent = new ArrayDeque<>();
    }

    private final int retention;
    private final Map<String, Stream> streams = new HashMap<>();

    OutputLog(int retention) {
        if (retention <= 0) {
            throw new IllegalArgumentException("Retention must be positive");
        }
        this.retention = retention;
    }

    // Continues after a snapshot: numbering resumes, nothing is retained.
    static OutputLog resume(int retention, Map<String, Long> lastOseqs) {
        OutputLog log = new OutputLog(retention);
        lastOseqs.forEach((p, n) -> log.stream(p).lastOseq = n);
        return log;
    }

    // The private events of one command's output, numbered, in the order a client must see them: the requester's
    // reply first, then the makers' fills in match order.
    List<PrivateEvent> record(Command command, EngineOutput output) {
        List<PrivateEvent> events = new ArrayList<>();
        if (output instanceof OrderOutput o && command instanceof NewOrderCommand n) {
            String p = n.request().getParticipant();
            events.add(add(new PrivateEvent.OrderReply(p, next(p), n.request().getSide(), n.request().getQuantity(), o.response())));
            for (Execution e : o.passiveFills()) {
                events.add(add(new PrivateEvent.MakerFill(e.participant(), next(e.participant()), e)));
            }
        } else if (output instanceof CancelOutput c && command instanceof CancelOrderCommand cc) {
            events.add(add(new PrivateEvent.CancelReply(cc.participant(), next(cc.participant()), c.result(), false)));
        } else if (output instanceof CancelAllOutput a && command instanceof CancelAllCommand) {
            for (CancelResult r : a.results()) {
                events.add(add(new PrivateEvent.CancelReply(a.participant(), next(a.participant()), r, true)));
            }
        } else {
            throw new IllegalStateException("Output " + output.getClass().getSimpleName() + " does not match " + command);
        }
        return events;
    }

    long lastOseq(String participant) {
        Stream s = streams.get(participant);
        return s == null ? 0 : s.lastOseq;
    }

    // Events with oseq >= from that are still retained, plus where the retained range starts.
    ResendResult since(String participant, long from) {
        Stream s = streams.get(participant);
        if (s == null) {
            return new ResendResult(List.of(), 1, 0);
        }
        long firstAvailable = s.recent.isEmpty() ? s.lastOseq + 1 : s.recent.peekFirst().oseq();
        List<PrivateEvent> events = new ArrayList<>();
        for (PrivateEvent e : s.recent) {
            if (e.oseq() >= from) {
                events.add(e);
            }
        }
        return new ResendResult(events, firstAvailable, s.lastOseq);
    }

    // Sorted, so a snapshot file is the same bytes whatever the map's iteration order.
    Map<String, Long> lastOseqs() {
        Map<String, Long> result = new TreeMap<>();
        streams.forEach((p, s) -> result.put(p, s.lastOseq));
        return result;
    }

    private long next(String participant) {
        return stream(participant).lastOseq + 1;
    }

    private PrivateEvent add(PrivateEvent event) {
        Stream s = stream(event.participant());
        s.lastOseq = event.oseq();
        s.recent.addLast(event);
        if (s.recent.size() > retention) {
            s.recent.removeFirst();
        }
        return event;
    }

    private Stream stream(String participant) {
        return streams.computeIfAbsent(participant, p -> new Stream());
    }
}
