package exchangeserver.book;

import java.util.Objects;

// A race with a fill is a normal outcome, so UNKNOWN_ORDER is a return value, not an exception (D17).
public record CancelResult(CancelStatus status, long orderId, long cancelledQuantity) {

    public CancelResult {
        Objects.requireNonNull(status, "status");
        if (status == CancelStatus.CANCELLED && cancelledQuantity <= 0) {
            throw new IllegalArgumentException("A cancelled order had something left");
        }
        if (status == CancelStatus.UNKNOWN_ORDER && cancelledQuantity != 0) {
            throw new IllegalArgumentException("Nothing is cancelled for an unknown order");
        }
    }

    public static CancelResult cancelled(long orderId, long quantity) {
        return new CancelResult(CancelStatus.CANCELLED, orderId, quantity);
    }

    public static CancelResult unknown(long orderId) {
        return new CancelResult(CancelStatus.UNKNOWN_ORDER, orderId, 0);
    }
}
