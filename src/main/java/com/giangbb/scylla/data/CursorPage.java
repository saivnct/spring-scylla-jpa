package com.giangbb.scylla.data;

import java.util.List;

/**
 * Created by giangbb on 05/09/2026
 *
 * One keyset page: the items, and where to resume.
 *
 * {@code nextCursor} is <b>opaque to callers</b> - an encoded string a client hands back untouched,
 * never parsed, constructed or interpreted. A null cursor means the walk is finished; a page shorter
 * than the requested size <i>with</i> a cursor means the walk stopped early (it hit the per-request
 * fetch ceiling while filtering) and there is more to come. That distinction is the whole point of
 * the shape: a short page is a visible continuation, never a silent truncation.
 *
 * There is no total. Counting means walking the org's whole view partition, so it is its own call.
 */
public class CursorPage<T> {

    private final List<T> items;

    private final String nextCursor;

    public CursorPage(List<T> items, String nextCursor) {
        this.items = items;
        this.nextCursor = nextCursor;
    }

    public static <T> CursorPage<T> of(List<T> items, String nextCursor) {
        return new CursorPage<>(List.copyOf(items), nextCursor);
    }

    public List<T> getItems() {
        return items;
    }

    public String getNextCursor() {
        return nextCursor;
    }


    public boolean hasNext() {
        return nextCursor != null;
    }
}
