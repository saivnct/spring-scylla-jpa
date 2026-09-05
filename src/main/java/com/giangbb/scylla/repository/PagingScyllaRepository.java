package com.giangbb.scylla.repository;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.paging.OffsetPager;
import com.giangbb.scylla.core.ScyllaTemplate;
import org.springframework.data.domain.*;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Created by Giangbb on 15/07/2025
 */
public class PagingScyllaRepository<T> extends SimpleScyllaRepository<T> {
    private static final CqlIdentifier APPLIED = CqlIdentifier.fromInternal("[applied]");


    public PagingScyllaRepository(Class<T> tClass, ScyllaTemplate scyllaTemplate) {
        super(tClass, scyllaTemplate);
    }

    /**
     * The offset-paging counterpart of {@code PagingScyllaRepository.findSliceWithPageAndOffset}, over
     * a caller-supplied row mapper.
     *
     * <b>A page past the end of the result set comes back empty</b>, which is not what
     * {@link OffsetPager} does on its own. Its documented contract is "the requested page, <i>or the
     * last page if the requested page was past the end of the iterable</i>", and it reports which one
     * it actually returned in {@link OffsetPager.Page#getPageNumber()}. Ignoring that and building the
     * {@code Slice} from the requested {@code Pageable} labels the last page's rows with a page number
     * that does not exist - a caller that jumps past the end silently gets real data, and a "fetch the
     * next page until one comes back empty" loop never terminates because the last page repeats
     * forever. So the number the pager actually served is compared against the one asked for, and a
     * clamped result is returned as the empty page it should have been.
     *
     * @param boundStatement Query statement
     * @param pageable Pageable
     * @param mapper Function<Row, R>
     * @return the requested page, or an empty page when it lies past the end of the result set
     */
    protected <R> Slice<R> findSliceWithPageAndOffset(BoundStatement boundStatement, Pageable pageable, Function<Row, R> mapper) {
        //align the server page size with the logical page size
        BoundStatement stmt = boundStatement.setPageSize(pageable.getPageSize());

        ResultSet rs = this.execute(stmt);

        OffsetPager pager = new OffsetPager(pageable.getPageSize());
        //OffsetPager numbers pages from 1, Pageable from 0
        int targetPageNumber = pageable.getPageNumber() + 1;
        OffsetPager.Page<Row> pageRow = pager.getPage(rs, targetPageNumber);

        //the pager clamped to the last page rather than serving the one asked for
        if (pageRow.getPageNumber() != targetPageNumber) {
            return new SliceImpl<>(new ArrayList<>(), pageable, false);
        }

        List<R> content = new ArrayList<>(pageRow.getElements().size());
        for (Row row : pageRow.getElements()) {
            if (row != null && (row.getColumnDefinitions().size() != 1 || !row.getColumnDefinitions().get(0).getName().equals(APPLIED))){
                R mapped = mapper.apply(row);
                if (mapped != null) {
                    content.add(mapped);
                }
            }
        }

        return new SliceImpl<>(content, pageable, !pageRow.isLast());
    }


    /**
     * Paging query over the entity's own row mapper, which needs every entity column present in the
     * row - so this form is for views selecting {@code .all()}. A view selecting a subset of columns
     * uses the three-argument form with its own mapper.
     *
     * @param boundStatement Query statement
     * @param pageable Pageable
     * @return the requested page, or an empty page when it lies past the end of the result set
     */
    protected Slice<T> findSliceWithPageAndOffset(BoundStatement boundStatement, Pageable pageable) {
        return findSliceWithPageAndOffset(boundStatement, pageable, this.getSingleRowMapper());
    }
}
