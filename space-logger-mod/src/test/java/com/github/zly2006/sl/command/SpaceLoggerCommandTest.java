package com.github.zly2006.sl.command;

import com.mojang.brigadier.exceptions.CommandSyntaxException;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SpaceLoggerCommandTest {
    @Test
    void parseFiltersSupportsPageAndBuildsCommands() throws CommandSyntaxException {
        SpaceLoggerCommand.ParsedFilters filters = SpaceLoggerCommand.parseFilters(
            "subject:alice limit:10 page:3 verb:break",
            null
        );

        assertEquals("alice", filters.subject);
        assertEquals(10, filters.limit);
        assertEquals(3, filters.page);
        assertEquals("/sl q subject:alice limit:10 verb:break page:2", filters.commandForPage(2));
        assertEquals("/sl q subject:alice limit:10 verb:break", filters.commandForPage(1));
    }

    @Test
    void resolvePaginationCalculatesOffsetAndFetchLimit() throws CommandSyntaxException {
        SpaceLoggerCommand.QueryPagination pagination = SpaceLoggerCommand.resolvePagination(3, 5);

        assertEquals(10, pagination.offset);
        assertEquals(16, pagination.fetchLimit);
    }

    @Test
    void parseFiltersRejectsPageZero() {
        assertThrows(
            CommandSyntaxException.class,
            () -> SpaceLoggerCommand.parseFilters("page:0", null)
        );
    }

    @Test
    void completeFilterTokenSuggestsKeysAndVerbValues() {
        assertIterableEquals(
            List.of("page:"),
            SpaceLoggerCommand.completeFilterToken("pa", "Alice")
        );
        assertIterableEquals(
            List.of("verb:place"),
            SpaceLoggerCommand.completeFilterToken("verb:pl", "Alice")
        );
        assertIterableEquals(
            List.of("verb:break,place"),
            SpaceLoggerCommand.completeFilterToken("verb:break,pl", "Alice")
        );
    }

    @Test
    void completeFilterTokenSuggestsSubjectUsingPlayerName() {
        assertIterableEquals(
            List.of("subject:Alice"),
            SpaceLoggerCommand.completeFilterToken("subject:A", "Alice")
        );
    }
}
