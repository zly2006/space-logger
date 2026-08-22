package me.zly2006.sl.command

import com.mojang.brigadier.exceptions.CommandSyntaxException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertIterableEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class SpaceLoggerCommandTest {
    @Test
    fun parseFiltersSupportsPageAndBuildsCommands() {
        val filters = SpaceLoggerCommand.parseFilters(
            "subject:alice limit:10 page:3 verb:break",
            null,
        )

        assertEquals("alice", filters.subject)
        assertEquals(10, filters.limit)
        assertEquals(3, filters.page)
        assertEquals("/sl q subject:alice limit:10 verb:break page:2", filters.commandForPage(2))
        assertEquals("/sl q subject:alice limit:10 verb:break", filters.commandForPage(1))
    }

    @Test
    fun resolvePaginationCalculatesOffsetAndFetchLimit() {
        val pagination = SpaceLoggerCommand.resolvePagination(3, 5)
        assertEquals(10, pagination.offset)
        assertEquals(16, pagination.fetchLimit)
    }

    @Test
    fun parseFiltersRejectsPageZero() {
        assertThrows(CommandSyntaxException::class.java) {
            SpaceLoggerCommand.parseFilters("page:0", null)
        }
    }

    @Test
    fun completeFilterTokenSuggestsKeysAndVerbValues() {
        assertIterableEquals(listOf("page:"), SpaceLoggerCommand.completeFilterToken("pa", "Alice"))
        assertIterableEquals(listOf("verb:place"), SpaceLoggerCommand.completeFilterToken("verb:pl", "Alice"))
        assertIterableEquals(listOf("verb:break,place"), SpaceLoggerCommand.completeFilterToken("verb:break,pl", "Alice"))
        assertIterableEquals(listOf("verb:command"), SpaceLoggerCommand.completeFilterToken("verb:co", "Alice"))
    }

    @Test
    fun completeFilterTokenSuggestsSubjectUsingPlayerName() {
        assertIterableEquals(listOf("subject:Alice"), SpaceLoggerCommand.completeFilterToken("subject:A", "Alice"))
    }

    @Test
    fun teleportCommandIncludesDimension() {
        assertEquals("/sl tp the_nether 1 64 -3", SpaceLoggerCommand.teleportCommand("the_nether", 1, 64, -3))
    }
}
