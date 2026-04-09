package me.zly2006.sl.command

import com.mojang.brigadier.arguments.IntegerArgumentType
import com.mojang.brigadier.arguments.StringArgumentType
import com.mojang.brigadier.context.CommandContext
import com.mojang.brigadier.exceptions.CommandSyntaxException
import com.mojang.brigadier.exceptions.SimpleCommandExceptionType
import com.mojang.brigadier.suggestion.Suggestions
import com.mojang.brigadier.suggestion.SuggestionsBuilder
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import me.zly2006.sl.SpaceLogger
import me.zly2006.sl.jni.NativeSpaceLoggerBridge
import net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback
import net.minecraft.ChatFormatting
import net.minecraft.commands.CommandSourceStack
import net.minecraft.core.BlockPos
import net.minecraft.core.Holder
import net.minecraft.core.registries.BuiltInRegistries
import net.minecraft.core.registries.Registries
import net.minecraft.gametest.framework.*
import net.minecraft.network.chat.ClickEvent
import net.minecraft.network.chat.Component
import net.minecraft.network.chat.HoverEvent
import net.minecraft.network.chat.MutableComponent
import net.minecraft.resources.Identifier
import net.minecraft.resources.ResourceKey
import net.minecraft.server.level.ServerLevel
import net.minecraft.server.level.ServerPlayer
import net.minecraft.util.RandomSource
import net.minecraft.world.level.portal.TeleportTransition
import net.minecraft.world.level.storage.LevelData.RespawnData
import net.minecraft.world.phys.Vec3
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.regex.Pattern

object SpaceLoggerCommand {
    private const val DEFAULT_LIMIT = 5
    private const val DEFAULT_PAGE = 1
    private const val MAX_LIMIT = 200
    private val HUMAN_DURATION_PATTERN = Pattern.compile("(\\d+)(ms|s|m|h|d|w)", Pattern.CASE_INSENSITIVE)
    private val TIME_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS z")
    private val FILTER_KEYS = listOf("subject", "object", "verb", "dimension", "range", "limit", "page", "before", "after")
    private val VERB_SUGGESTIONS = listOf("hurt", "kill", "break", "place", "use", "add_item", "remove_item", "command")
    private val DIMENSION_SUGGESTIONS = listOf("overworld", "the_nether", "the_end")
    private val RANGE_SUGGESTIONS = listOf("8", "16", "32", "64")
    private val LIMIT_SUGGESTIONS = listOf("5", "10", "25", "50", "100")
    private val PAGE_SUGGESTIONS = listOf("1", "2", "3", "4")
    private val TIME_SUGGESTIONS = listOf("30s", "5m", "30m", "1h", "1d")
    private val PLAYER_ONLY = SimpleCommandExceptionType(Component.literal("`/sl q` can only be executed by a player"))

    @JvmStatic
    fun register() {
        CommandRegistrationCallback.EVENT.register { dispatcher, _, _ ->
            dispatcher.register("gametest-all") {
                executes {
                    val level = it.source.server.overworld()
                    val random: RandomSource = level.getRandom()
                    val startPos = BlockPos(
                        random.nextIntBetweenInclusive(-14999992, 14999992),
                        -59,
                        random.nextIntBetweenInclusive(-14999992, 14999992)
                    )
                    it.source.player?.teleport(TeleportTransition(level, startPos.bottomCenter, Vec3.ZERO, 0.0f, 0.0f, TeleportTransition.DO_NOTHING))
                    level.setRespawnData(RespawnData.of(level.dimension(), startPos, 0.0f, 0.0f))
                    val testRegistry = level.registryAccess().lookupOrThrow(Registries.TEST_INSTANCE)
                    val testBatches = GameTestBatchFactory.divideIntoBatches(testRegistry.listElements()
                        .filter { test -> !test!!.value().manualOnly() }
                        .toList(), GameTestBatchFactory.DIRECT, level)
                    val runner = GameTestRunner.Builder.fromBatches(testBatches, level)
                        .newStructureSpawner(StructureGridSpawner(startPos, 8, false)).build()
                    val testInfos: MutableCollection<GameTestInfo> = runner.getTestInfos()
                    runner.start()
                    GlobalScope.launch(it.source.server.asCoroutineDispatcher()) {
                        while (testInfos.any { info -> !info.isDone }) {
                            kotlinx.coroutines.delay(50)
                        }
                        val passed = testInfos.count { info -> info.error == null }
                        val total = testInfos.size
                        it.source.server.playerList.players.toMutableList().forEach {
                            if ("MockPlayer" in it.scoreboardName) {
                                it.connection.disconnect(Component.literal("test completed"))
                            }
                        }
                        it.source.sendSystemMessage(Component.literal("====================").withStyle(ChatFormatting.GREEN))
                        it.source.sendSystemMessage(Component.literal("所有测试完成: $passed/$total 通过").withStyle(ChatFormatting.GREEN))
                        it.source.sendSystemMessage(Component.literal("====================").withStyle(ChatFormatting.GREEN))
                    }
                }
            }
            dispatcher.register("sl") {
                literal("tp") {
                    argument("dimension", StringArgumentType.word()) {
                        argument("x", IntegerArgumentType.integer()) {
                            argument("y", IntegerArgumentType.integer()) {
                                argument("z", IntegerArgumentType.integer()) {
                                    executeReturns { ctx ->
                                        executeTeleport(
                                            ctx.source,
                                            StringArgumentType.getString(ctx, "dimension"),
                                            IntegerArgumentType.getInteger(ctx, "x"),
                                            IntegerArgumentType.getInteger(ctx, "y"),
                                            IntegerArgumentType.getInteger(ctx, "z"),
                                        )
                                    }
                                }
                            }
                        }
                    }
                    argument("x", IntegerArgumentType.integer()) {
                        argument("y", IntegerArgumentType.integer()) {
                            argument("z", IntegerArgumentType.integer()) {
                                executeReturns { ctx ->
                                    executeTeleport(
                                        ctx.source,
                                        NativeSpaceLoggerBridge.dimension(ctx.source.level),
                                        IntegerArgumentType.getInteger(ctx, "x"),
                                        IntegerArgumentType.getInteger(ctx, "y"),
                                        IntegerArgumentType.getInteger(ctx, "z"),
                                    )
                                }
                            }
                        }
                    }
                }
                literal("q") {
                    executeReturns { ctx -> executeQuery(ctx.source, "") }
                    argument("filters", StringArgumentType.greedyString()) {
                        suggests { context, builder -> suggestQueryFilters(context, builder) }
                        executeReturns { ctx -> executeQuery(ctx.source, StringArgumentType.getString(ctx, "filters")) }
                    }
                }
                literal("stat") {
                    executeReturns { ctx -> executeStat(ctx.source) }
                }
            }
        }
    }

    private fun executeTeleport(source: CommandSourceStack, dimensionName: String, x: Int, y: Int, z: Int): Int {
        val player = source.player ?: throw PLAYER_ONLY.create()
        val targetLevel = resolveLevel(source, dimensionName)
        player.teleportTo(targetLevel, x + 0.5, y.toDouble(), z + 0.5, emptySet(), player.yRot, player.xRot, false)
        return 1
    }

    private fun executeQuery(source: CommandSourceStack, filterText: String): Int {
        val player = source.player ?: throw PLAYER_ONLY.create()
        val filters = parseFilters(filterText, player)
        val pagination = resolvePagination(filters.page, filters.limit)
        val rows = SpaceLogger.bridge().queryRows(
            filters.dimension,
            filters.subject,
            filters.objectValue,
            filters.verbMask,
            filters.minX,
            filters.maxX,
            filters.minY,
            filters.maxY,
            filters.minZ,
            filters.maxZ,
            filters.afterTimeMs,
            filters.beforeTimeMs,
            pagination.fetchLimit,
        )
        val nowMs = System.currentTimeMillis()

        if (rows.size <= pagination.offset) {
            source.sendSystemMessage(formatEmptyPageMessage(filters))
            return 1
        }

        val hasNextPage = rows.size > pagination.offset + filters.limit
        val pageEndExclusive = minOf(rows.size, pagination.offset + filters.limit)
        val pageRows = rows.subList(pagination.offset, pageEndExclusive)
        source.sendSystemMessage(formatPageSummary(filters, pagination, pageRows.size, hasNextPage))

        var index = pagination.offset + 1
        for (row in pageRows) {
            source.sendSystemMessage(formatQueryRowLine(index, row, nowMs))
            index += 1
        }
        if (filters.page > DEFAULT_PAGE || hasNextPage) {
            source.sendSystemMessage(formatPaginationControls(filters, hasNextPage))
        }

        return pageRows.size
    }

    private fun executeStat(source: CommandSourceStack): Int {
        val stats = SpaceLogger.bridge().stats(5)
        source.sendSystemMessage(
            Component.literal(
                String.format(
                    Locale.ROOT,
                    "[space-logger] schema=%d rows=%d memtable=%d segments=%d wal=%s disk=%s",
                    stats.schemaVersion(),
                    stats.totalRows(),
                    stats.memtableRows(),
                    stats.segmentCount(),
                    formatBytes(stats.walSizeBytes()),
                    formatBytes(stats.diskUsageBytes()),
                ),
            ).withStyle(ChatFormatting.AQUA),
        )

        val latestSegments = stats.latestSegments()
        if (latestSegments.isEmpty()) {
            source.sendSystemMessage(Component.literal("[space-logger] no persisted segments").withStyle(ChatFormatting.DARK_GRAY))
            return 1
        }

        source.sendSystemMessage(Component.literal("[space-logger] latest segments:").withStyle(ChatFormatting.GRAY))
        latestSegments.forEachIndexed { index, segment ->
            source.sendSystemMessage(
                Component.literal(
                    String.format(
                        Locale.ROOT,
                        "#%d %s rows=%d seq=%d..%d time=%d..%d size=%s xyz=[%d..%d,%d..%d,%d..%d]",
                        index + 1,
                        segment.fileName(),
                        segment.rowCount(),
                        segment.minSeq(),
                        segment.maxSeq(),
                        segment.minTimeMs(),
                        segment.maxTimeMs(),
                        formatBytes(segment.sizeBytes()),
                        segment.minX(),
                        segment.maxX(),
                        segment.minY(),
                        segment.maxY(),
                        segment.minZ(),
                        segment.maxZ(),
                    ),
                ).withStyle(ChatFormatting.YELLOW),
            )
        }
        return 1
    }

    private fun formatQueryRowLine(index: Int, row: NativeSpaceLoggerBridge.QueryRow, nowMs: Long): MutableComponent {
        val fullTimestamp = Instant.ofEpochMilli(row.timeMs()).atZone(ZoneId.systemDefault()).format(TIME_FORMATTER)
        val relative = formatRelativeTime(row.timeMs(), nowMs)
        val coordText = "@" + when (row.dimension()) {
            "overworld" -> "O"
            "the_nether" -> "N"
            "the_end" -> "E"
            else -> row.dimension()
        } + " ${row.x()},${row.y()},${row.z()}"
        val tpCommand = teleportCommand(row.dimension(), row.x(), row.y(), row.z())

        val line = Component.empty()
        line.append(Component.literal("#$index").withStyle(ChatFormatting.GRAY))
        line.append(Component.literal(" "))
        line.append(
            Component.literal(relative).withStyle { style ->
                style.withColor(ChatFormatting.AQUA)
                    .withHoverEvent(HoverEvent.ShowText(Component.literal(fullTimestamp)))
            },
        )
        line.append(Component.literal(" "))
        line.append(Component.literal(row.subject()).withStyle(ChatFormatting.WHITE))
        line.append(Component.literal(" "))
        line.append(Component.literal(row.verbName()).withStyle(ChatFormatting.GOLD))
        line.append(Component.literal(" "))
        line.append(formatObjectComponent(row).withStyle(ChatFormatting.YELLOW))
        line.append(Component.literal(" "))
        line.append(
            Component.literal(coordText).withStyle { style ->
                style.withColor(ChatFormatting.GREEN)
                    .withHoverEvent(HoverEvent.ShowText(Component.literal("点击可传送")))
                    .withClickEvent(ClickEvent.RunCommand(tpCommand))
            },
        )
        if (row.dataLen() > 0) {
            if (row.verb() == NativeSpaceLoggerBridge.VERB_COMMAND) {
                line.append(Component.literal(" "))
                line.append(
                    Component.literal("(cmd)").withStyle { style ->
                        style.withColor(ChatFormatting.DARK_GRAY)
                            .withHoverEvent(HoverEvent.ShowText(Component.literal(String(row.dataHead()))))
                    },
                )
            } else {
                line.append(Component.literal(" "))
                line.append(
                    Component.literal("(+${row.dataLen()})").withStyle { style ->
                        style.withColor(ChatFormatting.DARK_GRAY)
                            .withHoverEvent(HoverEvent.ShowText(Component.literal("nbt数据大小: ${row.dataLen()} bytes")))
                    },
                )
            }
        }
        return line
    }

    private fun formatObjectComponent(row: NativeSpaceLoggerBridge.QueryRow): MutableComponent {
        val objectComponent = formatObjectComponent(row.`object`())
        val quantity = quantityForDisplay(row)
        if (quantity > 0) {
            objectComponent.append(Component.literal(" x$quantity"))
        }
        return objectComponent
    }

    private fun formatObjectComponent(objectRaw: String?): MutableComponent {
        if (objectRaw.isNullOrBlank()) {
            return Component.literal("")
        }

        val objectId = Identifier.tryParse(objectRaw) ?: return Component.literal(objectRaw)
        return when {
            BuiltInRegistries.ITEM.containsKey(objectId) ->
                Component.translatable(BuiltInRegistries.ITEM.getValue(objectId).descriptionId)
            BuiltInRegistries.BLOCK.containsKey(objectId) ->
                Component.translatable(objectId.toLanguageKey("block"))
            BuiltInRegistries.ENTITY_TYPE.containsKey(objectId) ->
                Component.translatable(BuiltInRegistries.ENTITY_TYPE.getValue(objectId).descriptionId)
            else -> Component.literal(objectRaw)
        }
    }

    private fun quantityForDisplay(row: NativeSpaceLoggerBridge.QueryRow): Int {
        if (!row.hasInventoryDataHeader()) {
            return 0
        }
        return kotlin.math.abs(row.quantityDelta()).takeIf { it != 0 } ?: 0
    }

    private fun formatRelativeTime(eventTimeMs: Long, nowMs: Long): String {
        val deltaMs = maxOf(0L, nowMs - eventTimeMs)
        val deltaSec = deltaMs / 1_000L
        if (deltaSec < 1L) {
            return "now"
        }
        val steps = longArrayOf(7L * 24L * 3_600L, 24L * 3_600L, 3_600L, 60L, 1L)
        val suffixes = arrayOf("w", "d", "h", "m", "s")
        for (i in steps.indices) {
            val unit = steps[i]
            if (deltaSec >= unit) {
                return "${deltaSec / unit}${suffixes[i]} ago"
            }
        }
        return "now"
    }

    private fun formatBytes(bytes: Long): String {
        if (bytes < 1024L) return "$bytes B"
        val kib = bytes / 1024.0
        if (kib < 1024.0) return String.format(Locale.ROOT, "%.1f KiB", kib)
        val mib = kib / 1024.0
        if (mib < 1024.0) return String.format(Locale.ROOT, "%.1f MiB", mib)
        return String.format(Locale.ROOT, "%.1f GiB", mib / 1024.0)
    }

    @JvmStatic
    fun teleportCommand(dimension: String, x: Int, y: Int, z: Int): String {
        return "/sl tp $dimension $x $y $z"
    }

    private fun resolveLevel(source: CommandSourceStack, dimensionName: String): ServerLevel {
        val key = ResourceKey.create(Registries.DIMENSION, parseDimensionIdentifier(dimensionName))
        return source.server.getLevel(key) ?: throw syntax("unknown dimension: `$dimensionName`")
    }

    private fun parseDimensionIdentifier(dimensionName: String?): Identifier {
        if (dimensionName.isNullOrBlank()) {
            throw syntax("dimension must not be empty")
        }
        return Identifier.tryParse(dimensionName)
            ?: throw syntax("invalid dimension identifier: `$dimensionName`")
    }

    @JvmStatic
    fun parseFilters(rawFilterText: String?, player: ServerPlayer?): ParsedFilters {
        val tokens = rawFilterText
            ?.takeIf { it.isNotBlank() }
            ?.trim()
            ?.split(Regex("\\s+"))
            ?: emptyList()

        var subject = ""
        var objectValue = ""
        var dimension = ""
        var verbMask = NativeSpaceLoggerBridge.VERB_MASK_ALL
        var range: Int? = null
        var limit: Int? = null
        var page: Int? = null
        var afterTimeMs: Long? = null
        var beforeTimeMs: Long? = null
        val nowMs = System.currentTimeMillis()
        val pageBaseTokens = mutableListOf<String>()
        var explicitDimension = false

        for (token in tokens) {
            val kv = token.split(":", limit = 2)
            if (kv.size != 2 || kv[0].isBlank() || kv[1].isBlank()) {
                throw syntax("invalid filter token: `$token`, expected key:value")
            }

            val key = kv[0].lowercase(Locale.ROOT)
            val value = kv[1]
            when (key) {
                "subject" -> subject = value
                "object" -> objectValue = value
                "dimension" -> {
                    dimension = value
                    explicitDimension = true
                }
                "verb" -> verbMask = verbMask and parseVerbMask(value)
                "range" -> range = parsePositiveInt(value, "range")
                "limit" -> {
                    val parsed = parsePositiveInt(value, "limit")
                    if (parsed == 0) throw syntax("limit must be > 0")
                    if (parsed > MAX_LIMIT) throw syntax("limit too large, max is $MAX_LIMIT")
                    limit = parsed
                }
                "page" -> {
                    val parsed = parsePositiveInt(value, "page")
                    if (parsed == 0) throw syntax("page must be > 0")
                    page = parsed
                }
                "before" -> beforeTimeMs = nowMs - parseHumanDurationMillis(value)
                "after" -> afterTimeMs = nowMs - parseHumanDurationMillis(value)
                else -> throw syntax("unknown filter key: `$key`")
            }
            if (key != "page") {
                pageBaseTokens += token
            }
        }

        val minTime = afterTimeMs ?: Long.MIN_VALUE
        val maxTime = beforeTimeMs ?: Long.MAX_VALUE
        if (minTime > maxTime) {
            throw syntax("`after` must be earlier than `before`")
        }

        var minX = Int.MIN_VALUE
        var maxX = Int.MAX_VALUE
        var minY = Int.MIN_VALUE
        var maxY = Int.MAX_VALUE
        var minZ = Int.MIN_VALUE
        var maxZ = Int.MAX_VALUE
        if (player != null && dimension.isBlank()) {
            dimension = NativeSpaceLoggerBridge.dimension(player.level())
            if (!explicitDimension) {
                pageBaseTokens += "dimension:$dimension"
            }
        }
        if (range != null) {
            val center = player?.blockPosition() ?: throw syntax("range filter requires a player context")
            minX = safeAdd(center.x, -range)
            maxX = safeAdd(center.x, range)
            minY = safeAdd(center.y, -range)
            maxY = safeAdd(center.y, range)
            minZ = safeAdd(center.z, -range)
            maxZ = safeAdd(center.z, range)
        }

        return ParsedFilters(
            subject = subject,
            objectValue = objectValue,
            dimension = dimension,
            verbMask = verbMask,
            minX = minX,
            maxX = maxX,
            minY = minY,
            maxY = maxY,
            minZ = minZ,
            maxZ = maxZ,
            afterTimeMs = minTime,
            beforeTimeMs = maxTime,
            limit = limit ?: DEFAULT_LIMIT,
            page = page ?: DEFAULT_PAGE,
            pageBaseTokens = pageBaseTokens.toList(),
        )
    }

    @JvmStatic
    fun resolvePagination(page: Int, limit: Int): QueryPagination {
        val offset = (page - 1).toLong() * limit
        val fetchLimit = offset + limit + 1L
        if (offset > Int.MAX_VALUE) throw syntax("page is too large")
        if (fetchLimit > Int.MAX_VALUE) throw syntax("page * limit is too large")
        return QueryPagination(offset.toInt(), fetchLimit.toInt())
    }

    @JvmStatic
    fun completeFilterToken(rawFilters: String?, playerName: String?): List<String> {
        val filters = rawFilters ?: ""
        val endsWithSpace = filters.isNotEmpty() && filters.last().isWhitespace()
        val tokenStart = if (endsWithSpace) filters.length else filters.lastIndexOf(' ') + 1
        val token = if (tokenStart >= filters.length) "" else filters.substring(tokenStart)
        return suggestionsForToken(token, playerName)
    }

    private fun suggestQueryFilters(
        context: CommandContext<CommandSourceStack>,
        builder: SuggestionsBuilder,
    ): CompletableFuture<Suggestions> {
        val rawFilters = builder.remaining
        val endsWithSpace = rawFilters.isNotEmpty() && rawFilters.last().isWhitespace()
        val tokenStart = if (endsWithSpace) rawFilters.length else rawFilters.lastIndexOf(' ') + 1
        val token = if (tokenStart >= rawFilters.length) "" else rawFilters.substring(tokenStart)
        val playerName = context.source.player?.scoreboardName
        val tokenBuilder = builder.createOffset(builder.start + tokenStart)
        for (suggestion in suggestionsForToken(token, playerName)) {
            tokenBuilder.suggest(suggestion)
        }
        return tokenBuilder.buildFuture()
    }

    private fun suggestionsForToken(token: String?, playerName: String?): List<String> {
        if (token.isNullOrBlank()) {
            return FILTER_KEYS.map { "$it:" }
        }

        val colonIndex = token.indexOf(':')
        if (colonIndex < 0) {
            val prefix = token.lowercase(Locale.ROOT)
            return FILTER_KEYS.filter { it.startsWith(prefix) }.map { "$it:" }
        }

        val key = token.substring(0, colonIndex).lowercase(Locale.ROOT)
        val value = token.substring(colonIndex + 1)
        return when (key) {
            "subject" -> suggestSingleValueToken(key, value, subjectSuggestions(playerName))
            "object" -> suggestSingleValueToken(key, value, listOf("minecraft:chest", "minecraft:stone", "minecraft:lever"))
            "verb" -> suggestVerbToken(value)
            "dimension" -> suggestSingleValueToken(key, value, DIMENSION_SUGGESTIONS)
            "range" -> suggestSingleValueToken(key, value, RANGE_SUGGESTIONS)
            "limit" -> suggestSingleValueToken(key, value, LIMIT_SUGGESTIONS)
            "page" -> suggestSingleValueToken(key, value, PAGE_SUGGESTIONS)
            "before", "after" -> suggestSingleValueToken(key, value, TIME_SUGGESTIONS)
            else -> emptyList()
        }
    }

    private fun subjectSuggestions(playerName: String?): List<String> {
        val suggestions = LinkedHashSet<String>()
        if (!playerName.isNullOrBlank()) suggestions += playerName
        suggestions += "player"
        suggestions += "minecraft:zombie"
        return suggestions.toList()
    }

    private fun suggestSingleValueToken(key: String, value: String?, candidates: List<String>): List<String> {
        val normalizedValue = value ?: ""
        return candidates.filter { it.startsWith(normalizedValue) }.map { "$key:$it" }
    }

    private fun suggestVerbToken(value: String?): List<String> {
        val normalizedValue = value ?: ""
        val commaIndex = normalizedValue.lastIndexOf(',')
        val prefix = if (commaIndex < 0) "" else normalizedValue.substring(0, commaIndex + 1)
        val fragment = if (commaIndex < 0) normalizedValue else normalizedValue.substring(commaIndex + 1)
        return VERB_SUGGESTIONS.filter { it.startsWith(fragment) }.map { "verb:$prefix$it" }
    }

    private fun formatEmptyPageMessage(filters: ParsedFilters): MutableComponent {
        val message = Component.literal("[space-logger] 第 ${filters.page} 页没有记录").withStyle(ChatFormatting.RED)
        if (filters.page > DEFAULT_PAGE) {
            message.append(Component.literal(" "))
            message.append(pageLink("上一页", filters.commandForPage(filters.page - 1), ChatFormatting.YELLOW))
        }
        return message
    }

    private fun formatPageSummary(
        filters: ParsedFilters,
        pagination: QueryPagination,
        pageSize: Int,
        hasNextPage: Boolean,
    ): MutableComponent {
        val startIndex = pagination.offset + 1
        val endIndex = pagination.offset + pageSize
        val message = Component.literal("[space-logger] 第 ${filters.page} 页，显示第 $startIndex-$endIndex 条").withStyle(ChatFormatting.GRAY)
        if (hasNextPage) {
            message.append(Component.literal(" (还有更多)").withStyle(ChatFormatting.DARK_GRAY))
        }
        return message
    }

    private fun formatPaginationControls(filters: ParsedFilters, hasNextPage: Boolean): MutableComponent {
        val message = Component.literal("[space-logger] ").withStyle(ChatFormatting.GRAY)
        if (filters.page > DEFAULT_PAGE) {
            message.append(pageLink("上一页", filters.commandForPage(filters.page - 1), ChatFormatting.YELLOW))
        } else {
            message.append(Component.literal("上一页").withStyle(ChatFormatting.DARK_GRAY))
        }
        message.append(Component.literal(" | ").withStyle(ChatFormatting.DARK_GRAY))
        if (hasNextPage) {
            message.append(pageLink("下一页", filters.commandForPage(filters.page + 1), ChatFormatting.GREEN))
        } else {
            message.append(Component.literal("下一页").withStyle(ChatFormatting.DARK_GRAY))
        }
        return message
    }

    private fun pageLink(label: String, command: String, color: ChatFormatting): MutableComponent {
        return Component.literal(label).withStyle { style ->
            style.withColor(color)
                .withHoverEvent(HoverEvent.ShowText(Component.literal(command)))
                .withClickEvent(ClickEvent.RunCommand(command))
        }
    }

    private fun parsePositiveInt(value: String, field: String): Int {
        val parsed = value.toIntOrNull() ?: throw syntax("$field expects an integer value")
        if (parsed < 0) throw syntax("$field must be >= 0")
        return parsed
    }

    private fun parseVerbMask(rawValue: String?): Int {
        if (rawValue.isNullOrBlank()) {
            throw syntax("verb filter must not be empty")
        }

        var mask = 0
        for (token in rawValue.split(",")) {
            val value = token.trim()
            if (value.isEmpty()) continue

            var verbId = NativeSpaceLoggerBridge.verbIdFromName(value)
            if (verbId < 0) {
                verbId = value.toIntOrNull() ?: throw syntax("unknown verb: `$value`")
            }
            if (verbId !in 0 until 32) {
                throw syntax("verb id out of range: `$verbId`, expected 0..31")
            }
            mask = mask or NativeSpaceLoggerBridge.verbMaskSingle(verbId)
        }

        if (mask == 0) throw syntax("verb filter produced empty set")
        return mask
    }

    private fun parseHumanDurationMillis(value: String): Long {
        val matcher = HUMAN_DURATION_PATTERN.matcher(value)
        var index = 0
        var total = 0L
        var matchedAny = false

        while (matcher.find()) {
            if (matcher.start() != index) throw syntax("invalid time expression: `$value`")
            matchedAny = true
            val amount = matcher.group(1).toLongOrNull() ?: throw syntax("invalid time amount in `$value`")
            val unitMillis = when (matcher.group(2).lowercase(Locale.ROOT)) {
                "ms" -> 1L
                "s" -> 1_000L
                "m" -> 60_000L
                "h" -> 3_600_000L
                "d" -> 86_400_000L
                "w" -> 604_800_000L
                else -> throw syntax("unsupported time unit in `$value`")
            }
            try {
                total = Math.addExact(total, Math.multiplyExact(amount, unitMillis))
            } catch (_: ArithmeticException) {
                throw syntax("time expression overflow: `$value`")
            }
            index = matcher.end()
        }

        if (!matchedAny || index != value.length) {
            throw syntax("invalid time expression: `$value`; examples: 30m, 3h, 1d12h")
        }
        return total
    }

    private fun safeAdd(base: Int, delta: Int): Int {
        try {
            return Math.addExact(base, delta)
        } catch (_: ArithmeticException) {
            throw syntax("range overflows world coordinate bounds")
        }
    }

    private fun syntax(message: String): CommandSyntaxException {
        return SimpleCommandExceptionType(Component.literal(message)).create()
    }

    class ParsedFilters(
        @JvmField val subject: String,
        @JvmField val objectValue: String,
        @JvmField val dimension: String,
        @JvmField val verbMask: Int,
        @JvmField val minX: Int,
        @JvmField val maxX: Int,
        @JvmField val minY: Int,
        @JvmField val maxY: Int,
        @JvmField val minZ: Int,
        @JvmField val maxZ: Int,
        @JvmField val afterTimeMs: Long,
        @JvmField val beforeTimeMs: Long,
        @JvmField val limit: Int,
        @JvmField val page: Int,
        @JvmField val pageBaseTokens: List<String>,
    ) {
        @JvmField
        val object_: String = objectValue

        fun commandForPage(targetPage: Int): String {
            val tokens = pageBaseTokens.toMutableList()
            if (targetPage > DEFAULT_PAGE) {
                tokens += "page:$targetPage"
            }
            return if (tokens.isEmpty()) "/sl q" else "/sl q ${tokens.joinToString(" ")}"
        }
    }

    class QueryPagination(
        @JvmField val offset: Int,
        @JvmField val fetchLimit: Int,
    )
}
