package com.github.zly2006.sl.command;

import com.github.zly2006.sl.SpaceLogger;
import com.github.zly2006.sl.jni.NativeSpaceLoggerBridge;
import com.mojang.brigadier.arguments.IntegerArgumentType;
import com.mojang.brigadier.arguments.StringArgumentType;
import com.mojang.brigadier.context.CommandContext;
import com.mojang.brigadier.exceptions.CommandSyntaxException;
import com.mojang.brigadier.exceptions.SimpleCommandExceptionType;
import com.mojang.brigadier.suggestion.Suggestions;
import com.mojang.brigadier.suggestion.SuggestionsBuilder;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback;
import net.minecraft.ChatFormatting;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.core.BlockPos;
import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.network.chat.ClickEvent;
import net.minecraft.network.chat.Component;
import net.minecraft.network.chat.HoverEvent;
import net.minecraft.network.chat.MutableComponent;
import net.minecraft.resources.Identifier;
import net.minecraft.server.level.ServerPlayer;

public final class SpaceLoggerCommand {
    private static final int DEFAULT_LIMIT = 5;
    private static final int DEFAULT_PAGE = 1;
    private static final int MAX_LIMIT = 200;
    private static final Pattern HUMAN_DURATION_PATTERN = Pattern.compile("(\\d+)(ms|s|m|h|d|w)", Pattern.CASE_INSENSITIVE);
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS z");
    private static final List<String> FILTER_KEYS = List.of("subject", "object", "verb", "range", "limit", "page", "before", "after");
    private static final List<String> VERB_SUGGESTIONS = List.of("hurt", "kill", "break", "place", "use", "add_item", "remove_item");
    private static final List<String> RANGE_SUGGESTIONS = List.of("8", "16", "32", "64");
    private static final List<String> LIMIT_SUGGESTIONS = List.of("5", "10", "25", "50", "100");
    private static final List<String> PAGE_SUGGESTIONS = List.of("1", "2", "3", "4");
    private static final List<String> TIME_SUGGESTIONS = List.of("30s", "5m", "30m", "1h", "1d");

    private static final SimpleCommandExceptionType PLAYER_ONLY = new SimpleCommandExceptionType(
        Component.literal("`/sl q` can only be executed by a player")
    );

    private SpaceLoggerCommand() {
    }

    public static void register() {
        CommandRegistrationCallback.EVENT.register((dispatcher, buildContext, selection) ->
            dispatcher.register(
                Commands.literal("sl")
                    .then(
                        Commands.literal("tp")
                            .then(
                                Commands.argument("x", IntegerArgumentType.integer())
                                    .then(
                                        Commands.argument("y", IntegerArgumentType.integer())
                                            .then(
                                                Commands.argument("z", IntegerArgumentType.integer())
                                                    .executes(ctx -> executeTeleport(
                                                        ctx.getSource(),
                                                        IntegerArgumentType.getInteger(ctx, "x"),
                                                        IntegerArgumentType.getInteger(ctx, "y"),
                                                        IntegerArgumentType.getInteger(ctx, "z")
                                                    ))
                                            )
                                    )
                            )
                    )
                    .then(
                        Commands.literal("q")
                            .executes(ctx -> executeQuery(ctx.getSource(), ""))
                            .then(
                                Commands.argument("filters", StringArgumentType.greedyString())
                                    .suggests(SpaceLoggerCommand::suggestQueryFilters)
                                    .executes(ctx -> executeQuery(ctx.getSource(), StringArgumentType.getString(ctx, "filters")))
                            )
                    )
            )
        );
    }

    private static int executeTeleport(CommandSourceStack source, int x, int y, int z) throws CommandSyntaxException {
        ServerPlayer player = source.getPlayer();
        if (player == null) {
            throw PLAYER_ONLY.create();
        }

        player.teleportTo(x + 0.5D, y, z + 0.5D);
        return 1;
    }

    private static int executeQuery(CommandSourceStack source, String filterText) throws CommandSyntaxException {
        ServerPlayer player = source.getPlayer();
        if (player == null) {
            throw PLAYER_ONLY.create();
        }

        ParsedFilters filters = parseFilters(filterText, player);
        QueryPagination pagination = resolvePagination(filters.page, filters.limit);
        List<NativeSpaceLoggerBridge.QueryRow> rows = SpaceLogger.bridge().queryRows(
            filters.subject,
            filters.object,
            filters.verbMask,
            filters.minX,
            filters.maxX,
            filters.minY,
            filters.maxY,
            filters.minZ,
            filters.maxZ,
            filters.afterTimeMs,
            filters.beforeTimeMs,
            pagination.fetchLimit
        );
        long nowMs = System.currentTimeMillis();

        if (rows.size() <= pagination.offset) {
            source.sendSystemMessage(formatEmptyPageMessage(filters));
            return 1;
        }

        boolean hasNextPage = rows.size() > pagination.offset + filters.limit;
        int pageEndExclusive = Math.min(rows.size(), pagination.offset + filters.limit);
        List<NativeSpaceLoggerBridge.QueryRow> pageRows = rows.subList(pagination.offset, pageEndExclusive);
        source.sendSystemMessage(formatPageSummary(filters, pagination, pageRows.size(), hasNextPage));

        int index = pagination.offset + 1;
        for (NativeSpaceLoggerBridge.QueryRow row : pageRows) {
            source.sendSystemMessage(formatQueryRowLine(index, row, nowMs));
            index += 1;
        }
        if (filters.page > DEFAULT_PAGE || hasNextPage) {
            source.sendSystemMessage(formatPaginationControls(filters, hasNextPage));
        }

        return pageRows.size();
    }

    private static MutableComponent formatQueryRowLine(int index, NativeSpaceLoggerBridge.QueryRow row, long nowMs) {
        String fullTimestamp = Instant.ofEpochMilli(row.timeMs())
            .atZone(ZoneId.systemDefault())
            .format(TIME_FORMATTER);
        String relative = formatRelativeTime(row.timeMs(), nowMs);
        String coordText = "@ " + row.x() + "," + row.y() + "," + row.z();
        String tpCommand = "/sl tp " + row.x() + " " + row.y() + " " + row.z();

        MutableComponent line = Component.empty();
        line.append(Component.literal("#" + index).withStyle(ChatFormatting.GRAY));
        line.append(Component.literal(" "));
        line.append(Component.literal(relative).withStyle(style ->
            style.withColor(ChatFormatting.AQUA)
                .withHoverEvent(new HoverEvent.ShowText(Component.literal(fullTimestamp)))
        ));
        line.append(Component.literal(" "));
        line.append(Component.literal(row.subject()).withStyle(ChatFormatting.WHITE));
        line.append(Component.literal(" "));
        line.append(Component.literal(row.verbName()).withStyle(ChatFormatting.GOLD));
        line.append(Component.literal(" "));
        line.append(formatObjectComponent(row).withStyle(ChatFormatting.YELLOW));
        line.append(Component.literal(" "));
        line.append(Component.literal(coordText).withStyle(style ->
            style.withColor(ChatFormatting.GREEN)
                .withHoverEvent(new HoverEvent.ShowText(Component.literal("点击可传送")))
                .withClickEvent(new ClickEvent.RunCommand(tpCommand))
        ));
        if (row.dataLen() > 0) {
            line.append(Component.literal(" "));
            line.append(Component.literal("(+" + row.dataLen() + ")").withStyle(style ->
                    style.withColor(ChatFormatting.DARK_GRAY)
                            .withHoverEvent(new HoverEvent.ShowText(Component.literal("nbt数据大小: " + row.dataLen() + " bytes")))
            ));
        }
        return line;
    }

    private static MutableComponent formatObjectComponent(NativeSpaceLoggerBridge.QueryRow row) {
        MutableComponent objectComponent = formatObjectComponent(row.object());
        int quantity = quantityForDisplay(row);
        if (quantity > 0) {
            objectComponent.append(Component.literal(" x" + quantity));
        }
        return objectComponent;
    }

    private static MutableComponent formatObjectComponent(String objectRaw) {
        if (objectRaw == null || objectRaw.isBlank()) {
            return Component.literal("");
        }

        Identifier objectId = Identifier.tryParse(objectRaw);
        if (objectId == null) {
            return Component.literal(objectRaw);
        }

        if (BuiltInRegistries.ITEM.containsKey(objectId)) {
            return Component.translatable(BuiltInRegistries.ITEM.getValue(objectId).getDescriptionId());
        }
        if (BuiltInRegistries.BLOCK.containsKey(objectId)) {
            return Component.translatable(objectId.toLanguageKey("block"));
        }
        if (BuiltInRegistries.ENTITY_TYPE.containsKey(objectId)) {
            return Component.translatable(BuiltInRegistries.ENTITY_TYPE.getValue(objectId).getDescriptionId());
        }

        return Component.literal(objectRaw);
    }

    private static int quantityForDisplay(NativeSpaceLoggerBridge.QueryRow row) {
        if (!row.hasInventoryDataHeader()) {
            return 0;
        }
        int delta = row.quantityDelta();
        if (delta == 0) {
            return 0;
        }
        return Math.abs(delta);
    }

    private static String formatRelativeTime(long eventTimeMs, long nowMs) {
        long deltaMs = Math.max(0L, nowMs - eventTimeMs);
        long deltaSec = deltaMs / 1_000L;
        if (deltaSec < 1L) {
            return "now";
        }
        long[] steps = new long[] {
            7L * 24L * 3_600L,
            24L * 3_600L,
            3_600L,
            60L,
            1L
        };
        String[] suffixes = new String[] {"w", "d", "h", "m", "s"};
        for (int i = 0; i < steps.length; i++) {
            long unit = steps[i];
            if (deltaSec >= unit) {
                return (deltaSec / unit) + suffixes[i] + " ago";
            }
        }
        return "now";
    }

    static ParsedFilters parseFilters(String rawFilterText, ServerPlayer player) throws CommandSyntaxException {
        String[] tokens = rawFilterText == null || rawFilterText.isBlank()
            ? new String[0]
            : rawFilterText.trim().split("\\s+");

        String subject = "";
        String object = "";
        int verbMask = NativeSpaceLoggerBridge.VERB_MASK_ALL;
        Integer range = null;
        Integer limit = null;
        Integer page = null;
        Long afterTimeMs = null;
        Long beforeTimeMs = null;
        long nowMs = System.currentTimeMillis();
        List<String> pageBaseTokens = new ArrayList<>();

        for (String token : tokens) {
            String[] kv = token.split(":", 2);
            if (kv.length != 2 || kv[0].isBlank() || kv[1].isBlank()) {
                throw syntax("invalid filter token: `" + token + "`, expected key:value");
            }

            String key = kv[0].toLowerCase(Locale.ROOT);
            String value = kv[1];

            switch (key) {
                case "subject" -> subject = value;
                case "object" -> object = value;
                case "verb" -> verbMask &= parseVerbMask(value);
                case "range" -> {
                    int parsed = parsePositiveInt(value, "range");
                    range = parsed;
                }
                case "limit" -> {
                    int parsed = parsePositiveInt(value, "limit");
                    if (parsed == 0) {
                        throw syntax("limit must be > 0");
                    }
                    if (parsed > MAX_LIMIT) {
                        throw syntax("limit too large, max is " + MAX_LIMIT);
                    }
                    limit = parsed;
                }
                case "page" -> {
                    int parsed = parsePositiveInt(value, "page");
                    if (parsed == 0) {
                        throw syntax("page must be > 0");
                    }
                    page = parsed;
                }
                case "before" -> {
                    long durationMs = parseHumanDurationMillis(value);
                    beforeTimeMs = nowMs - durationMs;
                }
                case "after" -> {
                    long durationMs = parseHumanDurationMillis(value);
                    afterTimeMs = nowMs - durationMs;
                }
                default -> throw syntax("unknown filter key: `" + key + "`");
            }

            if (!"page".equals(key)) {
                pageBaseTokens.add(token);
            }
        }

        long minTime = afterTimeMs == null ? Long.MIN_VALUE : afterTimeMs;
        long maxTime = beforeTimeMs == null ? Long.MAX_VALUE : beforeTimeMs;
        if (minTime > maxTime) {
            throw syntax("`after` must be earlier than `before`");
        }

        int minX = Integer.MIN_VALUE;
        int maxX = Integer.MAX_VALUE;
        int minY = Integer.MIN_VALUE;
        int maxY = Integer.MAX_VALUE;
        int minZ = Integer.MIN_VALUE;
        int maxZ = Integer.MAX_VALUE;
        if (range != null) {
            if (player == null) {
                throw syntax("range filter requires a player context");
            }
            BlockPos center = player.blockPosition();
            minX = safeAdd(center.getX(), -range);
            maxX = safeAdd(center.getX(), range);
            minY = safeAdd(center.getY(), -range);
            maxY = safeAdd(center.getY(), range);
            minZ = safeAdd(center.getZ(), -range);
            maxZ = safeAdd(center.getZ(), range);
        }

        return new ParsedFilters(
            subject,
            object,
            verbMask,
            minX,
            maxX,
            minY,
            maxY,
            minZ,
            maxZ,
            minTime,
            maxTime,
            limit == null ? DEFAULT_LIMIT : limit,
            page == null ? DEFAULT_PAGE : page,
            pageBaseTokens
        );
    }

    static QueryPagination resolvePagination(int page, int limit) throws CommandSyntaxException {
        long offset = (long) (page - 1) * limit;
        long fetchLimit = offset + limit + 1L;
        if (offset > Integer.MAX_VALUE) {
            throw syntax("page is too large");
        }
        if (fetchLimit > Integer.MAX_VALUE) {
            throw syntax("page * limit is too large");
        }
        return new QueryPagination((int) offset, (int) fetchLimit);
    }

    static List<String> completeFilterToken(String rawFilters, String playerName) {
        String filters = rawFilters == null ? "" : rawFilters;
        boolean endsWithSpace = !filters.isEmpty() && Character.isWhitespace(filters.charAt(filters.length() - 1));
        int tokenStart = endsWithSpace ? filters.length() : filters.lastIndexOf(' ') + 1;
        String token = tokenStart >= filters.length() ? "" : filters.substring(tokenStart);
        return suggestionsForToken(token, playerName);
    }

    private static CompletableFuture<Suggestions> suggestQueryFilters(CommandContext<CommandSourceStack> context, SuggestionsBuilder builder) {
        String rawFilters = builder.getRemaining();
        boolean endsWithSpace = !rawFilters.isEmpty() && Character.isWhitespace(rawFilters.charAt(rawFilters.length() - 1));
        int tokenStart = endsWithSpace ? rawFilters.length() : rawFilters.lastIndexOf(' ') + 1;
        String token = tokenStart >= rawFilters.length() ? "" : rawFilters.substring(tokenStart);
        String playerName = null;
        ServerPlayer player = context.getSource().getPlayer();
        if (player != null) {
            playerName = player.getScoreboardName();
        }

        SuggestionsBuilder tokenBuilder = builder.createOffset(builder.getStart() + tokenStart);
        for (String suggestion : suggestionsForToken(token, playerName)) {
            tokenBuilder.suggest(suggestion);
        }
        return tokenBuilder.buildFuture();
    }

    private static List<String> suggestionsForToken(String token, String playerName) {
        if (token == null || token.isBlank()) {
            return FILTER_KEYS.stream()
                .map(key -> key + ":")
                .toList();
        }

        int colonIndex = token.indexOf(':');
        if (colonIndex < 0) {
            return FILTER_KEYS.stream()
                .filter(key -> key.startsWith(token.toLowerCase(Locale.ROOT)))
                .map(key -> key + ":")
                .toList();
        }

        String key = token.substring(0, colonIndex).toLowerCase(Locale.ROOT);
        String value = token.substring(colonIndex + 1);
        return switch (key) {
            case "subject" -> suggestSingleValueToken(key, value, subjectSuggestions(playerName));
            case "object" -> suggestSingleValueToken(key, value, List.of("minecraft:chest", "minecraft:stone", "minecraft:lever"));
            case "verb" -> suggestVerbToken(value);
            case "range" -> suggestSingleValueToken(key, value, RANGE_SUGGESTIONS);
            case "limit" -> suggestSingleValueToken(key, value, LIMIT_SUGGESTIONS);
            case "page" -> suggestSingleValueToken(key, value, PAGE_SUGGESTIONS);
            case "before", "after" -> suggestSingleValueToken(key, value, TIME_SUGGESTIONS);
            default -> List.of();
        };
    }

    private static List<String> subjectSuggestions(String playerName) {
        Set<String> suggestions = new LinkedHashSet<>();
        if (playerName != null && !playerName.isBlank()) {
            suggestions.add(playerName);
        }
        suggestions.add("player");
        suggestions.add("minecraft:zombie");
        return List.copyOf(suggestions);
    }

    private static List<String> suggestSingleValueToken(String key, String value, List<String> candidates) {
        String normalizedValue = value == null ? "" : value;
        return candidates.stream()
            .filter(candidate -> candidate.startsWith(normalizedValue))
            .map(candidate -> key + ":" + candidate)
            .toList();
    }

    private static List<String> suggestVerbToken(String value) {
        String normalizedValue = value == null ? "" : value;
        int commaIndex = normalizedValue.lastIndexOf(',');
        String prefix = commaIndex < 0 ? "" : normalizedValue.substring(0, commaIndex + 1);
        String fragment = commaIndex < 0 ? normalizedValue : normalizedValue.substring(commaIndex + 1);
        return VERB_SUGGESTIONS.stream()
            .filter(candidate -> candidate.startsWith(fragment))
            .map(candidate -> "verb:" + prefix + candidate)
            .toList();
    }

    private static MutableComponent formatEmptyPageMessage(ParsedFilters filters) {
        MutableComponent message = Component.literal("[space-logger] 第 " + filters.page + " 页没有记录")
            .withStyle(ChatFormatting.RED);
        if (filters.page > DEFAULT_PAGE) {
            message.append(Component.literal(" "));
            message.append(pageLink("上一页", filters.commandForPage(filters.page - 1), ChatFormatting.YELLOW));
        }
        return message;
    }

    private static MutableComponent formatPageSummary(
        ParsedFilters filters,
        QueryPagination pagination,
        int pageSize,
        boolean hasNextPage
    ) {
        int startIndex = pagination.offset + 1;
        int endIndex = pagination.offset + pageSize;
        MutableComponent message = Component.literal(
            "[space-logger] 第 " + filters.page + " 页，显示第 " + startIndex + "-" + endIndex + " 条"
        ).withStyle(ChatFormatting.GRAY);
        if (hasNextPage) {
            message.append(Component.literal(" (还有更多)").withStyle(ChatFormatting.DARK_GRAY));
        }
        return message;
    }

    private static MutableComponent formatPaginationControls(ParsedFilters filters, boolean hasNextPage) {
        MutableComponent message = Component.literal("[space-logger] ").withStyle(ChatFormatting.GRAY);
        if (filters.page > DEFAULT_PAGE) {
            message.append(pageLink("上一页", filters.commandForPage(filters.page - 1), ChatFormatting.YELLOW));
        } else {
            message.append(Component.literal("上一页").withStyle(ChatFormatting.DARK_GRAY));
        }
        message.append(Component.literal(" | ").withStyle(ChatFormatting.DARK_GRAY));
        if (hasNextPage) {
            message.append(pageLink("下一页", filters.commandForPage(filters.page + 1), ChatFormatting.GREEN));
        } else {
            message.append(Component.literal("下一页").withStyle(ChatFormatting.DARK_GRAY));
        }
        return message;
    }

    private static MutableComponent pageLink(String label, String command, ChatFormatting color) {
        return Component.literal(label).withStyle(style ->
            style.withColor(color)
                .withHoverEvent(new HoverEvent.ShowText(Component.literal(command)))
                .withClickEvent(new ClickEvent.RunCommand(command))
        );
    }

    private static int parsePositiveInt(String value, String field) throws CommandSyntaxException {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) {
                throw syntax(field + " must be >= 0");
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw syntax(field + " expects an integer value");
        }
    }

    private static int parseVerbMask(String rawValue) throws CommandSyntaxException {
        if (rawValue == null || rawValue.isBlank()) {
            throw syntax("verb filter must not be empty");
        }

        int mask = 0;
        String[] tokens = rawValue.split(",");
        for (String token : tokens) {
            String value = token.trim();
            if (value.isEmpty()) {
                continue;
            }

            int verbId = NativeSpaceLoggerBridge.verbIdFromName(value);
            if (verbId < 0) {
                try {
                    verbId = Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    throw syntax("unknown verb: `" + value + "`");
                }
            }

            if (verbId < 0 || verbId >= 32) {
                throw syntax("verb id out of range: `" + verbId + "`, expected 0..31");
            }
            mask |= NativeSpaceLoggerBridge.verbMaskSingle(verbId);
        }

        if (mask == 0) {
            throw syntax("verb filter produced empty set");
        }
        return mask;
    }

    private static long parseHumanDurationMillis(String value) throws CommandSyntaxException {
        Matcher matcher = HUMAN_DURATION_PATTERN.matcher(value);
        int index = 0;
        long total = 0L;
        boolean matchedAny = false;

        while (matcher.find()) {
            if (matcher.start() != index) {
                throw syntax("invalid time expression: `" + value + "`");
            }

            matchedAny = true;
            long amount;
            try {
                amount = Long.parseLong(matcher.group(1));
            } catch (NumberFormatException e) {
                throw syntax("invalid time amount in `" + value + "`");
            }

            long unitMillis = switch (matcher.group(2).toLowerCase(Locale.ROOT)) {
                case "ms" -> 1L;
                case "s" -> 1_000L;
                case "m" -> 60_000L;
                case "h" -> 3_600_000L;
                case "d" -> 86_400_000L;
                case "w" -> 604_800_000L;
                default -> throw syntax("unsupported time unit in `" + value + "`");
            };

            try {
                total = Math.addExact(total, Math.multiplyExact(amount, unitMillis));
            } catch (ArithmeticException e) {
                throw syntax("time expression overflow: `" + value + "`");
            }

            index = matcher.end();
        }

        if (!matchedAny || index != value.length()) {
            throw syntax("invalid time expression: `" + value + "`; examples: 30m, 3h, 1d12h");
        }
        return total;
    }

    private static int safeAdd(int base, int delta) throws CommandSyntaxException {
        try {
            return Math.addExact(base, delta);
        } catch (ArithmeticException e) {
            throw syntax("range overflows world coordinate bounds");
        }
    }

    private static CommandSyntaxException syntax(String message) {
        return new SimpleCommandExceptionType(Component.literal(message)).create();
    }

    static final class ParsedFilters {
        final String subject;
        final String object;
        final int verbMask;
        final int minX;
        final int maxX;
        final int minY;
        final int maxY;
        final int minZ;
        final int maxZ;
        final long afterTimeMs;
        final long beforeTimeMs;
        final int limit;
        final int page;
        final List<String> pageBaseTokens;

        private ParsedFilters(
            String subject,
            String object,
            int verbMask,
            int minX,
            int maxX,
            int minY,
            int maxY,
            int minZ,
            int maxZ,
            long afterTimeMs,
            long beforeTimeMs,
            int limit,
            int page,
            List<String> pageBaseTokens
        ) {
            this.subject = subject;
            this.object = object;
            this.verbMask = verbMask;
            this.minX = minX;
            this.maxX = maxX;
            this.minY = minY;
            this.maxY = maxY;
            this.minZ = minZ;
            this.maxZ = maxZ;
            this.afterTimeMs = afterTimeMs;
            this.beforeTimeMs = beforeTimeMs;
            this.limit = limit;
            this.page = page;
            this.pageBaseTokens = List.copyOf(pageBaseTokens);
        }

        String commandForPage(int targetPage) {
            List<String> tokens = new ArrayList<>(this.pageBaseTokens);
            if (targetPage > DEFAULT_PAGE) {
                tokens.add("page:" + targetPage);
            }
            if (tokens.isEmpty()) {
                return "/sl q";
            }
            return "/sl q " + String.join(" ", tokens);
        }
    }

    static final class QueryPagination {
        final int offset;
        final int fetchLimit;

        private QueryPagination(int offset, int fetchLimit) {
            this.offset = offset;
            this.fetchLimit = fetchLimit;
        }
    }
}
