package com.github.zly2006.sl.command;

import com.github.zly2006.sl.jni.NativeSpaceLoggerBridge;
import com.mojang.brigadier.arguments.IntegerArgumentType;
import com.mojang.brigadier.arguments.StringArgumentType;
import com.mojang.brigadier.exceptions.CommandSyntaxException;
import com.mojang.brigadier.exceptions.SimpleCommandExceptionType;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.fabricmc.fabric.api.command.v2.CommandRegistrationCallback;
import net.minecraft.ChatFormatting;
import net.minecraft.commands.CommandSourceStack;
import net.minecraft.commands.Commands;
import net.minecraft.core.BlockPos;
import net.minecraft.network.chat.ClickEvent;
import net.minecraft.network.chat.Component;
import net.minecraft.network.chat.HoverEvent;
import net.minecraft.network.chat.MutableComponent;
import net.minecraft.server.level.ServerPlayer;

public final class SpaceLoggerCommand {
    private static final int DEFAULT_LIMIT = 5;
    private static final int MAX_LIMIT = 200;
    private static final Pattern HUMAN_DURATION_PATTERN = Pattern.compile("(\\d+)(ms|s|m|h|d|w)", Pattern.CASE_INSENSITIVE);
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS z");

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
        List<NativeSpaceLoggerBridge.QueryRow> rows = NativeSpaceLoggerBridge.queryRows(
            filters.subject,
            filters.object,
            filters.verb,
            filters.minX,
            filters.maxX,
            filters.minY,
            filters.maxY,
            filters.minZ,
            filters.maxZ,
            filters.afterTimeMs,
            filters.beforeTimeMs,
            filters.limit
        );
        long nowMs = System.currentTimeMillis();

        source.sendSystemMessage(Component.literal("[space-logger] matched_rows=" + rows.size()));
        if (rows.isEmpty()) {
            source.sendSystemMessage(Component.literal("[space-logger] no rows matched this query"));
            return 1;
        }

        int index = 1;
        for (NativeSpaceLoggerBridge.QueryRow row : rows) {
            source.sendSystemMessage(formatQueryRowLine(index, row, nowMs));
            index += 1;
        }

        return rows.size();
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
        line.append(Component.literal(row.verb()).withStyle(ChatFormatting.GOLD));
        line.append(Component.literal(" "));
        line.append(Component.literal(coordText).withStyle(style ->
            style.withColor(ChatFormatting.GREEN)
                .withUnderlined(true)
                .withHoverEvent(new HoverEvent.ShowText(Component.literal("点击可传送")))
                .withClickEvent(new ClickEvent.RunCommand(tpCommand))
        ));
        line.append(Component.literal(" "));
        line.append(Component.literal("(+" + row.dataLen() + ")").withStyle(style ->
            style.withColor(ChatFormatting.DARK_GRAY)
                .withHoverEvent(new HoverEvent.ShowText(Component.literal("nbt数据大小: " + row.dataLen() + " bytes")))
        ));
        return line;
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

    private static ParsedFilters parseFilters(String rawFilterText, ServerPlayer player) throws CommandSyntaxException {
        String[] tokens = rawFilterText == null || rawFilterText.isBlank()
            ? new String[0]
            : rawFilterText.trim().split("\\s+");

        String subject = "";
        String object = "";
        String verb = "";
        Integer range = null;
        Integer limit = null;
        Long afterTimeMs = null;
        Long beforeTimeMs = null;
        long nowMs = System.currentTimeMillis();

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
                case "verb" -> verb = value;
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
            verb,
            minX,
            maxX,
            minY,
            maxY,
            minZ,
            maxZ,
            minTime,
            maxTime,
            limit == null ? DEFAULT_LIMIT : limit
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

    private static final class ParsedFilters {
        private final String subject;
        private final String object;
        private final String verb;
        private final int minX;
        private final int maxX;
        private final int minY;
        private final int maxY;
        private final int minZ;
        private final int maxZ;
        private final long afterTimeMs;
        private final long beforeTimeMs;
        private final int limit;

        private ParsedFilters(
            String subject,
            String object,
            String verb,
            int minX,
            int maxX,
            int minY,
            int maxY,
            int minZ,
            int maxZ,
            long afterTimeMs,
            long beforeTimeMs,
            int limit
        ) {
            this.subject = subject;
            this.object = object;
            this.verb = verb;
            this.minX = minX;
            this.maxX = maxX;
            this.minY = minY;
            this.maxY = maxY;
            this.minZ = minZ;
            this.maxZ = maxZ;
            this.afterTimeMs = afterTimeMs;
            this.beforeTimeMs = beforeTimeMs;
            this.limit = limit;
        }
    }
}
