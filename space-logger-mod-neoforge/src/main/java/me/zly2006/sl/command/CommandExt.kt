package me.zly2006.sl.command

import com.mojang.brigadier.CommandDispatcher
import com.mojang.brigadier.arguments.ArgumentType
import com.mojang.brigadier.builder.ArgumentBuilder
import com.mojang.brigadier.builder.LiteralArgumentBuilder
import com.mojang.brigadier.builder.RequiredArgumentBuilder
import com.mojang.brigadier.context.CommandContext
import com.mojang.brigadier.suggestion.SuggestionProvider
import net.minecraft.commands.CommandSourceStack
import net.minecraft.commands.Commands

class CommandBuilderWrapper<T : ArgumentBuilder<CommandSourceStack, T>>(
    val builder: T,
) {
    fun literal(
        name: String,
        builderAction: CommandBuilderWrapper<LiteralArgumentBuilder<CommandSourceStack>>.() -> Unit = {},
    ) {
        val literalBuilder = Commands.literal(name)
        CommandBuilderWrapper(literalBuilder).builderAction()
        builder.then(literalBuilder)
    }

    fun <V : Any> argument(
        name: String,
        type: ArgumentType<V>,
        builderAction: CommandBuilderWrapper<RequiredArgumentBuilder<CommandSourceStack, V>>.() -> Unit = {},
    ) {
        val argumentBuilder = Commands.argument(name, type)
        CommandBuilderWrapper(argumentBuilder).builderAction()
        builder.then(argumentBuilder)
    }

    fun executes(executor: (CommandContext<CommandSourceStack>) -> Unit) {
        builder.executes { ctx ->
            executor(ctx)
            1
        }
    }

    fun executeReturns(executor: (CommandContext<CommandSourceStack>) -> Int) {
        builder.executes(executor)
    }
}

fun <V : Any> CommandBuilderWrapper<RequiredArgumentBuilder<CommandSourceStack, V>>.suggests(
    provider: SuggestionProvider<CommandSourceStack>,
) {
    builder.suggests(provider)
}

fun CommandDispatcher<CommandSourceStack>.register(
    literal: String,
    builderAction: CommandBuilderWrapper<LiteralArgumentBuilder<CommandSourceStack>>.() -> Unit,
) {
    val root = Commands.literal(literal)
    CommandBuilderWrapper(root).builderAction()
    register(root)
}
