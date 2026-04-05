import org.jetbrains.kotlin.gradle.dsl.JvmTarget
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.gradle.api.GradleException

plugins {
    kotlin("jvm") version "2.3.10"
    id("net.fabricmc.fabric-loom") version "1.15.5"
    id("maven-publish")
}

version = project.property("mod_version") as String
group = project.property("maven_group") as String

base {
    archivesName.set(project.property("archives_base_name") as String)
}

val targetJavaVersion = 25
java {
    toolchain.languageVersion = JavaLanguageVersion.of(targetJavaVersion)
    // Loom will automatically attach sourcesJar to a RemapSourcesJar task and to the "build" task
    // if it is present.
    // If you remove this line, sources will not be generated.
    withSourcesJar()
}

loom {
    splitEnvironmentSourceSets()

    mods {
        register("space-logger-mod") {
            sourceSet("main")
            sourceSet("client")
        }
    }
}


repositories {
    // Add repositories to retrieve artifacts from in here.
    // You should only use this when depending on other mods because
    // Loom adds the essential maven repositories to download Minecraft and libraries from automatically.
    // See https://docs.gradle.org/current/userguide/declaring_repositories.html
    // for more information about repositories.
}

dependencies {
    // To change the versions see the gradle.properties file
    minecraft("com.mojang:minecraft:${project.property("minecraft_version")}")
    implementation("net.fabricmc:fabric-loader:${project.property("loader_version")}")
    implementation("net.fabricmc:fabric-language-kotlin:${project.property("kotlin_loader_version")}")

    implementation("net.fabricmc.fabric-api:fabric-api:${project.property("fabric_version")}")

    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

val nativeProfile = providers.gradleProperty("spaceLoggerNativeProfile")
    .map { profile -> if (profile.equals("release", ignoreCase = true)) "release" else "debug" }
    .orElse("debug")
val cargoExecutable = run {
    val fromEnv = System.getenv("CARGO")?.takeIf { it.isNotBlank() }
        ?: System.getenv("CARGO_BIN")?.takeIf { it.isNotBlank() }
    if (fromEnv != null) {
        fromEnv
    } else {
        val home = System.getenv("HOME")
        val cargoFromHome = if (home.isNullOrBlank()) null else file("$home/.cargo/bin/cargo")
        if (cargoFromHome != null && cargoFromHome.exists()) cargoFromHome.absolutePath else "cargo"
    }
}
val nativeLibName = System.mapLibraryName("space_logger_native")
val nativeLibPath = nativeProfile.map { profile ->
    file("native-logger/target/$profile/$nativeLibName")
}
val cargoBuildNative by tasks.registering(Exec::class) {
    group = "build"
    description = "Build Rust JNI library for space-logger-mod."
    workingDir = file("native-logger")
    val profile = nativeProfile.get()
    val args = mutableListOf("build")
    if (profile == "release") {
        args.add("--release")
    }
    commandLine(cargoExecutable, *args.toTypedArray())
    val home = System.getenv("HOME")
    if (!home.isNullOrBlank()) {
        val currentPath = System.getenv("PATH") ?: ""
        environment("PATH", "$home/.cargo/bin:$currentPath")
    }

    inputs.files(
        file("native-logger/Cargo.toml"),
        file("native-logger/Cargo.lock"),
        file("../Cargo.toml"),
        file("../Cargo.lock"),
    )
    inputs.dir(file("native-logger/src"))
    inputs.dir(file("../src"))
    outputs.upToDateWhen { false }
}

tasks.processResources {
    dependsOn(cargoBuildNative)
    inputs.property("version", project.version)
    inputs.property("minecraft_version", project.property("minecraft_version"))
    inputs.property("loader_version", project.property("loader_version"))
    filteringCharset = "UTF-8"

    filesMatching("fabric.mod.json") {
        expand(
            "version" to project.version,
        )
    }
}

tasks.withType<JavaCompile>().configureEach {
    dependsOn(cargoBuildNative)
    // ensure that the encoding is set to UTF-8, no matter what the system default is
    // this fixes some edge cases with special characters not displaying correctly
    // see http://yodaconditions.net/blog/fix-for-java-file-encoding-problems-with-gradle.html
    // If Javadoc is generated, this must be specified in that task too.
    options.encoding = "UTF-8"
    options.release.set(targetJavaVersion)
}

tasks.withType<KotlinCompile>().configureEach {
    dependsOn(cargoBuildNative)
    compilerOptions.jvmTarget.set(JvmTarget.fromTarget(targetJavaVersion.toString()))
}

tasks.withType<JavaExec>().configureEach {
    dependsOn(cargoBuildNative)
    doFirst {
        val nativeLib = nativeLibPath.get().absoluteFile
        if (!nativeLib.exists()) {
            throw GradleException(
                "Native library not found after cargo build: ${nativeLib.absolutePath}"
            )
        }
        systemProperty("space_logger_native_lib", nativeLib.absolutePath)
    }
}

tasks.test {
    useJUnitPlatform()
}

tasks.jar {
    from("LICENSE") {
        rename { "${it}_${project.base.archivesName.get()}" }
    }
}

// configure the maven publication
publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = project.property("archives_base_name") as String
            from(components["java"])
        }
    }

    // See https://docs.gradle.org/current/userguide/publishing_maven.html for information on how to set up publishing.
    repositories {
        // Add repositories to publish to here.
        // Notice: This block does NOT have the same function as the block in the top level.
        // The repositories here will be used for publishing your artifact, not for
        // retrieving dependencies.
    }
}
