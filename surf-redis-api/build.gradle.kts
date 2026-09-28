@file:OptIn(ExperimentalAbiValidation::class)

import dev.slne.surf.api.gradle.util.slneReleases
import org.jetbrains.dokka.gradle.tasks.DokkaGenerateTask
import org.jetbrains.kotlin.gradle.dsl.abi.ExperimentalAbiValidation
import java.security.MessageDigest
import java.util.HexFormat
import java.util.zip.ZipFile

plugins {
    id("dev.slne.surf.api.gradle.core")
    id("org.jetbrains.dokka-javadoc") version "2.2.0"
}

surfCoreApi {
    withApiValidation()
}

kotlin {
    abiValidation {
        filters {
            exclude {
                annotatedWith.add("dev.slne.surf.redis.util.InternalRedisAPI")
                byNames.add("org.redisson.**")
            }
        }
    }
}

dokka {
    dokkaSourceSets.configureEach {
        perPackageOption {
            matchingRegex = """org\.redisson(\..*)?"""
            suppress = true
        }
    }
}

@Suppress("UNCHECKED_CAST")
val shadedPackages = rootProject.extra["shadedPackages"] as Map<String, String>

@Suppress("UNCHECKED_CAST")
val patchedRedissonClasses = rootProject.extra["patchedRedissonClasses"] as Map<String, Pair<String, String>>

val redissonPackagePath = "org/redisson"
val redissonRelocationPath = rootProject.extra["redissonRelocationPath"] as String
val redissonPatchSources = fileTree("src/main/java") { include("$redissonPackagePath/**") }
val relocationExtraDepth =
    redissonRelocationPath.count { it == '/' } - redissonPackagePath.count { it == '/' }

val shadedPackageRewrites = shadedPackages
    .flatMap { (from, to) ->
        listOf(from to to, from.replace('.', '/') to to.replace('.', '/'))
    }
    .distinctBy { it.first }

fun String.applyShadedPackages(): String {
    var rewritten = this
    for ((from, to) in shadedPackageRewrites) {
        if (rewritten.contains(from)) rewritten = rewritten.replace(from, to)
    }
    return rewritten
}

fun redissonArtifact(configurationName: String, classifier: String): Configuration {
    val configuration = configurations.create(configurationName) {
        isCanBeConsumed = false
        isCanBeResolved = true
        isTransitive = false
    }

    dependencies {
        add(configurationName, variantOf(libs.redisson) { classifier(classifier) })
    }

    return configuration
}

fun registerRedissonRelocation(
    taskName: String,
    source: Configuration,
    outputDirectory: String,
    textFileSuffix: String,
    rewriteLinkDepth: Boolean,
    patchedFiles: FileTree? = null,
) = tasks.register<Sync>(taskName) {
    description = "Rewrites Redisson's $textFileSuffix files onto the shaded package names."
    group = JavaBasePlugin.DOCUMENTATION_GROUP

    into(layout.buildDirectory.dir(outputDirectory))
    includeEmptyDirs = false
    filteringCharset = Charsets.UTF_8.name()

    inputs.property("shadedPackageRewrites", shadedPackageRewrites.toString())
    inputs.property("rewriteLinkDepth", rewriteLinkDepth)

    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    from(source.elements.map { jars -> listOfNotNull(patchedFiles) + jars.map(::zipTree) }) {
        include("$redissonPackagePath/**")

        eachFile {
            val depth = relativePath.segments.size - 1

            if (name.endsWith(textFileSuffix)) {
                filter { line ->
                    val relinked = if (rewriteLinkDepth) {
                        line.replace(
                            "../".repeat(depth),
                            "../".repeat(depth + relocationExtraDepth),
                        )
                    } else {
                        line
                    }

                    relinked.applyShadedPackages()
                }
            }

            path = redissonRelocationPath + path.removePrefix(redissonPackagePath)
        }
    }
}

dependencies {
    api(libs.redisson) {
        exclude("org.slf4j")
        exclude("org.reactivestreams")
        exclude("io.projectreactor", "reactor-core")
    }

    testImplementation(kotlin("test-junit5"))
}

val relocateRedissonJavadoc = registerRedissonRelocation(
    taskName = "relocateRedissonJavadoc",
    source = redissonArtifact("redissonJavadoc", "javadoc"),
    outputDirectory = "docs/redisson-javadoc",
    textFileSuffix = ".html",
    rewriteLinkDepth = true,
)

val redissonSources = redissonArtifact("redissonSources", "sources")

val relocateRedissonSources = registerRedissonRelocation(
    taskName = "relocateRedissonSources",
    source = redissonSources,
    outputDirectory = "docs/redisson-sources",
    textFileSuffix = ".java",
    rewriteLinkDepth = false,
    patchedFiles = redissonPatchSources,
)

val verifyRedissonPatches = tasks.register("verifyRedissonPatches") {
    description = "Fails when Redisson's upstream source of a patched class no longer matches its recorded hash."
    group = LifecycleBasePlugin.VERIFICATION_GROUP

    val sourceJars: FileCollection = redissonSources
    val expectedHashes = patchedRedissonClasses.entries.associate { (path, patch) -> "$path.java" to patch.second }

    doLast {
        ZipFile(sourceJars.singleFile).use { zip ->
            for ((path, expected) in expectedHashes) {
                val entry = checkNotNull(zip.getEntry(path)) { "Redisson no longer ships $path" }
                val bytes = zip.getInputStream(entry).use { it.readBytes() }
                val actual = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes))
                check(actual == expected) {
                    "Redisson's $path changed (SHA-256 $actual). Re-apply the surf-redis patch in " +
                        "src/main/java/$path onto the new upstream source, then update its hash in the root build.gradle.kts."
                }
            }
        }
    }
}

tasks.check {
    dependsOn(verifyRedissonPatches)
}

tasks.javadoc {
    setSource(files())
    setDestinationDir(layout.buildDirectory.dir("docs/javadoc-unused").get().asFile)
}

tasks.named<Jar>("javadocJar") {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    from(tasks.named<DokkaGenerateTask>("dokkaGeneratePublicationJavadoc").flatMap { it.outputDirectory })
    from(relocateRedissonJavadoc)
}

tasks.named<Jar>("sourcesJar") {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE

    exclude("$redissonPackagePath/**")

    from(relocateRedissonSources)
}

java {
    withSourcesJar()
    withJavadocJar()
}

val shadowComponent = components["shadow"] as AdhocComponentWithVariants
shadowComponent.addVariantsFromConfiguration(configurations["sourcesElements"]) {}
shadowComponent.addVariantsFromConfiguration(configurations["javadocElements"]) {}

publishing {
    publications {
        create<MavenPublication>("shadow") {
            from(components["shadow"])
        }
    }

    repositories {
        slneReleases()
    }
}

tasks.test {
    useJUnitPlatform()
    failOnNoDiscoveredTests = false
}
