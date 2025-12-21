plugins {
    id("dev.slne.surf.surfapi.gradle.core")
}

dependencies {
    implementation("io.lettuce:lettuce-core:6.3.0.RELEASE")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.10.1")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.7.3")
}

tasks.test {
    useJUnitPlatform()
}