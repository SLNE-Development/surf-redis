plugins {
    id("dev.slne.surf.surfapi.gradle.paper-plugin")
}

surfPaperPluginApi {
    mainClass("dev.slne.surf.redis.test.paper.PaperMain")
    authors.add("red")

    generateLibraryLoader(false)
}
dependencies {
    api(project(":surf-redis-api"))
}