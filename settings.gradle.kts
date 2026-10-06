dependencyResolutionManagement {
    @Suppress("UnstableApiUsage")
    repositories {
        mavenCentral()
    }
}

plugins {
    // Use the Foojay Toolchains plugin to automatically download JDKs required by subprojects.
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}

include(":task-rate-limiter")
include(":task-voice-assistant-pipeline")
include(":task-sse-json-parser")

rootProject.name = "hw-02-coroutines"
