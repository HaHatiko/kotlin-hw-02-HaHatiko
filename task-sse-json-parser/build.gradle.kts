plugins {
    id("buildsrc.convention.kotlin-jvm")
}

dependencies {
    implementation(libs.kotlinxCoroutines)
    implementation(libs.kotlinxSerialization)
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
}

tasks.withType<Test>().configureEach {
    systemProperty("kotlinx.coroutines.stacktrace.recovery", "false")
}
