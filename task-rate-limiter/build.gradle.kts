plugins {
    id("buildsrc.convention.kotlin-jvm")
}

dependencies {
    implementation(libs.kotlinxCoroutines)
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
}
