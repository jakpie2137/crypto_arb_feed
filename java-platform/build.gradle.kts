plugins {
    java
    application
    id("com.google.protobuf") version "0.9.5"
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

dependencies {
    val okhttpVersion: String by rootProject.extra
    val jacksonVersion: String by rootProject.extra
    val protobufVersion: String by rootProject.extra
    val postgresVersion: String by rootProject.extra
    val hikariVersion: String by rootProject.extra
    val junitBomVersion: String by rootProject.extra
    val grpcVersion: String by rootProject.extra

    implementation("com.squareup.okhttp3:okhttp:$okhttpVersion")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:${jacksonVersion}")

    implementation("com.google.protobuf:protobuf-java:${protobufVersion}")
    implementation("io.grpc:grpc-protobuf:${grpcVersion}")
    implementation("io.grpc:grpc-stub:${grpcVersion}")
    runtimeOnly("io.grpc:grpc-netty-shaded:${grpcVersion}")
    compileOnly("javax.annotation:javax.annotation-api:1.3.2")

    implementation("org.postgresql:postgresql:$postgresVersion")
    implementation("com.zaxxer:HikariCP:$hikariVersion")
    implementation("org.java-websocket:Java-WebSocket:1.5.4")
    implementation("ch.qos.logback:logback-classic:1.4.14")

    testImplementation(platform("org.junit:junit-bom:$junitBomVersion"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")

    compileOnly("org.projectlombok:lombok:1.18.30")
    annotationProcessor("org.projectlombok:lombok:1.18.30")
}

application {
    mainClass.set("feed_filter.FeedFilterApp")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${rootProject.extra["protobufVersion"]}"
    }
    plugins {
        create("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${rootProject.extra["grpcVersion"]}"
        }
    }
    generateProtoTasks {
        all().forEach { task ->
            task.plugins {
                create("grpc") { }
            }
        }
    }
}

val sourceSets = extensions.getByType<org.gradle.api.tasks.SourceSetContainer>()

tasks.register<JavaExec>("runFeedHeadless") {
    group = "application"
    description = "Feed headless (gRPC + optional detectors)"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("feed_filter.FeedFilterApp")
    environment("OB_GUI", "false")
    environment("FEED_GRPC_PORT", "50050")
    standardInput = System.`in`
}

tasks.register<JavaExec>("runFeedProxy") {
    group = "application"
    description = "Feed Proxy (gRPC fan-out + HTTP /orderbook)"
    classpath = sourceSets["main"].runtimeClasspath
    mainClass.set("feed_proxy.FeedProxyApp")
    environment("FEED_HOST", "localhost")
    environment("FEED_GRPC_PORT", "50050")
    environment("FEED_PROXY_GRPC_PORT", "50051")
    environment("FEED_PROXY_HTTP_PORT", "8082")
    standardInput = System.`in`
}

tasks.test {
    useJUnitPlatform()
}
