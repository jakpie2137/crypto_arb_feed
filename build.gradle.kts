plugins {
    // keep root minimal
}

allprojects {
    group = "local.crypto_arb_feed"
    version = "0.1.0"

    repositories {
        mavenCentral()
    }
}

ext {
    set("okhttpVersion", "4.12.0")
    set("jacksonVersion", "2.20.1")
    set("protobufVersion", "4.33.2")
    set("grpcVersion", "1.68.1")
    set("postgresVersion", "42.7.7")
    set("hikariVersion", "5.1.0")
    set("junitBomVersion", "5.10.0")
}
