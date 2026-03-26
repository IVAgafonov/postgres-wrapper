package io.github.ivagafonov.postgres.config

case class PostgresConfig(
                           host: String,
                           user: String,
                           password: String,
                           database: String,
                           port: Int = 5432,
                           poolInitialSize: Int = 1,
                           poolMaxSize: Int = 10,
                           maxWaitMillis: Int = 10000
                         ) {}
