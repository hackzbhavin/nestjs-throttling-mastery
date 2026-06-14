import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { RedisModule } from '@nestjs-modules/ioredis';
import { ThrottleModule } from './throttle/throttle.module';
import { ThrottleEntity } from './throttle/entities/throttle-fallback.entity';
import { DemoController } from './demo/demo.controller';

/**
 * @architecture Root application module.
 *
 * Wiring:
 *   ConfigModule   — env vars (.env)
 *   RedisModule    — ioredis singleton, injected via @InjectRedis()
 *   TypeOrmModule  — MySQL connection, entity registration
 *   ThrottleModule — @Global(), available everywhere without re-importing
 *   DemoController — shows throttle in action via HTTP endpoints
 */
@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),

    RedisModule.forRootAsync({
      useFactory: (config: ConfigService) => ({
        type: 'single',
        url: config.get<string>('REDIS_URL', 'redis://localhost:6379'),
      }),
      inject: [ConfigService],
    }),

    TypeOrmModule.forRootAsync({
      useFactory: (config: ConfigService) => ({
        type: 'mysql',
        host:     config.get('DB_HOST',     'localhost'),
        port:     config.get<number>('DB_PORT', 3306),
        username: config.get('DB_USER',     'root'),
        password: config.get('DB_PASSWORD', 'password'),
        database: config.get('DB_NAME',     'throttle_db'),
        entities:        [ThrottleEntity],
        synchronize:     true,  // set false in production — use migrations
        logging:         false,
      }),
      inject: [ConfigService],
    }),

    ThrottleModule,
  ],
  controllers: [DemoController],
})
export class AppModule {}
