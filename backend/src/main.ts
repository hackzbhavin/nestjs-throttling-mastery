import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { Logger } from '@nestjs/common';

async function bootstrap() {
  const app    = await NestFactory.create(AppModule);
  const logger = new Logger('Bootstrap');

  // Trust proxy headers (needed if behind nginx / load balancer)
  app.set?.('trust proxy', 1);

  // Global prefix — all routes under /api
  app.setGlobalPrefix('api');

  // Graceful shutdown
  app.enableShutdownHooks();

  const port = process.env.PORT ?? 3000;
  await app.listen(port);
  logger.log(`NestJS Throttling Mastery running on :${port}`);
  logger.log(`Node count: ${process.env.THROTTLE_NODE_COUNT ?? 2}`);
  logger.log(`Global limit: ${process.env.THROTTLE_GLOBAL_LIMIT ?? 100} req/min`);
  logger.log(`Peer nodes: ${process.env.THROTTLE_PEER_URLS ?? 'none (single node)'}`);
}

bootstrap();
