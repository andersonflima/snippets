import { createApp } from './app.js';
import { env } from './config/env.js';

const start = async () => {
  const app = await createApp();

  await app.listen({
    host: '0.0.0.0',
    port: env.port
  });
};

start().catch((error) => {
  // eslint-disable-next-line no-console
  console.error(error);
  process.exit(1);
});
