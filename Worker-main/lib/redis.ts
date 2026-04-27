import IORedis from 'ioredis';
import type { RedisOptions } from "ioredis";

/* eslint-disable no-var */
declare global {
  var redis: IORedis | undefined;
}
/* eslint-enable no-var */

const getRedisUrl = () => {
  const rawUrl = process.env.REDIS_URL || "redis://localhost:6379";

  if (!rawUrl.startsWith("redis-cli")) {
    return rawUrl;
  }

  const match = rawUrl.match(/-u\s+(\S+)/);
  const url = match?.[1] || "redis://localhost:6379";

  return rawUrl.includes("--tls") ? url.replace(/^redis:\/\//, "rediss://") : url;
};

const redisUrl = getRedisUrl();

const getRedisConnectionOptions = (): RedisOptions => {
  const parsed = new URL(redisUrl);
  const isTls = parsed.protocol === "rediss:";

  return {
    host: parsed.hostname,
    port: Number(parsed.port || (isTls ? 6379 : 6379)),
    password: parsed.password || undefined,
    username: parsed.username || undefined,
    maxRetriesPerRequest: null,
    tls: isTls ? {} : undefined,
  };
};

export const redisConnection = getRedisConnectionOptions();

export const getRedis = () => {
  if (!global.redis) {
    global.redis = new IORedis(redisConnection);
  }

  return global.redis;
};
