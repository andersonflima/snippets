import { createHash, timingSafeEqual } from 'node:crypto';

const toSha256 = (rawValue: string): string => createHash('sha256').update(rawValue).digest('hex');

const safeCompare = (left: string, right: string): boolean => {
  const leftBuffer = Buffer.from(left);
  const rightBuffer = Buffer.from(right);

  if (leftBuffer.length !== rightBuffer.length) {
    return false;
  }

  return timingSafeEqual(leftBuffer, rightBuffer);
};

export const hashPassword = (rawPassword: string): string => toSha256(rawPassword);

export const verifyPassword = (rawPassword: string, hashedPassword: string): boolean =>
  safeCompare(toSha256(rawPassword), hashedPassword);
