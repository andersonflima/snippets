import 'dotenv/config';

const toPositiveInt = (input: string | undefined, fallbackValue: number): number => {
  const parsedValue = Number(input);
  return Number.isInteger(parsedValue) && parsedValue > 0 ? parsedValue : fallbackValue;
};

const required = (input: string | undefined, fieldName: string): string => {
  if (!input) {
    throw new Error(`Missing required environment variable: ${fieldName}`);
  }

  return input;
};

const toBoolean = (input: string | undefined): boolean =>
  typeof input === 'string' && ['1', 'true', 'yes', 'on'].includes(input.toLowerCase());

const optional = (input: string | undefined): string | undefined => {
  const normalized = input?.trim();
  return normalized && normalized.length > 0 ? normalized : undefined;
};

const toAssumeRoleArnTemplate = (input: string | undefined): string | undefined => {
  const template = optional(input);

  if (!template) {
    return undefined;
  }

  if (!template.includes('{account_id}')) {
    throw new Error(
      'Invalid AWS_ASSUME_ROLE_ARN_TEMPLATE: expected placeholder {account_id}.'
    );
  }

  return template;
};

const awsAccessKeyId = optional(process.env.AWS_ACCESS_KEY_ID);
const awsSecretAccessKey = optional(process.env.AWS_SECRET_ACCESS_KEY);
const awsSessionToken = optional(process.env.AWS_SESSION_TOKEN);

export const env = Object.freeze({
  port: toPositiveInt(process.env.PORT, 3000),
  jwtSecret: required(process.env.JWT_SECRET, 'JWT_SECRET'),
  awsExternalId: optional(process.env.AWS_EXTERNAL_ID),
  awsAssumeRoleArnTemplate: toAssumeRoleArnTemplate(process.env.AWS_ASSUME_ROLE_ARN_TEMPLATE),
  awsAccessKeyId,
  awsSecretAccessKey,
  awsSessionToken,
  awsTlsInsecure: toBoolean(process.env.AWS_TLS_INSECURE),
  databaseUrl: required(process.env.DATABASE_URL, 'DATABASE_URL'),
  databaseSsl: toBoolean(process.env.DATABASE_SSL)
});
