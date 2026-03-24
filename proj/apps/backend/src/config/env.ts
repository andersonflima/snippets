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

const toAssumeRoleArnTemplate = (input: string | undefined): string => {
  const template = required(input, 'AWS_ASSUME_ROLE_ARN_TEMPLATE').trim();
  if (!template.includes('{account_id}')) {
    throw new Error(
      'Invalid AWS_ASSUME_ROLE_ARN_TEMPLATE: expected placeholder {account_id}.'
    );
  }

  return template;
};

export const env = Object.freeze({
  port: toPositiveInt(process.env.PORT, 3000),
  jwtSecret: required(process.env.JWT_SECRET, 'JWT_SECRET'),
  awsExternalId: process.env.AWS_EXTERNAL_ID,
  awsAssumeRoleArnTemplate: toAssumeRoleArnTemplate(process.env.AWS_ASSUME_ROLE_ARN_TEMPLATE),
  databaseUrl: required(process.env.DATABASE_URL, 'DATABASE_URL'),
  databaseSsl: toBoolean(process.env.DATABASE_SSL)
});
