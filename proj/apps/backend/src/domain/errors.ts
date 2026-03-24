export type AppError = {
  code: string;
  message: string;
  statusCode: number;
  details?: unknown;
};

export const createAppError = (
  code: string,
  message: string,
  statusCode: number,
  details?: unknown
): AppError => ({
  code,
  message,
  statusCode,
  details
});

export const isAppError = (value: unknown): value is AppError => {
  if (typeof value !== 'object' || value === null) {
    return false;
  }

  const candidate = value as Partial<AppError>;
  return (
    typeof candidate.code === 'string' &&
    typeof candidate.message === 'string' &&
    typeof candidate.statusCode === 'number'
  );
};
