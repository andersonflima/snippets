import { CommonModule } from '@angular/common';
import { Component, computed, effect, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import type {
  AwsAccount,
  AwsCategory,
  FinOpsOverview,
  PermissionScope,
  ResourceStateRecord,
  ResourceSummary,
  ResourceTemplate,
  ResourceUpdateProfile,
  UserRole
} from '@platform/shared';
import {
  getDefaultResourceUpdateProfile,
  getResourceTemplates,
  getResourceUpdateProfile,
  getResourceUpdateProfiles
} from '@platform/shared';

type CategoryDefinition = {
  id: AwsCategory;
  label: string;
};

type AwsAccountFormRow = {
  accountId: string;
  name: string;
  allowedRegions: readonly string[];
};

type ResourceAction = 'list' | 'get' | 'create' | 'update' | 'delete';
type ResourceOperationTab = 'create' | 'update';

type PermissionFormRow = {
  accountId: string;
  category: AwsCategory | '*';
  resourceType: string;
  action: ResourceAction;
};

type ResourceFieldValueMode = 'text' | 'number' | 'boolean' | 'json' | 'null';

type ResourceFieldRow = {
  id: string;
  key: string;
  value: string;
  valueMode: ResourceFieldValueMode;
};

type TemplateAwareResourceFieldRow = ResourceFieldRow & {
  label?: string;
  enumValues?: readonly string[];
  description?: string;
  placeholder?: string;
  kind?: string;
  required: boolean;
  fieldType: 'template' | 'custom';
};

type ResourceTagRow = {
  id: string;
  key: string;
  value: string;
};

type StructuredUpdateFieldKind =
  | 's3-versioning'
  | 's3-public-access'
  | 's3-encryption'
  | 'dynamodb-throughput';

type ResourceStateHistoryResponse = {
  history: readonly ResourceStateRecord[];
};

type PatchOperation = 'add' | 'remove' | 'replace' | 'move' | 'copy' | 'test';

type ResourcePatchRow = {
  op: PatchOperation;
  path: string;
  value: string;
  valueMode: ResourceFieldValueMode;
  from: string;
};

type PublicUser = {
  id: string;
  name: string;
  email: string;
  role: UserRole;
  accounts: readonly AwsAccount[];
};

type LoginResponse = {
  token: string;
  user: PublicUser;
};

type FinOpsOverviewResponse = FinOpsOverview;

type ContextSwitchResponse = {
  context: {
    accountId: string;
    region: string;
    category: AwsCategory;
  };
  resourceTypes: readonly string[];
  checkup: {
    resourceCounts: Record<string, number>;
  };
  checkupWarning?: string;
};

type ResourceListResponse = {
  resources: readonly ResourceSummary[];
};

type ResourceDetailsResponse = {
  identifier: string;
  typeName: string;
  properties: Record<string, unknown>;
  platformState?: ResourceStateRecord | null;
};

type DeleteIntentResponse = {
  intentId: string;
  expiresAt: number;
};

type AdminUser = {
  id: string;
  name: string;
  email: string;
  role: UserRole;
  accounts: readonly AwsAccount[];
  permissions: readonly PermissionScope[];
};

type AdminUsersResponse = {
  users: readonly AdminUser[];
};

type AdminUserResponse = {
  user: AdminUser;
};

type PermissionResponse = {
  permissions: readonly PermissionScope[];
};

type AuthMode = 'login' | 'register';
type ToastKind = 'error' | 'info';
type WorkspaceView = 'resources' | 'finops' | 'admin';

type ContextSelectionSnapshot = {
  category: AwsCategory;
  accountId: string;
  region: string;
};

type FinOpsResourceTypeSummary = {
  typeName: string;
  total: number;
  obsolete: number;
  percent: number;
};

type CheckupCardView = {
  typeName: string;
  serviceLabel: string;
  resourceLabel: string;
  total: number;
  fillPercentage: number;
};

type ToastItem = {
  id: string;
  kind: ToastKind;
  message: string;
};

type ResourceStateHistoryScope = 'resource' | 'type' | 'context';

const TOKEN_STORAGE_KEY = 'platform.token';
const HEADER_SELECTION_STORAGE_KEY = 'platform.resource-header-selection';
const API_BASE_URL = 'http://localhost:3000';
const validActions = ['list', 'get', 'create', 'update', 'delete'] as const;
const validRoles = ['admin', 'operator', 'viewer'] as const;

const categoryDefinitions: readonly CategoryDefinition[] = [
  { id: 'compute', label: 'Compute' },
  { id: 'storage', label: 'Storage' },
  { id: 'database', label: 'Database' },
  { id: 'network', label: 'Network' },
  { id: 'security', label: 'Security' },
  { id: 'management', label: 'Management' }
];

const defaultResourceTemplates = getResourceTemplates();
const topAwsResourceTypes: readonly string[] = defaultResourceTemplates.map((template) => template.typeName);
const getDefaultResourceTypesForCategory = (category: AwsCategory): readonly string[] =>
  defaultResourceTemplates
    .filter((template) => template.category === category)
    .map((template) => template.typeName);
const defaultComputeResourceTypes = getDefaultResourceTypesForCategory('compute');

const awsCommonRegions: readonly string[] = [
  'af-south-1',
  'ap-east-1',
  'ap-northeast-1',
  'ap-northeast-2',
  'ap-south-1',
  'ap-southeast-1',
  'ap-southeast-2',
  'ca-central-1',
  'eu-central-1',
  'eu-north-1',
  'eu-west-1',
  'eu-west-2',
  'eu-west-3',
  'sa-east-1',
  'us-east-1',
  'us-east-2',
  'us-west-1',
  'us-west-2'
];

const permissionCategoryOptions: readonly (AwsCategory | '*')[] = [
  'compute',
  'storage',
  'database',
  'network',
  'security',
  'management',
  '*'
];

const patchOperationOptions: readonly PatchOperation[] = ['add', 'remove', 'replace', 'move', 'copy', 'test'];

const resourceFieldValueModes: readonly ResourceFieldValueMode[] = ['text', 'number', 'boolean', 'json', 'null'];
const createToastId = (): string => `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
const awsTokenLabels: Record<string, string> = {
  CloudFormation: 'CloudFormation',
  CloudWatch: 'CloudWatch',
  DynamoDB: 'DynamoDB',
  EC2: 'EC2',
  ECS: 'ECS',
  EFS: 'EFS',
  Events: 'Events',
  FSx: 'FSx',
  IAM: 'IAM',
  KMS: 'KMS',
  Lambda: 'Lambda',
  RDS: 'RDS',
  SecretsManager: 'Secrets Manager'
};

const humanizeAwsToken = (value: string): string =>
  awsTokenLabels[value] ??
  value
    .replace(/([A-Z]+)([A-Z][a-z])/g, '$1 $2')
    .replace(/([a-z0-9])([A-Z])/g, '$1 $2');

const DEFAULT_FINOPS_STALE_DAYS = 30;
const toFinitePositiveInt = (value: number | undefined, fallback: number): number => {
  if (typeof value !== 'number' || !Number.isFinite(value) || !Number.isInteger(value) || value <= 0) {
    return fallback;
  }

  return value;
};

const toCheckupCardLabel = (typeName: string): Pick<CheckupCardView, 'serviceLabel' | 'resourceLabel'> => {
  const [, serviceName = '', resourceName = typeName] = typeName.split('::');

  return {
    serviceLabel: humanizeAwsToken(serviceName),
    resourceLabel: humanizeAwsToken(resourceName)
  };
};

let resourceFieldRowSequence = 0;

const createResourceFieldRowId = (prefix: 'template' | 'custom'): string => `${prefix}-${resourceFieldRowSequence++}`;

const createEmptyAccountRow = (): AwsAccountFormRow => ({
  accountId: '',
  name: '',
  allowedRegions: ['us-east-1']
});

const createEmptyResourceFieldRow = (): TemplateAwareResourceFieldRow => ({
  id: createResourceFieldRowId('custom'),
  key: '',
  value: '',
  valueMode: 'text',
  required: false,
  fieldType: 'custom'
});

const templateCreateValue = (field: ResourceTemplate['fields'][number]): string =>
  field.required ? toTemplateDefaultText(field.defaultValue) : '';

const createEmptyPatchRow = (): ResourcePatchRow => ({
  op: 'replace',
  path: '',
  value: '',
  valueMode: 'text',
  from: ''
});

const createEmptyTagRow = (): ResourceTagRow => ({
  id: createResourceFieldRowId('custom'),
  key: '',
  value: ''
});

const isPlainObject = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const parseStructuredFieldObject = (value: string): Record<string, unknown> => {
  const trimmed = value.trim();

  if (trimmed.length === 0) {
    return {};
  }

  try {
    const parsed = JSON.parse(trimmed) as unknown;
    return isPlainObject(parsed) ? parsed : {};
  } catch {
    return {};
  }
};

const stringifyStructuredFieldObject = (value: Record<string, unknown>): string =>
  Object.keys(value).length === 0 ? '' : JSON.stringify(value);

const resolveStructuredUpdateFieldKind = (
  typeName: string,
  field: TemplateAwareResourceFieldRow
): StructuredUpdateFieldKind | null => {
  if (field.fieldType !== 'template') {
    return null;
  }

  if (typeName === 'AWS::S3::Bucket' && field.key === 'VersioningConfiguration') {
    return 's3-versioning';
  }

  if (typeName === 'AWS::S3::Bucket' && field.key === 'PublicAccessBlockConfiguration') {
    return 's3-public-access';
  }

  if (typeName === 'AWS::S3::Bucket' && field.key === 'BucketEncryption') {
    return 's3-encryption';
  }

  if (typeName === 'AWS::DynamoDB::Table' && field.key === 'ProvisionedThroughput') {
    return 'dynamodb-throughput';
  }

  return null;
};

const asOptionalBooleanSelection = (value: unknown): string =>
  typeof value === 'boolean' ? String(value) : '';

const toOptionalBooleanValue = (value: string): boolean | undefined =>
  value === 'true' ? true : value === 'false' ? false : undefined;

const asOptionalNumberSelection = (value: unknown): string =>
  typeof value === 'number' && Number.isFinite(value) ? String(value) : '';

const toOptionalNumberValue = (value: string): number | undefined => {
  const trimmed = value.trim();

  if (trimmed.length === 0) {
    return undefined;
  }

  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : undefined;
};

const createEmptyPermissionRow = (): PermissionFormRow => ({
  accountId: '',
  category: '*',
  resourceType: '',
  action: 'list'
});

const toTemplateFieldMode = (kind: ResourceTemplate['fields'][number]['kind']): ResourceFieldValueMode =>
  kind === 'number'
    ? 'number'
    : kind === 'boolean'
      ? 'boolean'
      : kind === 'json' || kind === 'array' || kind === 'object'
        ? 'json'
        : 'text';

const toTemplateDefaultText = (defaultValue: unknown): string => {
  if (defaultValue === undefined || defaultValue === null) {
    return '';
  }

  if (typeof defaultValue === 'string' || typeof defaultValue === 'number' || typeof defaultValue === 'boolean') {
    return String(defaultValue);
  }

  return JSON.stringify(defaultValue, null, 2);
};

const formatStateDate = (value: number): string => {
  const parsed = new Date(value);

  return Number.isNaN(parsed.getTime()) ? String(value) : parsed.toLocaleString('pt-BR');
};

const isTemplateFieldRow = (
  row: ResourceFieldRow | TemplateAwareResourceFieldRow
): row is TemplateAwareResourceFieldRow =>
  (row as TemplateAwareResourceFieldRow).fieldType === 'template';

const asTemplateLabel = (row: TemplateAwareResourceFieldRow): string => row.label || row.key;

const isTemplateFieldEnum = (row: TemplateAwareResourceFieldRow): boolean =>
  (row.enumValues?.length ?? 0) > 0;

const resolveDeleteConfirmationToken = (resource: ResourceSummary | null): string => {
  const displayName = resource?.displayName?.trim();

  if (displayName && displayName.toLowerCase() !== 'unknown') {
    return displayName;
  }

  return resource?.identifier.trim() ?? '';
};

const resolveDeleteConfirmationPrompt = (resource: ResourceSummary | null): string => {
  const token = resolveDeleteConfirmationToken(resource);

  return token.length > 0
    ? `Digite o nome do recurso (${token}) para habilitar a exclusao`
    : 'Digite o nome do recurso para habilitar a exclusao';
};

const resolveDeleteConfirmationPlaceholder = (resource: ResourceSummary | null): string =>
  resolveDeleteConfirmationToken(resource) || 'Digite o nome do recurso';

const buildTemplateCreateRows = (
  template: ResourceTemplate | undefined
): readonly TemplateAwareResourceFieldRow[] => {
  if (!template || template.fields.length === 0) {
    return [createEmptyResourceFieldRow()];
  }

  const rows = template.fields.map((field) => ({
    id: `template-${field.key}`,
    key: field.key,
    label: field.label,
    value: templateCreateValue(field),
    valueMode: toTemplateFieldMode(field.kind),
    enumValues: field.enumValues,
    description: field.description,
    placeholder: field.placeholder,
    kind: field.kind,
    required: field.required,
    fieldType: 'template' as const
  }));

  return rows.length > 0 ? rows : [createEmptyResourceFieldRow()];
};

const buildTemplateCreateSeedState = (template: ResourceTemplate | undefined): Record<string, unknown> => {
  if (!template) {
    return {};
  }

  return template.fields.reduce<Record<string, unknown>>((accumulator, field) => {
    if (field.required) {
      accumulator[field.key] = field.defaultValue ?? '';
      return accumulator;
    }

    return accumulator;
  }, {});
};

const buildTemplateUpdateRows = (
  template: ResourceTemplate | undefined
): readonly TemplateAwareResourceFieldRow[] => {
  if (!template || template.fields.length === 0) {
    return [createEmptyResourceFieldRow()];
  }

  const rows = template.fields.map((field) => ({
    id: `template-${field.key}`,
    key: field.key,
    label: field.label,
    value: '',
    valueMode: toTemplateFieldMode(field.kind),
    enumValues: field.enumValues,
    description: field.description,
    placeholder: field.placeholder,
    kind: field.kind,
    required: field.required,
    fieldType: 'template' as const
  }));

  return rows.length > 0 ? rows : [createEmptyResourceFieldRow()];
};

const buildFocusedUpdateRows = (
  profile: ResourceUpdateProfile | undefined
): readonly TemplateAwareResourceFieldRow[] => {
  if (!profile || profile.kind === 'tags' || profile.fields.length === 0) {
    return [];
  }

  return profile.fields.map((field) => ({
    id: `update-profile-${profile.id}-${field.key}`,
    key: field.key,
    label: field.label,
    value: '',
    valueMode: toTemplateFieldMode(field.kind),
    enumValues: field.enumValues,
    description: field.description,
    placeholder: field.placeholder,
    kind: field.kind,
    required: false,
    fieldType: 'template' as const
  }));
};

const parseTagRows = (rows: readonly ResourceTagRow[]): readonly { Key: string; Value: string }[] =>
  rows
    .filter((entry) => entry.key.trim().length > 0 || entry.value.trim().length > 0)
    .map((entry, index) => {
      const key = entry.key.trim();

      if (key.length === 0) {
        throw new Error(`Tag #${index + 1}: chave obrigatoria.`);
      }

      return {
        Key: key,
        Value: entry.value.trim()
      };
    });

const dedupeValues = (values: readonly string[]): readonly string[] => {
  const normalized = values.map((value) => value.trim()).filter((value) => value.length > 0);

  return [...new Set(normalized)];
};

const parseAccountRows = (rows: readonly AwsAccountFormRow[]): readonly AwsAccount[] =>
  rows
    .map((entry) => ({
      accountId: entry.accountId.trim(),
      name: entry.name.trim(),
      allowedRegions: dedupeValues(entry.allowedRegions)
    }))
    .filter(
      (entry) =>
        entry.accountId.length > 0 || entry.name.length > 0 || entry.allowedRegions.length > 0
    )
    .map((entry, index) => {
      if (!/^\d{12}$/.test(entry.accountId)) {
        throw new Error(`Conta #${index + 1}: accountId invalido.`);
      }

      if (entry.name.length === 0) {
        throw new Error(`Conta #${index + 1}: name obrigatorio.`);
      }

      if (entry.allowedRegions.length === 0) {
        throw new Error(`Conta #${index + 1}: informe ao menos uma regiao.`);
      }

      return {
        accountId: entry.accountId,
        name: entry.name,
        allowedRegions: entry.allowedRegions
      };
    });

const parsePermissionRows = (
  rows: readonly PermissionFormRow[],
  knownAccountIds: readonly string[] = []
): readonly PermissionScope[] => {
  const allowedAccountIds = new Set(knownAccountIds.map((entry) => entry.trim()));

  return rows
    .map((entry) => ({
      accountId: entry.accountId.trim(),
      category: entry.category,
      resourceType: entry.resourceType.trim(),
      action: entry.action
    }))
    .filter((entry) => entry.accountId.length > 0 || entry.resourceType.length > 0)
    .map((entry, index) => {
      if (entry.accountId.length === 0) {
        throw new Error(`Permissao #${index + 1}: accountId obrigatorio.`);
      }

      if (entry.resourceType.length === 0) {
        throw new Error(`Permissao #${index + 1}: resourceType obrigatorio.`);
      }

      if (!isValidPermissionCategory(entry.category)) {
        throw new Error(`Permissao #${index + 1}: category invalida.`);
      }

      if (allowedAccountIds.size > 0 && !allowedAccountIds.has(entry.accountId)) {
        throw new Error(`Permissao #${index + 1}: accountId nao encontrado para este usuario.`);
      }

      return entry;
    });
};

const parseResourceFieldValue = (rawValue: string, mode: ResourceFieldValueMode, fieldName: string): unknown => {
  const trimmed = rawValue.trim();

  if (mode === 'null') {
    return null;
  }

  if (mode === 'number') {
    if (trimmed.length === 0) {
      throw new Error(`${fieldName} deve ser um numero valido.`);
    }

    const parsed = Number(trimmed);

    if (!Number.isFinite(parsed)) {
      throw new Error(`${fieldName} deve ser um numero valido.`);
    }

    return parsed;
  }

  if (mode === 'boolean') {
    const lowered = trimmed.toLowerCase();

    if (lowered === 'true') {
      return true;
    }

    if (lowered === 'false') {
      return false;
    }

    throw new Error(`${fieldName} deve ser true ou false.`);
  }

  if (mode === 'json') {
    if (trimmed.length === 0) {
      throw new Error(`${fieldName} em JSON nao pode ficar vazio.`);
    }

    try {
      return JSON.parse(trimmed);
    } catch {
      throw new Error(`${fieldName} deve ser um JSON valido.`);
    }
  }

  return rawValue;
};

const getTemplateRequiredKeys = (template?: ResourceTemplate): readonly string[] =>
  template?.fields.filter((entry) => entry.required).map((entry) => entry.key) ?? [];

const assertRequiredTemplateValues = (
  template: ResourceTemplate | undefined,
  state: Record<string, unknown>,
  label: string
): void => {
  if (!template) {
    return;
  }

  const missingFields = template.fields
    .filter((field) => field.required)
    .filter((field) => {
      const rawValue = state[field.key];
      if (rawValue === undefined || rawValue === null) {
        return true;
      }

      if (typeof rawValue === 'string') {
        return rawValue.trim().length === 0;
      }

      return false;
    });

  if (missingFields.length > 0) {
    const names = missingFields.map((field) => field.key).join(', ');
    throw new Error(`${label}: campos obrigatorios nao preenchidos no template: ${names}.`);
  }
};

const parseResourceFieldRows = (
  rows: readonly (ResourceFieldRow | TemplateAwareResourceFieldRow)[],
  label: string,
  allowEmpty = false,
  options?: {
    requiredKeys?: readonly string[];
  }
): Record<string, unknown> => {
  const requiredKeys = new Set(options?.requiredKeys ?? []);

  const normalizedRows = rows
    .map((entry) => ({
      key: entry.key.trim(),
      value: entry.value,
      valueMode: entry.valueMode,
      required: (entry as TemplateAwareResourceFieldRow).required,
      fieldType: (entry as TemplateAwareResourceFieldRow).fieldType
    }))
    .filter((entry) => entry.key.length > 0);

  if (normalizedRows.length === 0) {
    if (allowEmpty) {
      return {};
    }

    throw new Error(`${label} deve possuir ao menos um campo.`);
  }

  const usedKeys = new Set<string>();

  return normalizedRows.reduce<Record<string, unknown>>((accumulator, entry, index) => {
    if (usedKeys.has(entry.key)) {
      throw new Error(`${label}: chave repetida "${entry.key}" na linha ${index + 1}.`);
    }

    const isRequired = entry.required || requiredKeys.has(entry.key);
    const hasValue = entry.value.trim().length > 0;

    if (allowEmpty && !hasValue) {
      return accumulator;
    }

    if (!isRequired && !hasValue) {
      return accumulator;
    }

    const mustValidateRequired =
      isRequired &&
      entry.valueMode !== 'null' &&
      !hasValue &&
      !allowEmpty;

    if (mustValidateRequired) {
      throw new Error(`${label}: campo obrigatório "${entry.key}" (linha ${index + 1}) nao informado.`);
    }

    usedKeys.add(entry.key);
    accumulator[entry.key] = parseResourceFieldValue(
      entry.value,
      entry.valueMode,
      `${label} - ${entry.key} (linha ${index + 1})`
    );

    return accumulator;
  }, {});
};

const parsePatchRows = (rows: readonly ResourcePatchRow[], label: string, allowEmpty = false): readonly Record<string, unknown>[] => {
  const normalizedRows = rows
    .map((entry) => ({
      op: entry.op,
      path: entry.path.trim(),
      from: entry.from.trim(),
      value: entry.value,
      valueMode: entry.valueMode
    }))
    .filter((entry) => entry.op.length > 0 || entry.path.length > 0 || entry.from.length > 0 || entry.value.length > 0);

  if (normalizedRows.length === 0) {
    if (allowEmpty) {
      return [];
    }

    throw new Error(`${label} deve possuir ao menos uma operacao.`);
  }

  return normalizedRows.map((entry, index) => {
    if (entry.op.length === 0) {
      throw new Error(`${label}: op obrigatoria na linha ${index + 1}.`);
    }

    if (entry.path.length === 0) {
      throw new Error(`${label}: path obrigatoria na linha ${index + 1}.`);
    }

    const patch: Record<string, unknown> = {
      op: entry.op,
      path: entry.path
    };

    if (entry.op === 'move' || entry.op === 'copy') {
      if (entry.from.length === 0) {
        throw new Error(`${label}: from obrigatorio para op ${entry.op} na linha ${index + 1}.`);
      }

      patch.from = entry.from;
      return patch;
    }

    if (entry.op === 'remove') {
      return patch;
    }

    patch.value = parseResourceFieldValue(
      entry.value,
      entry.valueMode,
      `${label}: valor da linha ${index + 1}`
    );

    return patch;
  });
};

const mapAccountsToRows = (accounts: readonly AwsAccount[]): readonly AwsAccountFormRow[] =>
  accounts.map((account) => ({
    accountId: account.accountId,
    name: account.name,
    allowedRegions: account.allowedRegions
  }));

const mapPermissionsToRows = (permissions: readonly PermissionScope[]): readonly PermissionFormRow[] =>
  permissions.map((permission) => ({
    accountId: permission.accountId,
    category: permission.category,
    resourceType: permission.resourceType,
    action: permission.action
  }));

const toResourceFieldValueMode = (rawValue: unknown): ResourceFieldValueMode => {
  if (rawValue === null) {
    return 'null';
  }

  if (typeof rawValue === 'number') {
    return 'number';
  }

  if (typeof rawValue === 'boolean') {
    return 'boolean';
  }

  if (typeof rawValue === 'object') {
    return 'json';
  }

  return 'text';
};

const toResourceFieldText = (rawValue: unknown): string =>
  typeof rawValue === 'string' ? rawValue : JSON.stringify(rawValue) ?? '';

const mapResourceStateRowsToForm = (state: Record<string, unknown>): readonly TemplateAwareResourceFieldRow[] => {
  const rows = Object.entries(state).map(([key, value]) => ({
    id: createResourceFieldRowId('custom'),
    key,
    valueMode: toResourceFieldValueMode(value),
    value: toResourceFieldText(value)
  }));

  return rows.length > 0
    ? rows.map((row) => ({
        ...row,
        required: false,
        fieldType: 'custom' as const
      }))
    : [createEmptyResourceFieldRow()];
};

const mapPatchRowsToForm = (patches: readonly Record<string, unknown>[]): readonly ResourcePatchRow[] => {
  const rows = patches
    .map((entry) => {
      const rawOp = typeof entry.op === 'string' ? entry.op : 'replace';
      const op = patchOperationOptions.includes(rawOp as PatchOperation)
        ? (rawOp as PatchOperation)
        : 'replace';

      const rawPath = typeof entry.path === 'string' ? entry.path : '';
      const rawFrom = typeof entry.from === 'string' ? entry.from : '';
      const rawValue = entry.value !== undefined ? entry.value : '';

      return {
        op,
        path: rawPath,
        from: rawFrom,
        valueMode: toResourceFieldValueMode(rawValue),
        value: toResourceFieldText(rawValue)
      };
    })
    .filter((row) => row.path.length > 0 || row.op.length > 0 || row.from.length > 0 || row.value.length > 0);

  return rows.length > 0 ? rows : [createEmptyPatchRow()];
};

const safeJsonParse = (rawText: string): unknown => {
  try {
    return JSON.parse(rawText);
  } catch {
    return rawText;
  }
};

const parseAsObject = (rawText: string, fieldName: string): Record<string, unknown> => {
  const parsedValue = safeJsonParse(rawText);

  if (typeof parsedValue !== 'object' || parsedValue === null || Array.isArray(parsedValue)) {
    throw new Error(`${fieldName} deve ser um objeto JSON.`);
  }

  return parsedValue as Record<string, unknown>;
};

const parseAsPatchArray = (rawText: string): readonly Record<string, unknown>[] => {
  const parsedValue = safeJsonParse(rawText);

  if (!Array.isArray(parsedValue)) {
    throw new Error('Patch document deve ser um array JSON.');
  }

  const allEntriesAreObjects = parsedValue.every(
    (entry) => typeof entry === 'object' && entry !== null && !Array.isArray(entry)
  );

  if (!allEntriesAreObjects) {
    throw new Error('Cada item do patch document deve ser um objeto JSON.');
  }

  return parsedValue as readonly Record<string, unknown>[];
};

const isValidRole = (value: string): value is UserRole => validRoles.includes(value as UserRole);

const isValidCategory = (value: string): value is AwsCategory =>
  categoryDefinitions.some((category) => category.id === value);

const isValidPermissionCategory = (value: string): value is AwsCategory | '*' =>
  value === '*' || isValidCategory(value);

type PersistedHeaderSelection = {
  category: AwsCategory;
  accountId: string;
  region: string;
  resourceType: string;
};

const readPersistedHeaderSelection = (): PersistedHeaderSelection | null => {
  const rawValue = window.localStorage.getItem(HEADER_SELECTION_STORAGE_KEY);

  if (!rawValue) {
    return null;
  }

  try {
    const parsedValue = JSON.parse(rawValue) as unknown;

    if (typeof parsedValue !== 'object' || parsedValue === null || Array.isArray(parsedValue)) {
      return null;
    }

    const {
      category,
      accountId,
      region,
      resourceType
    } = parsedValue as Record<string, unknown>;

    return {
      category: typeof category === 'string' && isValidCategory(category) ? category : 'compute',
      accountId: typeof accountId === 'string' ? accountId : '',
      region: typeof region === 'string' ? region : '',
      resourceType: typeof resourceType === 'string' ? resourceType : ''
    };
  } catch {
    return null;
  }
};

const persistHeaderSelection = (selection: PersistedHeaderSelection): void => {
  window.localStorage.setItem(HEADER_SELECTION_STORAGE_KEY, JSON.stringify(selection));
};

const clearPersistedHeaderSelection = (): void => {
  window.localStorage.removeItem(HEADER_SELECTION_STORAGE_KEY);
};

const initialHeaderSelection = (() => {
  const persistedSelection = readPersistedHeaderSelection();
  const category = persistedSelection?.category ?? 'compute';
  const defaultResourceTypes = getDefaultResourceTypesForCategory(category);
  const resourceType =
    persistedSelection?.resourceType.trim().length
      ? persistedSelection.resourceType.trim()
      : defaultResourceTypes[0] ?? '';

  return {
    category,
    accountId: persistedSelection?.accountId ?? '',
    region: persistedSelection?.region ?? '',
    resourceType
  };
})();

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css'
})
export class AppComponent {

  readonly categories = categoryDefinitions;
  readonly loading = signal(false);
  readonly errorMessage = signal('');
  readonly infoMessage = signal('');
  readonly toasts = signal<readonly ToastItem[]>([]);

  readonly loginEmail = signal('admin@platform.local');
  readonly loginPassword = signal('change-me-please');
  readonly authMode = signal<AuthMode>('login');
  readonly registerName = signal('');
  readonly registerEmail = signal('');
  readonly registerPassword = signal('change-me-please');
  readonly registerAccountsRows = signal<readonly AwsAccountFormRow[]>([
    {
      accountId: '111111111111',
      name: 'Minha Conta AWS',
      allowedRegions: ['us-east-1', 'sa-east-1']
    }
  ]);

  readonly token = signal<string | null>(window.localStorage.getItem(TOKEN_STORAGE_KEY));
  readonly user = signal<PublicUser | null>(null);
  readonly workspaceView = signal<WorkspaceView>('resources');

  readonly selectedCategory = signal<AwsCategory>(initialHeaderSelection.category);
  readonly selectedAccountId = signal(initialHeaderSelection.accountId);
  readonly selectedRegion = signal(initialHeaderSelection.region);
  readonly resourceTypes = signal<readonly string[]>(
    getDefaultResourceTypesForCategory(initialHeaderSelection.category)
  );
  readonly selectedResourceType = signal(initialHeaderSelection.resourceType);
  readonly resourceFieldValueModes = resourceFieldValueModes;
  readonly patchOperationOptions = patchOperationOptions;
  readonly resourceTemplates = signal<readonly ResourceTemplate[]>(defaultResourceTemplates);
  readonly selectedResourceTemplate = computed(
    () => this.resourceTemplates().find((template) => template.typeName === this.selectedResourceType())
  );
  readonly finopsOverview = signal<FinOpsOverviewResponse | null>(null);
  readonly finopsStaleDays = signal<number>(DEFAULT_FINOPS_STALE_DAYS);

  readonly checkupCounts = signal<Record<string, number>>({});
  readonly resources = signal<readonly ResourceSummary[]>([]);
  readonly selectedResource = signal<ResourceSummary | null>(null);
  readonly resourceDetails = signal<ResourceDetailsResponse | null>(null);
  readonly resourceStateHistory = signal<readonly ResourceStateRecord[]>([]);
  readonly resourceStateHistoryScope = signal<ResourceStateHistoryScope>('context');
  readonly awsRegionOptions = awsCommonRegions;
  readonly permissionCategoryOptions = permissionCategoryOptions;
  readonly actionOptions = validActions;
  readonly formatStateDate = formatStateDate;
  readonly asTemplateLabel = asTemplateLabel;
  readonly isTemplateFieldRow = isTemplateFieldRow;
  readonly isTemplateFieldEnum = isTemplateFieldEnum;
  readonly resourceStateHistoryScopeLabel = computed(() => {
    switch (this.resourceStateHistoryScope()) {
      case 'resource':
        return 'recurso selecionado';
      case 'type':
        return 'tipo atual';
      default:
        return 'contexto atual';
    }
  });

  readonly createPayloadText = signal('{}');
  readonly createPayloadRows = signal<readonly TemplateAwareResourceFieldRow[]>([
    createEmptyResourceFieldRow()
  ]);
  readonly useJsonCreatePayload = signal(false);
  readonly showCreateOptionalTemplateFields = signal(false);
  readonly showCreateCustomFields = signal(false);
  readonly showCreateModal = signal(false);
  readonly resourceActionTab = signal<ResourceOperationTab>('create');
  readonly updateIdentifier = signal('');
  readonly updateDesiredStateText = signal(
    '{\n  "Tags": [\n    {"Key": "managed-by", "Value": "platform"}\n  ]\n}'
  );
  readonly updateDesiredStateRows = signal<readonly TemplateAwareResourceFieldRow[]>([
    createEmptyResourceFieldRow()
  ]);
  readonly updateTagRows = signal<readonly ResourceTagRow[]>([createEmptyTagRow()]);
  readonly clearAllUpdateTags = signal(false);
  readonly selectedUpdateProfileId = signal('');
  readonly useJsonUpdateDesiredState = signal(false);
  readonly showUpdateOptionalTemplateFields = signal(false);
  readonly showUpdateCustomFields = signal(false);
  readonly showUpdateModal = signal(false);
  readonly showUpdatePatchEditor = signal(false);
  readonly updatePatchText = signal(
    '[\n  {"op": "replace", "path": "/Tags", "value": [{"Key": "managed-by", "Value": "platform"}]}\n]'
  );
  readonly updatePatchRows = signal<readonly ResourcePatchRow[]>([createEmptyPatchRow()]);
  readonly useJsonPatchPayload = signal(false);

  readonly deleteCandidate = signal<ResourceSummary | null>(null);
  readonly deleteConfirmationText = signal('');
  readonly deleteIntentId = signal<string | null>(null);
  readonly deleteConfirmationToken = computed(() =>
    resolveDeleteConfirmationToken(this.deleteCandidate())
  );
  readonly deleteConfirmationPrompt = computed(() =>
    resolveDeleteConfirmationPrompt(this.deleteCandidate())
  );
  readonly deleteConfirmationPlaceholder = computed(() =>
    resolveDeleteConfirmationPlaceholder(this.deleteCandidate())
  );

  readonly adminUsers = signal<readonly AdminUser[]>([]);
  readonly selectedAdminUserId = signal('');
  readonly adminCreateName = signal('');
  readonly adminCreateEmail = signal('');
  readonly adminCreatePassword = signal('change-me-please');
  readonly adminCreateRole = signal<UserRole>('viewer');
  readonly adminCreateAccountsRows = signal<readonly AwsAccountFormRow[]>([
    {
      accountId: '222222222222',
      name: 'Sandbox',
      allowedRegions: ['us-east-1']
    }
  ]);

  readonly adminEditName = signal('');
  readonly adminEditEmail = signal('');
  readonly adminEditRole = signal<UserRole>('viewer');
  readonly adminEditPassword = signal('');
  readonly adminAccountsRows = signal<readonly AwsAccountFormRow[]>([]);
  readonly adminPermissionsRows = signal<readonly PermissionFormRow[]>([]);
  readonly adminDeleteConfirmationText = signal('');
  readonly adminDeleteIntentId = signal<string | null>(null);

  readonly isAuthenticated = computed(() => this.user() !== null && this.token() !== null);
  readonly isAdmin = computed(() => this.user()?.role === 'admin');
  readonly availableAccounts = computed(() => this.user()?.accounts ?? []);
  readonly availableRegions = computed(() => {
    const accountId = this.selectedAccountId();
    const account = this.availableAccounts().find((entry) => entry.accountId === accountId);
    return account?.allowedRegions ?? [];
  });
  readonly selectedAdminUser = computed(
    () => this.adminUsers().find((entry) => entry.id === this.selectedAdminUserId()) ?? null
  );
  readonly finopsWarnings = computed(() => this.finopsOverview()?.warnings ?? []);
  readonly finopsResourceTypeRows = computed<readonly FinOpsResourceTypeSummary[]>(() => {
    const overview = this.finopsOverview();
    if (!overview) {
      return [];
    }

    return Object.entries(overview.resourcesByType)
      .map(([typeName, total]) => {
        const obsolete = overview.obsoleteByType[typeName] ?? 0;
        const percent = total === 0 ? 0 : Number(((obsolete / total) * 100).toFixed(1));
        return {
          typeName,
          total,
          obsolete,
          percent
        };
      })
      .sort((left, right) =>
        right.total !== left.total
          ? right.total - left.total
          : left.typeName.localeCompare(right.typeName)
      );
  });
  readonly checkupCards = computed<readonly CheckupCardView[]>(() => {
    const entries = Object.entries(this.checkupCounts());
    const maxValue = Math.max(1, ...entries.map(([, total]) => total));

    return entries.map(([typeName, total]) => {
      const { serviceLabel, resourceLabel } = toCheckupCardLabel(typeName);

      return {
        typeName,
        serviceLabel,
        resourceLabel,
        total,
        fillPercentage: Math.max(8, Math.round((total / maxValue) * 100))
      };
    });
  });
  readonly activeResourceActionTab = computed(() => {
    return this.resourceActionTab();
  });
  readonly createTemplateRows = computed(() =>
    this.createPayloadRows().filter((entry): entry is TemplateAwareResourceFieldRow => isTemplateFieldRow(entry))
  );
  readonly createTemplateRequiredRows = computed(
    () => this.createTemplateRows().filter((entry) => entry.required)
  );
  readonly createTemplateOptionalRows = computed(
    () => this.createTemplateRows().filter((entry) => !entry.required)
  );
  readonly createTemplateRowsForForm = computed(() =>
    this.showCreateOptionalTemplateFields() ? this.createTemplateRows() : this.createTemplateRequiredRows()
  );
  readonly createCustomRows = computed(
    () => this.createPayloadRows().filter((entry) => !isTemplateFieldRow(entry))
  );
  readonly hasCreateTemplateOptionalRows = computed(() => this.createTemplateOptionalRows().length > 0);
  readonly updateProfiles = computed(() => getResourceUpdateProfiles(this.selectedResourceType()));
  readonly selectedUpdateProfile = computed(
    () =>
      this.updateProfiles().find((profile) => profile.id === this.selectedUpdateProfileId()) ??
      this.updateProfiles()[0] ??
      null
  );
  readonly isTagUpdateProfile = computed(() => this.selectedUpdateProfile()?.kind === 'tags');
  readonly updateTemplateRows = computed(() =>
    this.updateDesiredStateRows().filter((entry): entry is TemplateAwareResourceFieldRow => isTemplateFieldRow(entry))
  );
  readonly updateTemplateRequiredRows = computed(
    () => this.updateTemplateRows().filter((entry) => entry.required)
  );
  readonly updateTemplateOptionalRows = computed(
    () => this.updateTemplateRows().filter((entry) => !entry.required)
  );
  readonly updateTemplateRowsForForm = computed(() =>
    this.showUpdateOptionalTemplateFields() ? this.updateTemplateRows() : this.updateTemplateRequiredRows()
  );
  readonly updateTemplateGridMode = computed(() => {
    const total = this.updateTemplateRowsForForm().length;

    if (total <= 1) {
      return 'single';
    }

    if (total === 2) {
      return 'double';
    }

    if (total === 3) {
      return 'triple';
    }

    return 'quad';
  });
  readonly updateCustomRows = computed(
    () => this.updateDesiredStateRows().filter((entry) => !isTemplateFieldRow(entry))
  );
  readonly hasUpdateTemplateOptionalRows = computed(() => this.updateTemplateOptionalRows().length > 0);
  readonly selectedCategoryLabel = computed(
    () => this.categories.find((entry) => entry.id === this.selectedCategory())?.label ?? this.selectedCategory()
  );
  readonly adminCreateAccountIds = computed(() =>
    [...new Set(this.adminCreateAccountsRows().map((entry) => entry.accountId.trim()).filter((entry) => entry.length > 0))]
  );
  readonly adminEditAccountIds = computed(() =>
    [...new Set(this.adminAccountsRows().map((entry) => entry.accountId.trim()).filter((entry) => entry.length > 0))]
  );
  readonly adminPermissionResourceTypes = computed(() => {
    const existingTypes = this.adminPermissionsRows()
      .map((entry) => entry.resourceType.trim())
      .filter((entry) => entry.length > 0);

    return [...new Set([...topAwsResourceTypes, ...existingTypes])];
  });

  constructor() {
    effect(() => {
      const message = this.errorMessage().trim();
      if (message.length > 0) {
        this.enqueueToast('error', message);
      }
    }, { allowSignalWrites: true });

    effect(() => {
      const message = this.infoMessage().trim();
      if (message.length > 0) {
        this.enqueueToast('info', message);
      }
    }, { allowSignalWrites: true });

    effect(() => {
      if (!this.token()) {
        clearPersistedHeaderSelection();
        return;
      }

      persistHeaderSelection({
        category: this.selectedCategory(),
        accountId: this.selectedAccountId(),
        region: this.selectedRegion(),
        resourceType: this.selectedResourceType()
      });
    });

    this.applyTemplateDrivenRows(this.selectedResourceType());
    void this.restoreSession();
  }

  setResourceActionTab(action: ResourceOperationTab): void {
    this.resourceActionTab.set(action);
  }

  setAuthMode(mode: AuthMode): void {
    this.authMode.set(mode);
    this.clearMessages();
  }

  openCreateModal(): void {
    const resourceType = this.selectedResourceType();

    if (resourceType.length === 0) {
      this.errorMessage.set('Selecione um tipo de recurso antes de criar.');
      return;
    }

    this.applyTemplateDrivenRows(resourceType);
    this.clearMessages();
    this.showCreateModal.set(true);
  }

  closeCreateModal(): void {
    this.showCreateModal.set(false);
  }

  async openUpdateModal(resource: ResourceSummary): Promise<void> {
    try {
      await this.alignContextForResource(resource);
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao preparar update.');
      return;
    }

    this.selectedResourceType.set(resource.typeName);
    this.applyTemplateDrivenRows(resource.typeName);
    this.updateIdentifier.set(resource.identifier);
    this.clearMessages();
    this.showUpdatePatchEditor.set(false);
    this.showUpdateModal.set(true);
  }

  closeUpdateModal(): void {
    this.showUpdateModal.set(false);
  }

  selectUpdateProfile(profileId: string): void {
    const resourceType = this.selectedResourceType();
    const profile = getResourceUpdateProfile(resourceType, profileId);

    this.selectedUpdateProfileId.set(profile?.id ?? '');
    this.updateDesiredStateRows.set(buildFocusedUpdateRows(profile));
    this.updateTagRows.set([createEmptyTagRow()]);
    this.clearAllUpdateTags.set(false);
    this.useJsonUpdateDesiredState.set(false);
    this.useJsonPatchPayload.set(false);
    this.showUpdatePatchEditor.set(false);
    this.showUpdateOptionalTemplateFields.set(false);
    this.showUpdateCustomFields.set(false);
  }

  resolveStructuredUpdateFieldKind(field: TemplateAwareResourceFieldRow): StructuredUpdateFieldKind | null {
    return resolveStructuredUpdateFieldKind(this.selectedResourceType(), field);
  }

  getStructuredUpdateFieldVersioningStatus(fieldId: string): string {
    const current = this.getStructuredUpdateFieldObject(fieldId);
    return typeof current.Status === 'string' ? current.Status : '';
  }

  updateStructuredUpdateFieldVersioningStatus(fieldId: string, status: string): void {
    this.setStructuredUpdateFieldObject(fieldId, status.length > 0 ? { Status: status } : {});
  }

  getStructuredUpdateFieldBooleanValue(fieldId: string, key: string): string {
    const current = this.getStructuredUpdateFieldObject(fieldId);
    return asOptionalBooleanSelection(current[key]);
  }

  updateStructuredUpdateFieldBooleanValue(fieldId: string, key: string, value: string): void {
    const current = this.getStructuredUpdateFieldObject(fieldId);
    const nextValue = toOptionalBooleanValue(value);
    const nextState = { ...current };

    if (nextValue === undefined) {
      delete nextState[key];
    } else {
      nextState[key] = nextValue;
    }

    this.setStructuredUpdateFieldObject(fieldId, nextState);
  }

  getStructuredUpdateFieldEncryptionAlgorithm(fieldId: string): string {
    const configuration = this.getStructuredUpdateFieldEncryptionConfiguration(fieldId);
    return typeof configuration.SSEAlgorithm === 'string' ? configuration.SSEAlgorithm : '';
  }

  updateStructuredUpdateFieldEncryptionAlgorithm(fieldId: string, algorithm: string): void {
    const configuration = this.getStructuredUpdateFieldEncryptionConfiguration(fieldId);

    if (algorithm.length === 0) {
      this.setStructuredUpdateFieldObject(fieldId, {});
      return;
    }

    this.setStructuredUpdateFieldEncryptionConfiguration(fieldId, {
      ...configuration,
      SSEAlgorithm: algorithm
    });
  }

  getStructuredUpdateFieldEncryptionKeyId(fieldId: string): string {
    const configuration = this.getStructuredUpdateFieldEncryptionConfiguration(fieldId);
    return typeof configuration.KMSMasterKeyID === 'string' ? configuration.KMSMasterKeyID : '';
  }

  updateStructuredUpdateFieldEncryptionKeyId(fieldId: string, keyId: string): void {
    const configuration = this.getStructuredUpdateFieldEncryptionConfiguration(fieldId);

    if (!configuration.SSEAlgorithm) {
      return;
    }

    const nextConfiguration = { ...configuration };

    if (keyId.trim().length === 0) {
      delete nextConfiguration.KMSMasterKeyID;
    } else {
      nextConfiguration.KMSMasterKeyID = keyId.trim();
    }

    this.setStructuredUpdateFieldEncryptionConfiguration(fieldId, nextConfiguration);
  }

  getStructuredUpdateFieldThroughputValue(fieldId: string, key: string): string {
    const current = this.getStructuredUpdateFieldObject(fieldId);
    return asOptionalNumberSelection(current[key]);
  }

  updateStructuredUpdateFieldThroughputValue(fieldId: string, key: string, value: string): void {
    const current = this.getStructuredUpdateFieldObject(fieldId);
    const nextValue = toOptionalNumberValue(value);
    const nextState = { ...current };

    if (nextValue === undefined) {
      delete nextState[key];
    } else {
      nextState[key] = nextValue;
    }

    this.setStructuredUpdateFieldObject(fieldId, nextState);
  }

  toggleCreateTemplateOptionalFields(): void {
    this.showCreateOptionalTemplateFields.update((state) => !state);
  }

  startCreateCustomFields(): void {
    this.showCreateCustomFields.set(true);
    if (this.createCustomRows().length === 0) {
      this.addCreatePayloadFieldRow();
    }
  }

  toggleCreateCustomFields(): void {
    this.showCreateCustomFields.update((state) => !state);
  }

  toggleUpdateTemplateOptionalFields(): void {
    this.showUpdateOptionalTemplateFields.update((state) => !state);
  }

  startUpdateCustomFields(): void {
    this.showUpdateCustomFields.set(true);
    if (this.updateCustomRows().length === 0) {
      this.addUpdateDesiredStateFieldRow();
    }
  }

  toggleUpdateCustomFields(): void {
    this.showUpdateCustomFields.update((state) => !state);
  }

  toggleUpdatePatchEditor(): void {
    this.showUpdatePatchEditor.update((state) => !state);
  }

  addUpdateTagRow(): void {
    this.updateTagRows.set([...this.updateTagRows(), createEmptyTagRow()]);
  }

  toggleClearAllUpdateTags(): void {
    this.clearAllUpdateTags.update((state) => !state);
  }

  updateUpdateTagRow(id: string, patch: Partial<Omit<ResourceTagRow, 'id'>>): void {
    this.updateTagRows.set(
      this.updateTagRows().map((entry) =>
        entry.id === id ? { ...entry, ...patch } : entry
      )
    );
  }

  removeUpdateTagRow(id: string): void {
    const nextRows = this.updateTagRows().filter((entry) => entry.id !== id);
    this.updateTagRows.set(nextRows.length > 0 ? nextRows : [createEmptyTagRow()]);
  }

  async onLogin(): Promise<void> {
    this.setLoading(true);
    this.clearMessages();

    try {
      const response = await this.apiRequest<LoginResponse>('/api/auth/login', {
        method: 'POST',
        body: JSON.stringify({
          email: this.loginEmail(),
          password: this.loginPassword()
        })
      });

      this.saveSession(response.token, response.user);
      this.restoreHeaderContextFromUser(response.user);
      await this.switchContext();
      await this.refreshAdminStateIfNeeded();
      this.infoMessage.set('Login realizado com sucesso.');
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao autenticar.');
    } finally {
      this.setLoading(false);
    }
  }

  async onRegister(): Promise<void> {
    this.setLoading(true);
    this.clearMessages();

    try {
      const accounts = parseAccountRows(this.registerAccountsRows());
      if (accounts.length === 0) {
        throw new Error('Informe ao menos uma conta para cadastro.');
      }

      const response = await this.apiRequest<LoginResponse>('/api/auth/register', {
        method: 'POST',
        body: JSON.stringify({
          name: this.registerName().trim(),
          email: this.registerEmail().trim(),
          password: this.registerPassword(),
          accounts
        })
      });

      this.saveSession(response.token, response.user);
      this.restoreHeaderContextFromUser(response.user);
      await this.switchContext();
      await this.refreshAdminStateIfNeeded();
      this.infoMessage.set('Cadastro realizado com sucesso.');
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao cadastrar usuario.');
    } finally {
      this.setLoading(false);
    }
  }

  addRegisterAccountRow(): void {
    this.registerAccountsRows.set([...this.registerAccountsRows(), createEmptyAccountRow()]);
  }

  updateRegisterAccountRow(index: number, patch: Partial<AwsAccountFormRow>): void {
    this.registerAccountsRows.set(
      this.registerAccountsRows().map((entry, rowIndex) =>
        rowIndex === index ? { ...entry, ...patch } : entry
      )
    );
  }

  removeRegisterAccountRow(index: number): void {
    this.registerAccountsRows.set(this.registerAccountsRows().filter((_, rowIndex) => rowIndex !== index));
  }

  updateRegisterAccountRegions(index: number, allowedRegions: readonly string[]): void {
    this.updateRegisterAccountRow(index, {
      allowedRegions: dedupeValues(allowedRegions)
    });
  }

  addCreatePayloadFieldRow(): void {
    this.createPayloadRows.set([...this.createPayloadRows(), createEmptyResourceFieldRow()]);
  }

  updateCreatePayloadFieldRow(index: number, patch: Partial<ResourceFieldRow>): void {
    const current = this.createPayloadRows()[index];
    if (isTemplateFieldRow(current) && (patch.key !== undefined || patch.valueMode !== undefined)) {
      return;
    }

    this.createPayloadRows.set(
      this.createPayloadRows().map((entry, rowIndex) =>
        rowIndex === index ? { ...entry, ...patch } : entry
      )
    );
  }

  updateCreatePayloadFieldRowById(id: string, patch: Partial<ResourceFieldRow>): void {
    const rowIndex = this.createPayloadRows().findIndex((entry) => entry.id === id);
    if (rowIndex < 0) {
      return;
    }

    this.updateCreatePayloadFieldRow(rowIndex, patch);
  }

  removeCreatePayloadFieldRow(index: number): void {
    const field = this.createPayloadRows()[index];
    if (field?.fieldType === 'template' && field.required) {
      this.errorMessage.set('Campo template obrigatório não pode ser removido.');
      return;
    }

    this.createPayloadRows.set(this.createPayloadRows().filter((_, rowIndex) => rowIndex !== index));
    if (this.createPayloadRows().length === 0) {
      this.createPayloadRows.set([createEmptyResourceFieldRow()]);
    }
  }

  removeCreatePayloadFieldRowById(id: string): void {
    const rowIndex = this.createPayloadRows().findIndex((entry) => entry.id === id);
    if (rowIndex < 0) {
      return;
    }

    this.removeCreatePayloadFieldRow(rowIndex);
  }

  addUpdateDesiredStateFieldRow(): void {
    this.updateDesiredStateRows.set([...this.updateDesiredStateRows(), createEmptyResourceFieldRow()]);
  }

  updateUpdateDesiredStateFieldRow(index: number, patch: Partial<ResourceFieldRow>): void {
    const current = this.updateDesiredStateRows()[index];
    if (isTemplateFieldRow(current) && (patch.key !== undefined || patch.valueMode !== undefined)) {
      return;
    }

    this.updateDesiredStateRows.set(
      this.updateDesiredStateRows().map((entry, rowIndex) =>
        rowIndex === index ? { ...entry, ...patch } : entry
      )
    );
  }

  updateUpdateDesiredStateFieldRowById(id: string, patch: Partial<ResourceFieldRow>): void {
    const rowIndex = this.updateDesiredStateRows().findIndex((entry) => entry.id === id);
    if (rowIndex < 0) {
      return;
    }

    this.updateUpdateDesiredStateFieldRow(rowIndex, patch);
  }

  removeUpdateDesiredStateFieldRow(index: number): void {
    this.updateDesiredStateRows.set(
      this.updateDesiredStateRows().filter((_, rowIndex) => rowIndex !== index)
    );

    if (this.updateDesiredStateRows().length === 0) {
      this.updateDesiredStateRows.set([createEmptyResourceFieldRow()]);
    }
  }

  removeUpdateDesiredStateFieldRowById(id: string): void {
    const rowIndex = this.updateDesiredStateRows().findIndex((entry) => entry.id === id);
    if (rowIndex < 0) {
      return;
    }

    this.removeUpdateDesiredStateFieldRow(rowIndex);
  }

  addUpdatePatchRow(): void {
    this.updatePatchRows.set([...this.updatePatchRows(), createEmptyPatchRow()]);
  }

  updatePatchRow(index: number, patch: Partial<ResourcePatchRow>): void {
    this.updatePatchRows.set(
      this.updatePatchRows().map((entry, rowIndex) =>
        rowIndex === index ? { ...entry, ...patch } : entry
      )
    );
  }

  removeUpdatePatchRow(index: number): void {
    this.updatePatchRows.set(this.updatePatchRows().filter((_, rowIndex) => rowIndex !== index));

    if (this.updatePatchRows().length === 0) {
      this.updatePatchRows.set([createEmptyPatchRow()]);
    }
  }

  toggleCreatePayloadMode(): void {
    const nextMode = !this.useJsonCreatePayload();
    if (nextMode) {
      const requiredKeys = getTemplateRequiredKeys(this.selectedResourceTemplate());
      try {
        this.createPayloadText.set(
          JSON.stringify(
            parseResourceFieldRows(this.createPayloadRows(), 'Create Payload', true, {
              requiredKeys
            }),
            null,
            2
          )
        );
      } catch (error) {
        this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao montar JSON de payload.');
        return;
      }
    }
    this.useJsonCreatePayload.set(nextMode);
  }

  toggleUpdateDesiredStateMode(): void {
    const nextMode = !this.useJsonUpdateDesiredState();
    if (nextMode) {
      try {
        this.updateDesiredStateText.set(
          JSON.stringify(parseResourceFieldRows(this.updateDesiredStateRows(), 'DesiredState', true), null, 2)
        );
      } catch (error) {
        this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao montar JSON desiredState.');
        return;
      }
    } else {
      try {
        this.updateDesiredStateRows.set(
          mapResourceStateRowsToForm(parseAsObject(this.updateDesiredStateText(), 'DesiredState'))
        );
      } catch (error) {
        this.errorMessage.set(error instanceof Error ? error.message : 'JSON de desiredState invalido.');
        return;
      }
    }
    this.useJsonUpdateDesiredState.set(nextMode);
  }

  toggleUpdatePatchMode(): void {
    const nextMode = !this.useJsonPatchPayload();
    if (nextMode) {
      try {
        this.updatePatchText.set(
          JSON.stringify(parsePatchRows(this.updatePatchRows(), 'Patch Document', true), null, 2)
        );
      } catch (error) {
        this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao montar patch JSON.');
        return;
      }
    } else {
      try {
        this.updatePatchRows.set(mapPatchRowsToForm(parseAsPatchArray(this.updatePatchText())));
      } catch (error) {
        this.errorMessage.set(error instanceof Error ? error.message : 'JSON de patch invalido.');
        return;
      }
    }
    this.useJsonPatchPayload.set(nextMode);
  }

  addAdminCreateAccountRow(): void {
    this.adminCreateAccountsRows.set([...this.adminCreateAccountsRows(), createEmptyAccountRow()]);
  }

  updateAdminCreateAccountRow(index: number, patch: Partial<AwsAccountFormRow>): void {
    this.adminCreateAccountsRows.set(
      this.adminCreateAccountsRows().map((entry, rowIndex) =>
        rowIndex === index ? { ...entry, ...patch } : entry
      )
    );
  }

  removeAdminCreateAccountRow(index: number): void {
    this.adminCreateAccountsRows.set(
      this.adminCreateAccountsRows().filter((_, rowIndex) => rowIndex !== index)
    );
  }

  updateAdminCreateAccountRegions(index: number, allowedRegions: readonly string[]): void {
    this.updateAdminCreateAccountRow(index, {
      allowedRegions: dedupeValues(allowedRegions)
    });
  }

  addAdminAccountRow(): void {
    this.adminAccountsRows.set([...this.adminAccountsRows(), createEmptyAccountRow()]);
  }

  updateAdminAccountRow(index: number, patch: Partial<AwsAccountFormRow>): void {
    this.adminAccountsRows.set(
      this.adminAccountsRows().map((entry, rowIndex) =>
        rowIndex === index ? { ...entry, ...patch } : entry
      )
    );
  }

  removeAdminAccountRow(index: number): void {
    this.adminAccountsRows.set(this.adminAccountsRows().filter((_, rowIndex) => rowIndex !== index));
  }

  updateAdminAccountRegions(index: number, allowedRegions: readonly string[]): void {
    this.updateAdminAccountRow(index, {
      allowedRegions: dedupeValues(allowedRegions)
    });
  }

  addAdminPermissionRow(): void {
    this.adminPermissionsRows.set([...this.adminPermissionsRows(), createEmptyPermissionRow()]);
  }

  updateAdminPermissionRow(index: number, patch: Partial<PermissionFormRow>): void {
    this.adminPermissionsRows.set(
      this.adminPermissionsRows().map((entry, rowIndex) =>
        rowIndex === index ? { ...entry, ...patch } : entry
      )
    );
  }

  removeAdminPermissionRow(index: number): void {
    this.adminPermissionsRows.set(
      this.adminPermissionsRows().filter((_, rowIndex) => rowIndex !== index)
    );
  }

  async switchView(nextView: WorkspaceView): Promise<void> {
    if (nextView === 'admin' && !this.isAdmin()) {
      this.errorMessage.set('Acesso restrito ao perfil admin.');
      return;
    }

    this.workspaceView.set(nextView);
    this.clearMessages();

    if (nextView === 'admin') {
      await this.loadAdminUsers();
      return;
    }

    if (nextView === 'finops') {
      await this.loadFinopsOverview();
      return;
    }

    await this.loadResources();
  }

  async onCategoryChange(category: AwsCategory): Promise<void> {
    await this.updateContextSelection(() => {
      this.selectedCategory.set(category);
    }, 'Erro ao trocar categoria.');
  }

  async onAccountChange(accountId: string): Promise<void> {
    await this.updateContextSelection(() => {
      this.selectedAccountId.set(accountId);

      const regions = this.availableRegions();
      if (regions.length > 0) {
        this.selectedRegion.set(regions[0]);
      }
    }, 'Erro ao trocar conta.');
  }

  async onRegionChange(region: string): Promise<void> {
    await this.updateContextSelection(() => {
      this.selectedRegion.set(region);
    }, 'Erro ao trocar regiao.');
  }

  async onResourceTypeChange(resourceType: string): Promise<void> {
    this.selectedResourceType.set(resourceType);
    this.applyTemplateDrivenRows(resourceType);
    this.selectedResource.set(null);
    this.resourceDetails.set(null);
    this.resourceStateHistory.set([]);
    this.resourceStateHistoryScope.set('context');
    if (this.workspaceView() === 'resources') {
      await this.loadResources();
    }
    if (this.workspaceView() === 'finops') {
      await this.loadFinopsOverview();
    }
  }

  async loadFinopsOverview(): Promise<void> {
    const accountId = this.selectedAccountId();
    const region = this.selectedRegion();

    if (accountId.length === 0 || region.length === 0) {
      this.finopsOverview.set(null);
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      const query = new URLSearchParams({
        staleDays: String(toFinitePositiveInt(this.finopsStaleDays(), DEFAULT_FINOPS_STALE_DAYS))
      });

      const response = await this.apiRequest<FinOpsOverviewResponse>(
        `/api/finops/overview?${query.toString()}`
      );

      this.finopsOverview.set(response);
      this.finopsStaleDays.set(response.staleThresholdDays);
    } catch (error) {
      this.finopsOverview.set(null);
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao carregar resumo FinOps.');
    } finally {
      this.setLoading(false);
    }
  }

  async onFinopsStaleDaysChange(value: string): Promise<void> {
    this.finopsStaleDays.set(toFinitePositiveInt(Number(value), DEFAULT_FINOPS_STALE_DAYS));
    await this.loadFinopsOverview();
  }

  async refreshCurrentViewData(): Promise<void> {
    if (this.workspaceView() === 'finops') {
      await this.loadFinopsOverview();
      return;
    }

    if (this.workspaceView() === 'resources') {
      await this.loadResources();
    }
  }

  async loadResourceDetails(
    resource: ResourceSummary,
    options?: { silent?: boolean }
  ): Promise<void> {
    if (!options?.silent) {
      this.clearMessages();
    }

    this.selectedResource.set(resource);
    this.resourceStateHistory.set([]);
    this.resourceStateHistoryScope.set('resource');

    try {
      await this.alignContextForResource(resource);

      const query = new URLSearchParams({
        typeName: resource.typeName,
        identifier: resource.identifier
      });

      const details = await this.apiRequest<ResourceDetailsResponse>(
        `/api/resources/details?${query.toString()}`
      );

      this.resourceDetails.set(details);
    } catch (error) {
      this.selectedResource.set(null);
      this.resourceDetails.set(null);
      this.resourceStateHistory.set([]);
      this.resourceStateHistoryScope.set('context');
      if (!options?.silent) {
        this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao carregar detalhes.');
      }
      return;
    }

    try {
      await this.loadResourceStateHistory(resource.typeName, resource.identifier);
    } catch (error) {
      this.resourceStateHistory.set([]);
      this.resourceStateHistoryScope.set('context');
      if (!options?.silent) {
        this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao carregar histórico.');
      }
    }
  }

  private async loadResourceStateHistory(typeName?: string, identifier?: string): Promise<void> {
    const fetchHistory = async (queryValues: {
      typeName?: string;
      identifier?: string;
    }): Promise<readonly ResourceStateRecord[]> => {
      const query = new URLSearchParams();
      if (queryValues.typeName && queryValues.typeName.length > 0) {
        query.set('typeName', queryValues.typeName);
      }
      if (queryValues.identifier && queryValues.identifier.length > 0) {
        query.set('identifier', queryValues.identifier);
      }

      const queryString = query.toString();
      const endpoint =
        queryString.length > 0 ? `/api/resources/state?${queryString}` : '/api/resources/state';

      const response = await this.apiRequest<ResourceStateHistoryResponse>(endpoint);
      return response.history;
    };

    if (typeName && identifier) {
      const resourceHistory = await fetchHistory({ typeName, identifier });
      if (resourceHistory.length > 0) {
        this.resourceStateHistory.set(resourceHistory);
        this.resourceStateHistoryScope.set('resource');
        return;
      }
    }

    if (typeName) {
      const typeHistory = await fetchHistory({ typeName });
      if (typeHistory.length > 0) {
        this.resourceStateHistory.set(typeHistory);
        this.resourceStateHistoryScope.set('type');
        return;
      }
    }

    const contextHistory = await fetchHistory({});
    this.resourceStateHistory.set(contextHistory);
    this.resourceStateHistoryScope.set('context');
  }

  async createResource(): Promise<void> {
    const typeName = this.selectedResourceType();
    const template = this.selectedResourceTemplate();
    const requiredKeys = getTemplateRequiredKeys(template);

    if (typeName.length === 0) {
      this.errorMessage.set('Selecione o tipo de recurso para criar.');
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      const desiredState = this.useJsonCreatePayload()
        ? parseAsObject(this.createPayloadText(), 'DesiredState')
        : parseResourceFieldRows(this.createPayloadRows(), 'Payload', false, {
          requiredKeys
        });

      if (this.useJsonCreatePayload()) {
        assertRequiredTemplateValues(template, desiredState, 'Payload');
      }
      assertRequiredTemplateValues(template, desiredState, 'Payload');

      if (requiredKeys.length > 0 && !this.useJsonCreatePayload()) {
        this.createPayloadText.set(
          JSON.stringify(desiredState, null, 2)
        );
      }

      await this.apiRequest('/api/resources', {
        method: 'POST',
        body: JSON.stringify({
          typeName,
          desiredState
        })
      });

      this.infoMessage.set('Operacao de create enviada com sucesso.');
      this.closeCreateModal();
      await this.switchContext();
      await this.refreshCurrentViewData();
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao criar recurso.');
    } finally {
      this.setLoading(false);
    }
  }

  async updateResource(): Promise<void> {
    const typeName = this.selectedResourceType();
    const identifier = this.updateIdentifier().trim();
    const updateProfile = this.selectedUpdateProfile();

    if (typeName.length === 0 || identifier.length === 0) {
      this.errorMessage.set('Informe tipo e identifier para atualizar.');
      return;
    }

    if (!updateProfile) {
      this.errorMessage.set('Nao existe mapeamento de update disponivel para este recurso.');
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      const desiredState =
        updateProfile.kind === 'tags'
          ? this.clearAllUpdateTags()
            ? { Tags: [] }
            : (() => {
                const tags = parseTagRows(this.updateTagRows());
                return tags.length > 0 ? { Tags: tags } : {};
              })()
          : parseResourceFieldRows(this.updateDesiredStateRows(), 'DesiredState', true);

      if (Object.keys(desiredState).length === 0) {
        throw new Error('Informe ao menos um campo para atualizar.');
      }

      await this.apiRequest('/api/resources', {
        method: 'PUT',
        body: JSON.stringify({
          typeName,
          identifier,
          updateProfileId: updateProfile.id,
          desiredState
        })
      });

      this.infoMessage.set('Operacao de update enviada com sucesso.');
      this.closeUpdateModal();
      await this.switchContext();
      await this.refreshCurrentViewData();
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao atualizar recurso.');
    } finally {
      this.setLoading(false);
    }
  }

  async openDeleteFlow(resource: ResourceSummary): Promise<void> {
    try {
      await this.alignContextForResource(resource);
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao preparar delete.');
      return;
    }

    this.deleteCandidate.set(resource);
    this.deleteConfirmationText.set('');
    this.deleteIntentId.set(null);
    this.clearMessages();
  }

  cancelDeleteFlow(): void {
    this.deleteCandidate.set(null);
    this.deleteConfirmationText.set('');
    this.deleteIntentId.set(null);
  }

  async requestDeleteIntent(): Promise<void> {
    const candidate = this.deleteCandidate();
    const expectedConfirmation = this.deleteConfirmationToken();

    if (!candidate) {
      return;
    }

    if (this.deleteConfirmationText().trim() !== expectedConfirmation) {
      this.errorMessage.set(
        `Digite o nome exato do recurso (${expectedConfirmation}) para gerar a segunda confirmacao.`
      );
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      await this.alignContextForResource(candidate);

      const response = await this.apiRequest<DeleteIntentResponse>('/api/resources/delete-intent', {
        method: 'POST',
        body: JSON.stringify({
          typeName: candidate.typeName,
          resourceId: candidate.identifier
        })
      });

      this.deleteIntentId.set(response.intentId);
      this.infoMessage.set('Segunda confirmacao gerada. Execute o delete definitivo.');
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao gerar confirmacao.');
    } finally {
      this.setLoading(false);
    }
  }

  async executeDelete(): Promise<void> {
    const candidate = this.deleteCandidate();
    const intentId = this.deleteIntentId();

    if (!candidate || !intentId) {
      this.errorMessage.set('Gere a segunda confirmacao antes de deletar.');
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      await this.alignContextForResource(candidate);

      await this.apiRequest('/api/resources', {
        method: 'DELETE',
        body: JSON.stringify({
          intentId,
          typeName: candidate.typeName,
          resourceId: candidate.identifier
        })
      });

      this.infoMessage.set('Delete concluido com sucesso.');
      this.cancelDeleteFlow();
      await this.refreshCurrentViewData();
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao deletar recurso.');
    } finally {
      this.setLoading(false);
    }
  }

  async loadAdminUsers(): Promise<void> {
    if (!this.isAdmin()) {
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      const response = await this.apiRequest<AdminUsersResponse>('/api/admin/users');
      this.adminUsers.set(response.users);

      const selectedUserId = this.selectedAdminUserId();
      const selectedStillExists = response.users.some((entry) => entry.id === selectedUserId);
      const fallbackUserId = response.users[0]?.id ?? '';

      this.setAdminSelection(selectedStillExists ? selectedUserId : fallbackUserId);
      this.infoMessage.set('Usuarios administrativos atualizados.');
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao carregar usuarios.');
    } finally {
      this.setLoading(false);
    }
  }

  setAdminSelection(userId: string): void {
    this.selectedAdminUserId.set(userId);
    const selected = this.adminUsers().find((entry) => entry.id === userId);

    if (!selected) {
      this.adminEditName.set('');
      this.adminEditEmail.set('');
      this.adminEditRole.set('viewer');
      this.adminEditPassword.set('');
      this.adminAccountsRows.set([]);
      this.adminPermissionsRows.set([]);
      this.adminDeleteConfirmationText.set('');
      this.adminDeleteIntentId.set(null);
      return;
    }

    this.adminEditName.set(selected.name);
    this.adminEditEmail.set(selected.email);
    this.adminEditRole.set(selected.role);
    this.adminEditPassword.set('');
    this.adminAccountsRows.set(mapAccountsToRows(selected.accounts));
    this.adminPermissionsRows.set(mapPermissionsToRows(selected.permissions));
    this.adminDeleteConfirmationText.set('');
    this.adminDeleteIntentId.set(null);
  }

  async createAdminUser(): Promise<void> {
    if (!this.isAdmin()) {
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      const roleValue = this.adminCreateRole();
      if (!isValidRole(roleValue)) {
        throw new Error('Role invalida para criacao.');
      }

      const accounts = parseAccountRows(this.adminCreateAccountsRows());
      if (accounts.length === 0) {
        throw new Error('Informe ao menos uma conta para criar o usuario.');
      }

      const response = await this.apiRequest<AdminUserResponse>('/api/admin/users', {
        method: 'POST',
        body: JSON.stringify({
          name: this.adminCreateName().trim(),
          email: this.adminCreateEmail().trim(),
          password: this.adminCreatePassword(),
          role: roleValue,
          accounts
        })
      });

      this.adminCreateName.set('');
      this.adminCreateEmail.set('');
      this.adminCreatePassword.set('change-me-please');
      await this.loadAdminUsers();
      this.setAdminSelection(response.user.id);
      this.infoMessage.set('Usuario criado com sucesso.');
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao criar usuario.');
    } finally {
      this.setLoading(false);
    }
  }

  async updateAdminUser(): Promise<void> {
    const selected = this.selectedAdminUser();
    if (!selected) {
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      const roleValue = this.adminEditRole();
      if (!isValidRole(roleValue)) {
        throw new Error('Role invalida para atualizacao.');
      }

      const payload: Record<string, unknown> = {
        name: this.adminEditName().trim(),
        email: this.adminEditEmail().trim(),
        role: roleValue
      };

      if (this.adminEditPassword().trim().length > 0) {
        payload.password = this.adminEditPassword().trim();
      }

      await this.apiRequest(`/api/admin/users/${selected.id}`, {
        method: 'PATCH',
        body: JSON.stringify(payload)
      });

      await this.loadAdminUsers();
      this.setAdminSelection(selected.id);
      this.infoMessage.set('Usuario atualizado com sucesso.');
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao atualizar usuario.');
    } finally {
      this.setLoading(false);
    }
  }

  async saveAdminAccounts(): Promise<void> {
    const selected = this.selectedAdminUser();
    if (!selected) {
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      const accounts = parseAccountRows(this.adminAccountsRows());

      await this.apiRequest(`/api/admin/users/${selected.id}/accounts`, {
        method: 'PUT',
        body: JSON.stringify({
          accounts
        })
      });

      await this.loadAdminUsers();
      this.setAdminSelection(selected.id);
      this.infoMessage.set('Contas atualizadas com sucesso.');
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao salvar contas.');
    } finally {
      this.setLoading(false);
    }
  }

  async saveAdminPermissions(): Promise<void> {
    const selected = this.selectedAdminUser();
    if (!selected) {
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      const permissions = parsePermissionRows(
        this.adminPermissionsRows(),
        selected.accounts.map((entry) => entry.accountId)
      );

      const response = await this.apiRequest<PermissionResponse>(
        `/api/admin/users/${selected.id}/permissions`,
        {
          method: 'PUT',
          body: JSON.stringify({
            permissions
          })
        }
      );

      this.adminPermissionsRows.set(mapPermissionsToRows(response.permissions));
      await this.loadAdminUsers();
      this.setAdminSelection(selected.id);
      this.infoMessage.set('Permissoes atualizadas com sucesso.');
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao salvar permissoes.');
    } finally {
      this.setLoading(false);
    }
  }

  async resetAdminPermissions(): Promise<void> {
    const selected = this.selectedAdminUser();
    if (!selected) {
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      const response = await this.apiRequest<PermissionResponse>(
        `/api/admin/users/${selected.id}/permissions/reset`,
        {
          method: 'POST'
        }
      );

      this.adminPermissionsRows.set(mapPermissionsToRows(response.permissions));
      await this.loadAdminUsers();
      this.setAdminSelection(selected.id);
      this.infoMessage.set('Permissoes resetadas para o padrao do perfil.');
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao resetar permissoes.');
    } finally {
      this.setLoading(false);
    }
  }

  async requestAdminDeleteIntent(): Promise<void> {
    const selected = this.selectedAdminUser();
    if (!selected) {
      return;
    }

    if (this.adminDeleteConfirmationText().trim() !== 'DELETE') {
      this.errorMessage.set('Digite DELETE para gerar a segunda confirmacao do usuario.');
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      const response = await this.apiRequest<DeleteIntentResponse>(
        `/api/admin/users/${selected.id}/delete-intent`,
        {
          method: 'POST'
        }
      );

      this.adminDeleteIntentId.set(response.intentId);
      this.infoMessage.set('Segunda confirmacao do usuario gerada. Execute a remocao definitiva.');
    } catch (error) {
      this.errorMessage.set(
        error instanceof Error ? error.message : 'Erro ao gerar confirmacao de remocao.'
      );
    } finally {
      this.setLoading(false);
    }
  }

  async deleteAdminUser(): Promise<void> {
    const selected = this.selectedAdminUser();
    if (!selected) {
      return;
    }

    const intentId = this.adminDeleteIntentId();
    if (!intentId) {
      this.errorMessage.set('Gere a segunda confirmacao antes de remover o usuario.');
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      await this.apiRequest(`/api/admin/users/${selected.id}`, {
        method: 'DELETE',
        body: JSON.stringify({
          intentId
        })
      });

      await this.loadAdminUsers();
      this.infoMessage.set('Usuario removido com sucesso.');
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao remover usuario.');
    } finally {
      this.setLoading(false);
    }
  }

  logout(): void {
    this.token.set(null);
    this.user.set(null);
    this.authMode.set('login');
    this.toasts.set([]);
    this.workspaceView.set('resources');
    this.resourceTypes.set([]);
    this.selectedResourceType.set('');
    this.resources.set([]);
    this.resourceDetails.set(null);
    this.resourceStateHistory.set([]);
    this.checkupCounts.set({});
    this.deleteCandidate.set(null);
    this.deleteIntentId.set(null);
    this.deleteConfirmationText.set('');
    this.adminUsers.set([]);
    this.setAdminSelection('');
    this.adminDeleteConfirmationText.set('');
    this.adminDeleteIntentId.set(null);
    this.finopsOverview.set(null);
    this.finopsStaleDays.set(DEFAULT_FINOPS_STALE_DAYS);
    window.localStorage.removeItem(TOKEN_STORAGE_KEY);
    clearPersistedHeaderSelection();
  }

  private async alignContextForResource(resource: ResourceSummary): Promise<void> {
    const currentAccountId = this.selectedAccountId();
    const currentRegion = this.selectedRegion();

    if (currentAccountId === resource.accountId && currentRegion === resource.region) {
      return;
    }

    this.selectedAccountId.set(resource.accountId);
    this.selectedRegion.set(resource.region);
    await this.switchContext();
  }

  private async restoreSession(): Promise<void> {
    const existingToken = this.token();

    if (!existingToken) {
      return;
    }

    this.setLoading(true);

    try {
      const response = await this.apiRequest<{ user: PublicUser }>('/api/auth/me');
      this.user.set(response.user);
      this.restoreHeaderContextFromUser(response.user);
      await this.switchContext();
      await this.refreshAdminStateIfNeeded();
    } catch {
      this.logout();
    } finally {
      this.setLoading(false);
    }
  }

  private async refreshAdminStateIfNeeded(): Promise<void> {
    if (this.isAdmin()) {
      await this.loadAdminUsers();
      return;
    }

    this.adminUsers.set([]);
    this.setAdminSelection('');
    this.workspaceView.set('resources');
  }

  private async switchContext(): Promise<void> {
    const accountId = this.selectedAccountId();
    const region = this.selectedRegion();

    if (accountId.length === 0 || region.length === 0) {
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      const response = await this.apiRequest<ContextSwitchResponse>('/api/context/switch', {
        method: 'POST',
        body: JSON.stringify({
          accountId,
          region,
          category: this.selectedCategory()
        })
      });

      this.checkupCounts.set(response.checkup.resourceCounts);

      const defaultResourceTypesForCategory = getDefaultResourceTypesForCategory(
        this.selectedCategory()
      );
      const mergedResourceTypes = [
        ...new Set([
          ...response.resourceTypes,
          ...defaultResourceTypesForCategory
        ])
      ];
      this.resourceTypes.set(mergedResourceTypes);

      const currentType = this.selectedResourceType();
      const fallbackType = mergedResourceTypes[0] ?? '';
      const typeStillAvailable = currentType.length > 0 && mergedResourceTypes.includes(currentType);
      const nextType = typeStillAvailable ? currentType : fallbackType;

      this.selectedResourceType.set(nextType);
      this.applyTemplateDrivenRows(this.selectedResourceType());

      if (response.resourceTypes.length === 0) {
        this.resources.set([]);
        this.resourceDetails.set(null);
        this.finopsOverview.set(null);
        this.infoMessage.set(
          `Categoria ${this.selectedCategoryLabel()} sem tipos de recurso disponiveis para a conta/regiao atual.`
        );
        return;
      }

      if (this.workspaceView() === 'finops') {
        await this.loadFinopsOverview();
        return;
      }

      await this.loadResources();

      if (response.checkupWarning && response.checkupWarning.length > 0) {
        this.errorMessage.set(
          `Contexto atualizado com alerta no check-up: ${response.checkupWarning}`
        );
      }
    } finally {
      this.setLoading(false);
    }
  }

  private async loadResources(): Promise<void> {
    const typeName = this.selectedResourceType();
    const region = this.selectedRegion();

    if (typeName.length === 0) {
      this.resources.set([]);
      this.selectedResource.set(null);
      this.resourceDetails.set(null);
      this.resourceStateHistory.set([]);
      this.resourceStateHistoryScope.set('context');
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      const query = new URLSearchParams({ typeName });
      const response = await this.apiRequest<ResourceListResponse>(`/api/resources?${query.toString()}`);

      this.resources.set(response.resources);

      if (response.resources.length === 0) {
        this.selectedResource.set(null);
        this.resourceDetails.set(null);
        this.resourceStateHistory.set([]);
        this.resourceStateHistoryScope.set('context');
      } else {
        const currentSelection = this.selectedResource();
        const nextSelectedResource =
          response.resources.find((resource) => this.isSameResource(resource, currentSelection)) ??
          response.resources[0];

        await this.loadResourceDetails(nextSelectedResource, { silent: true });
      }

      this.infoMessage.set(`Inventario concluido para ${region}: ${response.resources.length} recurso(s) encontrados.`);
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao listar recursos.');
    } finally {
      this.setLoading(false);
    }
  }

  private restoreHeaderContextFromUser(user: PublicUser): void {
    const persistedSelection = readPersistedHeaderSelection();
    const category = persistedSelection?.category ?? this.selectedCategory();
    const persistedResourceType = persistedSelection?.resourceType.trim() ?? '';
    const defaultResourceTypes = getDefaultResourceTypesForCategory(category);
    const nextResourceTypes = [
      ...new Set([
        ...defaultResourceTypes,
        ...(
          persistedResourceType.length > 0
            ? [persistedResourceType]
            : []
        )
      ])
    ];
    const selectedAccount =
      user.accounts.find((entry) => entry.accountId === persistedSelection?.accountId) ??
      user.accounts[0];

    this.selectedCategory.set(category);
    this.resourceTypes.set(nextResourceTypes);
    this.selectedResourceType.set(
      persistedResourceType.length > 0
        ? persistedResourceType
        : nextResourceTypes[0] ?? ''
    );
    this.applyTemplateDrivenRows(this.selectedResourceType());

    if (!selectedAccount) {
      this.selectedAccountId.set('');
      this.selectedRegion.set('');
      return;
    }

    const nextRegion = selectedAccount.allowedRegions.includes(persistedSelection?.region ?? '')
      ? persistedSelection?.region ?? ''
      : selectedAccount.allowedRegions[0] ?? '';

    this.selectedAccountId.set(selectedAccount.accountId);
    this.selectedRegion.set(nextRegion);
  }

  private captureContextSelection(): ContextSelectionSnapshot {
    return {
      category: this.selectedCategory(),
      accountId: this.selectedAccountId(),
      region: this.selectedRegion()
    };
  }

  private restoreContextSelection(snapshot: ContextSelectionSnapshot): void {
    this.selectedCategory.set(snapshot.category);
    this.selectedAccountId.set(snapshot.accountId);
    this.selectedRegion.set(snapshot.region);
  }

  isSelectedResource(resource: ResourceSummary): boolean {
    return this.isSameResource(resource, this.selectedResource());
  }

  private isSameResource(
    left: ResourceSummary | null | undefined,
    right: ResourceSummary | null | undefined
  ): boolean {
    if (!left || !right) {
      return false;
    }

    return (
      left.accountId === right.accountId &&
      left.region === right.region &&
      left.typeName === right.typeName &&
      left.identifier === right.identifier
    );
  }

  private async updateContextSelection(
    applySelection: () => void,
    fallbackMessage: string
  ): Promise<void> {
    const previousSelection = this.captureContextSelection();
    applySelection();

    try {
      await this.switchContext();
    } catch (error) {
      this.restoreContextSelection(previousSelection);
      this.errorMessage.set(error instanceof Error ? error.message : fallbackMessage);
    }
  }

  private applyTemplateDrivenRows(resourceType: string): void {
    const template = this.resourceTemplates().find((entry) => entry.typeName === resourceType);
    const defaultUpdateProfile = getDefaultResourceUpdateProfile(resourceType);

    this.createPayloadRows.set(buildTemplateCreateRows(template));
    this.createPayloadText.set(JSON.stringify(buildTemplateCreateSeedState(template), null, 2));
    this.selectedUpdateProfileId.set(defaultUpdateProfile?.id ?? '');
    this.updateDesiredStateRows.set(buildFocusedUpdateRows(defaultUpdateProfile));
    this.updateTagRows.set([createEmptyTagRow()]);
    this.clearAllUpdateTags.set(false);
    this.showCreateOptionalTemplateFields.set(false);
    this.showUpdateOptionalTemplateFields.set(false);
    this.showCreateCustomFields.set(false);
    this.showUpdateCustomFields.set(false);
    this.showUpdatePatchEditor.set(false);
    this.updateIdentifier.set('');
    this.useJsonCreatePayload.set(false);
    this.useJsonUpdateDesiredState.set(false);
    this.useJsonPatchPayload.set(false);
    this.updatePatchRows.set([createEmptyPatchRow()]);
    this.updatePatchText.set(
      '[\n  {"op": "replace", "path": "/Tags", "value": [{"Key": "managed-by", "Value": "platform"}]}\n]'
    );
  }

  readEventValue(event: Event): string {
    const target = event.target as HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement | null;

    return target?.value ?? '';
  }

  trackByIndex = (_index: number, row: { index?: number }): number => row.index ?? _index;
  trackByFieldId = (_index: number, row: { id: string }): string => row.id;
  trackByToastId = (_index: number, toast: ToastItem): string => toast.id;

  private getStructuredUpdateFieldRow(fieldId: string): TemplateAwareResourceFieldRow | undefined {
    return this.updateTemplateRows().find((entry) => entry.id === fieldId);
  }

  private getStructuredUpdateFieldObject(fieldId: string): Record<string, unknown> {
    const row = this.getStructuredUpdateFieldRow(fieldId);
    return row ? parseStructuredFieldObject(row.value) : {};
  }

  private setStructuredUpdateFieldObject(fieldId: string, value: Record<string, unknown>): void {
    this.updateUpdateDesiredStateFieldRowById(fieldId, {
      value: stringifyStructuredFieldObject(value)
    });
  }

  private getStructuredUpdateFieldEncryptionConfiguration(fieldId: string): Record<string, unknown> {
    const current = this.getStructuredUpdateFieldObject(fieldId);
    const rules = current.ServerSideEncryptionConfiguration;

    if (!Array.isArray(rules) || rules.length === 0 || !isPlainObject(rules[0])) {
      return {};
    }

    const defaultEncryption = (rules[0] as Record<string, unknown>).ApplyServerSideEncryptionByDefault;
    return isPlainObject(defaultEncryption) ? defaultEncryption : {};
  }

  private setStructuredUpdateFieldEncryptionConfiguration(
    fieldId: string,
    configuration: Record<string, unknown>
  ): void {
    if (!configuration.SSEAlgorithm) {
      this.setStructuredUpdateFieldObject(fieldId, {});
      return;
    }

    this.setStructuredUpdateFieldObject(fieldId, {
      ServerSideEncryptionConfiguration: [
        {
          ApplyServerSideEncryptionByDefault: configuration
        }
      ]
    });
  }

  private saveSession(token: string, user: PublicUser): void {
    this.token.set(token);
    this.user.set(user);
    window.localStorage.setItem(TOKEN_STORAGE_KEY, token);
  }

  private setLoading(nextState: boolean): void {
    this.loading.set(nextState);
  }

  private clearMessages(): void {
    this.errorMessage.set('');
    this.infoMessage.set('');
  }

  dismissToast(toastId: string): void {
    this.toasts.set(this.toasts().filter((toast) => toast.id !== toastId));
  }

  private enqueueToast(kind: ToastKind, message: string): void {
    const nextToast: ToastItem = {
      id: createToastId(),
      kind,
      message
    };

    this.toasts.update((currentToasts) => [...currentToasts.slice(-3), nextToast]);

    setTimeout(() => {
      this.dismissToast(nextToast.id);
    }, 4200);
  }

  private async apiRequest<T>(path: string, init?: RequestInit): Promise<T> {
    const headers = new Headers(init?.headers);

    if (init?.body && !headers.has('Content-Type')) {
      headers.set('Content-Type', 'application/json');
    }

    const token = this.token();
    if (token) {
      headers.set('Authorization', `Bearer ${token}`);
    }

    const response = await fetch(`${API_BASE_URL}${path}`, {
      ...init,
      headers
    });

    const rawBody = await response.text();
    const parsedBody = rawBody.length > 0 ? safeJsonParse(rawBody) : {};

    if (!response.ok) {
      if (typeof parsedBody === 'object' && parsedBody !== null && 'message' in parsedBody) {
        const bodyWithMessage = parsedBody as { message?: unknown };
        if (typeof bodyWithMessage.message === 'string') {
          throw new Error(bodyWithMessage.message);
        }
      }

      throw new Error(`Falha na requisicao (${response.status}).`);
    }

    return parsedBody as T;
  }
}
