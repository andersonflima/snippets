import { CommonModule } from '@angular/common';
import { Component, OnDestroy, computed, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import type {
  AwsAccount,
  AwsCategory,
  ResourceTemplate,
  ResourceStateRecord,
  PermissionScope,
  ResourceSummary,
  UserRole
} from '@platform/shared';
import { getResourceTemplates } from '@platform/shared';

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
type ResourceOperationTab = 'create' | 'update' | 'delete';

type PermissionFormRow = {
  accountId: string;
  category: AwsCategory | '*';
  resourceType: string;
  action: ResourceAction;
};

type ResourceFieldValueMode = 'text' | 'number' | 'boolean' | 'json' | 'null';

type ResourceFieldRow = {
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

type DiscoveryRegionSummary = {
  region: string;
  status: 'ok' | 'error';
  total: number;
  message?: string;
};

type ResourceDiscoveryResponse = {
  accountId: string;
  category: AwsCategory;
  typeName: string;
  totalResources: number;
  regions: readonly DiscoveryRegionSummary[];
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

type WorkspaceView = 'resources' | 'admin';

const TOKEN_STORAGE_KEY = 'platform.token';
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

const topAwsResourceTypes: readonly string[] = getResourceTemplates().map((template) => template.typeName);

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

const createEmptyAccountRow = (): AwsAccountFormRow => ({
  accountId: '',
  name: '',
  allowedRegions: ['us-east-1']
});

const createEmptyResourceFieldRow = (): TemplateAwareResourceFieldRow => ({
  key: '',
  value: '',
  valueMode: 'text',
  required: false,
  fieldType: 'custom'
});

const templateCreateValue = (field: ResourceTemplate['fields'][number]): string =>
  field.required ? '' : toTemplateDefaultText(field.defaultValue);

const createEmptyPatchRow = (): ResourcePatchRow => ({
  op: 'replace',
  path: '',
  value: '',
  valueMode: 'text',
  from: ''
});

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

const buildTemplateCreateRows = (
  template: ResourceTemplate | undefined
): readonly TemplateAwareResourceFieldRow[] => {
  if (!template || template.fields.length === 0) {
    return [createEmptyResourceFieldRow()];
  }

  const rows = template.fields.map((field) => ({
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
      accumulator[field.key] = '';
      return accumulator;
    }

    if (field.defaultValue !== undefined) {
      accumulator[field.key] = field.defaultValue;
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

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './app.component.html',
  styleUrl: './app.component.css'
})
export class AppComponent implements OnDestroy {
  private readonly operationBreakpoint = 980;
  private mediaQuery: MediaQueryList | null = null;
  private mediaQueryListener: ((event: MediaQueryListEvent) => void) | null = null;

  readonly categories = categoryDefinitions;
  readonly loading = signal(false);
  readonly errorMessage = signal('');
  readonly infoMessage = signal('');

  readonly loginEmail = signal('admin@platform.local');
  readonly loginPassword = signal('change-me-please');
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

  readonly selectedCategory = signal<AwsCategory>('compute');
  readonly selectedAccountId = signal('');
  readonly selectedRegion = signal('');
  readonly resourceTypes = signal<readonly string[]>([]);
  readonly selectedResourceType = signal('');
  readonly resourceFieldValueModes = resourceFieldValueModes;
  readonly patchOperationOptions = patchOperationOptions;
  readonly resourceTemplates = signal<readonly ResourceTemplate[]>(getResourceTemplates());
  readonly selectedResourceTemplate = computed(
    () => this.resourceTemplates().find((template) => template.typeName === this.selectedResourceType())
  );

  readonly checkupCounts = signal<Record<string, number>>({});
  readonly resources = signal<readonly ResourceSummary[]>([]);
  readonly resourceDiscoveryRegions = signal<readonly DiscoveryRegionSummary[]>([]);
  readonly resourceDetails = signal<ResourceDetailsResponse | null>(null);
  readonly resourceStateHistory = signal<readonly ResourceStateRecord[]>([]);
  readonly awsRegionOptions = awsCommonRegions;
  readonly permissionCategoryOptions = permissionCategoryOptions;
  readonly actionOptions = validActions;
  readonly formatStateDate = formatStateDate;

  readonly createPayloadText = signal('{}');
  readonly createPayloadRows = signal<readonly TemplateAwareResourceFieldRow[]>([
    createEmptyResourceFieldRow()
  ]);
  readonly useJsonCreatePayload = signal(false);
  readonly resourceActionTab = signal<ResourceOperationTab>('create');
  readonly isMobileResourceActions = signal(false);
  readonly updateIdentifier = signal('');
  readonly updateDesiredStateText = signal(
    '{\n  "Tags": [\n    {"Key": "managed-by", "Value": "platform"}\n  ]\n}'
  );
  readonly updateDesiredStateRows = signal<readonly TemplateAwareResourceFieldRow[]>([
    createEmptyResourceFieldRow()
  ]);
  readonly useJsonUpdateDesiredState = signal(false);
  readonly updatePatchText = signal(
    '[\n  {"op": "replace", "path": "/Tags", "value": [{"Key": "managed-by", "Value": "platform"}]}\n]'
  );
  readonly updatePatchRows = signal<readonly ResourcePatchRow[]>([createEmptyPatchRow()]);
  readonly useJsonPatchPayload = signal(false);

  readonly deleteCandidate = signal<ResourceSummary | null>(null);
  readonly deleteConfirmationText = signal('');
  readonly deleteIntentId = signal<string | null>(null);

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
  readonly discoveryHealthyRegionCount = computed(
    () => this.resourceDiscoveryRegions().filter((entry) => entry.status === 'ok').length
  );
  readonly discoveryFailureCount = computed(
    () => this.resourceDiscoveryRegions().filter((entry) => entry.status === 'error').length
  );
  readonly activeResourceActionTab = computed(() => {
    if (this.isMobileResourceActions() && this.deleteCandidate()) {
      return 'delete' as const;
    }

    return this.resourceActionTab();
  });
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
    this.setupOperationViewportObserver();
    void this.restoreSession();
  }

  ngOnDestroy(): void {
    if (this.mediaQuery && this.mediaQueryListener) {
      this.mediaQuery.removeEventListener('change', this.mediaQueryListener);
      this.mediaQuery = null;
      this.mediaQueryListener = null;
    }
  }

  setResourceActionTab(action: ResourceOperationTab): void {
    this.resourceActionTab.set(action);
  }

  private setupOperationViewportObserver(): void {
    if (typeof window === 'undefined') {
      return;
    }

    const media = window.matchMedia(`(max-width: ${this.operationBreakpoint}px)`);
    this.mediaQuery = media;
    const onChange = (event: MediaQueryListEvent) => {
      this.isMobileResourceActions.set(event.matches);

      if (this.deleteCandidate()) {
        this.resourceActionTab.set('delete');
      }
    };

    this.mediaQueryListener = onChange;
    media.addEventListener('change', onChange);
    this.isMobileResourceActions.set(media.matches);
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
      this.setDefaultContextFromUser(response.user);
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
      this.setDefaultContextFromUser(response.user);
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

  removeUpdateDesiredStateFieldRow(index: number): void {
    this.updateDesiredStateRows.set(
      this.updateDesiredStateRows().filter((_, rowIndex) => rowIndex !== index)
    );

    if (this.updateDesiredStateRows().length === 0) {
      this.updateDesiredStateRows.set([createEmptyResourceFieldRow()]);
    }
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
    }
  }

  async onCategoryChange(category: AwsCategory): Promise<void> {
    this.selectedCategory.set(category);
    this.resetResourcePanelsForContextChange();

    try {
      await this.switchContext();
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao trocar categoria.');
    }
  }

  async onAccountChange(accountId: string): Promise<void> {
    this.selectedAccountId.set(accountId);
    this.resetResourcePanelsForContextChange();

    const regions = this.availableRegions();
    if (regions.length > 0) {
      this.selectedRegion.set(regions[0]);
    }

    try {
      await this.switchContext();
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao trocar conta.');
    }
  }

  async onRegionChange(region: string): Promise<void> {
    this.selectedRegion.set(region);
    this.resetResourcePanelsForContextChange();

    try {
      await this.switchContext();
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao trocar regiao.');
    }
  }

  async onResourceTypeChange(resourceType: string): Promise<void> {
    this.selectedResourceType.set(resourceType);
    this.applyTemplateDrivenRows(resourceType);
    this.resourceDetails.set(null);
    this.resourceStateHistory.set([]);
    await this.loadResources();
  }

  async refreshResources(): Promise<void> {
    await this.loadResources();
  }

  async loadResourceDetails(resource: ResourceSummary): Promise<void> {
    this.clearMessages();
    this.resourceStateHistory.set([]);

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
      await this.loadResourceStateHistory(resource.typeName, resource.identifier);
    } catch (error) {
      this.resourceDetails.set(null);
      this.resourceStateHistory.set([]);
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao carregar detalhes.');
    }
  }

  private async loadResourceStateHistory(typeName?: string, identifier?: string): Promise<void> {
    const query = new URLSearchParams();
    if (typeName && typeName.length > 0) {
      query.set('typeName', typeName);
    }
    if (identifier && identifier.length > 0) {
      query.set('identifier', identifier);
    }

    const queryString = query.toString();
    const endpoint = queryString.length > 0 ? `/api/resources/state?${queryString}` : '/api/resources/state';

    const response = await this.apiRequest<ResourceStateHistoryResponse>(endpoint);
    this.resourceStateHistory.set(response.history);
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
      await this.switchContext();
      await this.loadResources();
    } catch (error) {
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao criar recurso.');
    } finally {
      this.setLoading(false);
    }
  }

  async updateResource(): Promise<void> {
    const typeName = this.selectedResourceType();
    const identifier = this.updateIdentifier().trim();

    if (typeName.length === 0 || identifier.length === 0) {
      this.errorMessage.set('Informe tipo e identifier para atualizar.');
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      const desiredState = this.useJsonUpdateDesiredState()
        ? parseAsObject(this.updateDesiredStateText(), 'DesiredState')
        : parseResourceFieldRows(this.updateDesiredStateRows(), 'DesiredState', true);
      const patchDocument = this.useJsonPatchPayload()
        ? parseAsPatchArray(this.updatePatchText())
        : parsePatchRows(this.updatePatchRows(), 'Patch Document', true);

      const hasDesiredStateChanges = Object.keys(desiredState).length > 0;
      const hasPatchChanges = patchDocument.length > 0;

      if (!hasDesiredStateChanges && !hasPatchChanges) {
        throw new Error('Informe dados para desiredState ou patchDocument.');
      }

      await this.apiRequest('/api/resources', {
        method: 'PUT',
        body: JSON.stringify({
          typeName,
          identifier,
          desiredState,
          patchDocument
        })
      });

      this.infoMessage.set('Operacao de update enviada com sucesso.');
      await this.switchContext();
      await this.loadResources();
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
    this.setResourceActionTab('delete');
    this.clearMessages();
  }

  cancelDeleteFlow(): void {
    this.deleteCandidate.set(null);
    this.deleteConfirmationText.set('');
    this.deleteIntentId.set(null);
    this.setResourceActionTab('create');
  }

  async requestDeleteIntent(): Promise<void> {
    const candidate = this.deleteCandidate();

    if (!candidate) {
      return;
    }

    if (this.deleteConfirmationText().trim() !== 'DELETE') {
      this.errorMessage.set('Digite DELETE para gerar a segunda confirmacao.');
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
      await this.loadResources();
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
    this.workspaceView.set('resources');
    this.resourceTypes.set([]);
    this.selectedResourceType.set('');
    this.resources.set([]);
    this.resourceDiscoveryRegions.set([]);
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
    window.localStorage.removeItem(TOKEN_STORAGE_KEY);
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
      this.setDefaultContextFromUser(response.user);
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

    this.clearMessages();

    const response = await this.apiRequest<ContextSwitchResponse>('/api/context/switch', {
      method: 'POST',
      body: JSON.stringify({
        accountId,
        region,
        category: this.selectedCategory()
      })
    });

    this.checkupCounts.set(response.checkup.resourceCounts);

    const mergedResourceTypes = [...new Set([...response.resourceTypes, ...topAwsResourceTypes])];
    this.resourceTypes.set(mergedResourceTypes);

    const currentType = this.selectedResourceType();
    const fallbackType = mergedResourceTypes[0] ?? '';
    const typeStillAvailable = mergedResourceTypes.includes(currentType);

    this.selectedResourceType.set(typeStillAvailable ? currentType : fallbackType);
    this.applyTemplateDrivenRows(this.selectedResourceType());

    if (response.resourceTypes.length === 0) {
      this.resources.set([]);
      this.resourceDiscoveryRegions.set([]);
      this.resourceDetails.set(null);
      this.infoMessage.set(
        `Categoria ${this.selectedCategoryLabel()} sem tipos de recurso disponiveis para a conta/regiao atual.`
      );
    } else {
      await this.loadResources();
    }

    if (response.checkupWarning && response.checkupWarning.length > 0) {
      this.errorMessage.set(
        `Contexto atualizado com alerta no check-up: ${response.checkupWarning}`
      );
    }
  }

  private async loadResources(): Promise<void> {
    const typeName = this.selectedResourceType();

    if (typeName.length === 0) {
      this.resources.set([]);
      this.resourceDiscoveryRegions.set([]);
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      const query = new URLSearchParams({ typeName });
      const response = await this.apiRequest<ResourceDiscoveryResponse>(
        `/api/resources/discovery?${query.toString()}`
      );

      this.resources.set(response.resources);
      this.resourceDiscoveryRegions.set(response.regions);

      const healthyRegions = response.regions.filter((entry) => entry.status === 'ok').length;
      const failedRegions = response.regions.filter((entry) => entry.status === 'error').length;

      this.infoMessage.set(
        `Descoberta multi-regiao concluida: ${response.totalResources} recursos em ${healthyRegions} regioes${failedRegions > 0 ? ` (${failedRegions} com falha)` : ''}.`
      );
    } catch (error) {
      this.resourceDiscoveryRegions.set([]);
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao listar recursos.');
    } finally {
      this.setLoading(false);
    }
  }

  private resetResourcePanelsForContextChange(): void {
    this.resourceTypes.set([]);
    this.selectedResourceType.set('');
    this.createPayloadRows.set([createEmptyResourceFieldRow()]);
    this.updateDesiredStateRows.set([createEmptyResourceFieldRow()]);
    this.resources.set([]);
    this.resourceDiscoveryRegions.set([]);
    this.resourceDetails.set(null);
    this.resourceStateHistory.set([]);
    this.checkupCounts.set({});
  }

  private setDefaultContextFromUser(user: PublicUser): void {
    const firstAccount = user.accounts[0];

    if (!firstAccount) {
      return;
    }

    this.selectedAccountId.set(firstAccount.accountId);

    const firstRegion = firstAccount.allowedRegions[0] ?? '';
    this.selectedRegion.set(firstRegion);
  }

  private applyTemplateDrivenRows(resourceType: string): void {
    const template = this.resourceTemplates().find((entry) => entry.typeName === resourceType);

    this.createPayloadRows.set(buildTemplateCreateRows(template));
    this.createPayloadText.set(JSON.stringify(buildTemplateCreateSeedState(template), null, 2));
    this.updateDesiredStateRows.set(buildTemplateUpdateRows(template));
    this.updateIdentifier.set('');
    this.useJsonCreatePayload.set(false);
    this.useJsonUpdateDesiredState.set(false);
    this.useJsonPatchPayload.set(false);
    this.updatePatchRows.set([createEmptyPatchRow()]);
    this.updatePatchText.set(
      '[\n  {"op": "replace", "path": "/Tags", "value": [{"Key": "managed-by", "Value": "platform"}]}\n]'
    );
  }

  trackByIndex = (_index: number): number => _index;

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
