import { CommonModule } from '@angular/common';
import { Component, computed, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import type {
  AwsAccount,
  AwsCategory,
  PermissionScope,
  ResourceSummary,
  UserRole
} from '@platform/shared';

type CategoryDefinition = {
  id: AwsCategory;
  label: string;
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

const parseAsObjectArray = (rawText: string, fieldName: string): readonly Record<string, unknown>[] => {
  const parsedValue = safeJsonParse(rawText);

  if (!Array.isArray(parsedValue)) {
    throw new Error(`${fieldName} deve ser um array JSON.`);
  }

  const allObjects = parsedValue.every(
    (entry) => typeof entry === 'object' && entry !== null && !Array.isArray(entry)
  );

  if (!allObjects) {
    throw new Error(`${fieldName} deve conter apenas objetos.`);
  }

  return parsedValue as readonly Record<string, unknown>[];
};

const asString = (input: unknown): string => (typeof input === 'string' ? input.trim() : '');

const isValidRole = (value: string): value is UserRole => validRoles.includes(value as UserRole);

const isValidCategory = (value: string): value is AwsCategory =>
  categoryDefinitions.some((category) => category.id === value);

const isValidPermissionCategory = (value: string): value is AwsCategory | '*' =>
  value === '*' || isValidCategory(value);

const parseAccounts = (rawText: string): readonly AwsAccount[] =>
  parseAsObjectArray(rawText, 'Accounts').map((entry, index) => {
    const accountId = asString(entry.accountId);
    const name = asString(entry.name);
    const regionsRaw = entry.allowedRegions;
    const allowedRegions = Array.isArray(regionsRaw)
      ? regionsRaw.filter((region): region is string => typeof region === 'string')
      : [];

    if (!/^\d{12}$/.test(accountId)) {
      throw new Error(`Account #${index + 1}: accountId invalido.`);
    }

    if (name.length === 0) {
      throw new Error(`Account #${index + 1}: name obrigatorio.`);
    }

    if (allowedRegions.length === 0) {
      throw new Error(`Account #${index + 1}: informe ao menos uma regiao.`);
    }

    return {
      accountId,
      name,
      allowedRegions
    };
  });

const parsePermissions = (rawText: string): readonly PermissionScope[] =>
  parseAsObjectArray(rawText, 'Permissions').map((entry, index) => {
    const accountId = asString(entry.accountId);
    const category = asString(entry.category);
    const resourceType = asString(entry.resourceType);
    const action = asString(entry.action);

    if (accountId.length === 0) {
      throw new Error(`Permission #${index + 1}: accountId obrigatorio.`);
    }

    if (!isValidPermissionCategory(category)) {
      throw new Error(`Permission #${index + 1}: category invalida.`);
    }

    if (resourceType.length === 0) {
      throw new Error(`Permission #${index + 1}: resourceType obrigatorio.`);
    }

    if (!validActions.includes(action as (typeof validActions)[number])) {
      throw new Error(`Permission #${index + 1}: action invalida.`);
    }

    return {
      accountId,
      category,
      resourceType,
      action: action as (typeof validActions)[number]
    };
  });

const stringifyPretty = (value: unknown): string => JSON.stringify(value, null, 2);

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

  readonly loginEmail = signal('admin@platform.local');
  readonly loginPassword = signal('change-me-please');

  readonly token = signal<string | null>(window.localStorage.getItem(TOKEN_STORAGE_KEY));
  readonly user = signal<PublicUser | null>(null);
  readonly workspaceView = signal<WorkspaceView>('resources');

  readonly selectedCategory = signal<AwsCategory>('compute');
  readonly selectedAccountId = signal('');
  readonly selectedRegion = signal('');
  readonly resourceTypes = signal<readonly string[]>([]);
  readonly selectedResourceType = signal('');

  readonly checkupCounts = signal<Record<string, number>>({});
  readonly resources = signal<readonly ResourceSummary[]>([]);
  readonly resourceDiscoveryRegions = signal<readonly DiscoveryRegionSummary[]>([]);
  readonly resourceDetails = signal<ResourceDetailsResponse | null>(null);

  readonly createPayloadText = signal('{\n  "BucketName": "example-bucket-name"\n}');
  readonly updateIdentifier = signal('');
  readonly updateDesiredStateText = signal(
    '{\n  "Tags": [\n    {"Key": "managed-by", "Value": "platform"}\n  ]\n}'
  );
  readonly updatePatchText = signal(
    '[\n  {"op": "replace", "path": "/Tags", "value": [{"Key": "managed-by", "Value": "platform"}]}\n]'
  );

  readonly deleteCandidate = signal<ResourceSummary | null>(null);
  readonly deleteConfirmationText = signal('');
  readonly deleteIntentId = signal<string | null>(null);

  readonly adminUsers = signal<readonly AdminUser[]>([]);
  readonly selectedAdminUserId = signal('');
  readonly adminCreateName = signal('');
  readonly adminCreateEmail = signal('');
  readonly adminCreatePassword = signal('change-me-please');
  readonly adminCreateRole = signal<UserRole>('viewer');
  readonly adminCreateAccountsText = signal(
    '[\n  {\n    "accountId": "222222222222",\n    "name": "Sandbox",\n    "allowedRegions": ["us-east-1"]\n  }\n]'
  );

  readonly adminEditName = signal('');
  readonly adminEditEmail = signal('');
  readonly adminEditRole = signal<UserRole>('viewer');
  readonly adminEditPassword = signal('');
  readonly adminAccountsText = signal('[]');
  readonly adminPermissionsText = signal('[]');
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
  readonly selectedCategoryLabel = computed(
    () => this.categories.find((entry) => entry.id === this.selectedCategory())?.label ?? this.selectedCategory()
  );

  constructor() {
    void this.restoreSession();
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
    await this.loadResources();
  }

  async refreshResources(): Promise<void> {
    await this.loadResources();
  }

  async loadResourceDetails(resource: ResourceSummary): Promise<void> {
    this.clearMessages();

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
      this.errorMessage.set(error instanceof Error ? error.message : 'Erro ao carregar detalhes.');
    }
  }

  async createResource(): Promise<void> {
    const typeName = this.selectedResourceType();

    if (typeName.length === 0) {
      this.errorMessage.set('Selecione o tipo de recurso para criar.');
      return;
    }

    this.setLoading(true);
    this.clearMessages();

    try {
      const desiredState = parseAsObject(this.createPayloadText(), 'DesiredState');

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
      const desiredState = parseAsObject(this.updateDesiredStateText(), 'DesiredState');
      const patchDocument = parseAsPatchArray(this.updatePatchText());

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
    this.clearMessages();
  }

  cancelDeleteFlow(): void {
    this.deleteCandidate.set(null);
    this.deleteConfirmationText.set('');
    this.deleteIntentId.set(null);
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
      this.adminAccountsText.set('[]');
      this.adminPermissionsText.set('[]');
      this.adminDeleteConfirmationText.set('');
      this.adminDeleteIntentId.set(null);
      return;
    }

    this.adminEditName.set(selected.name);
    this.adminEditEmail.set(selected.email);
    this.adminEditRole.set(selected.role);
    this.adminEditPassword.set('');
    this.adminAccountsText.set(stringifyPretty(selected.accounts));
    this.adminPermissionsText.set(stringifyPretty(selected.permissions));
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

      const accounts = parseAccounts(this.adminCreateAccountsText());

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
      const accounts = parseAccounts(this.adminAccountsText());

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
      const permissions = parsePermissions(this.adminPermissionsText());

      const response = await this.apiRequest<PermissionResponse>(
        `/api/admin/users/${selected.id}/permissions`,
        {
          method: 'PUT',
          body: JSON.stringify({
            permissions
          })
        }
      );

      this.adminPermissionsText.set(stringifyPretty(response.permissions));
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

      this.adminPermissionsText.set(stringifyPretty(response.permissions));
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
    this.resourceTypes.set(response.resourceTypes);

    const currentType = this.selectedResourceType();
    const fallbackType = response.resourceTypes[0] ?? '';
    const typeStillAvailable = response.resourceTypes.includes(currentType);

    this.selectedResourceType.set(typeStillAvailable ? currentType : fallbackType);

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
    this.resources.set([]);
    this.resourceDiscoveryRegions.set([]);
    this.resourceDetails.set(null);
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
