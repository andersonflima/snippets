import { Routes } from '@angular/router';
import { IntegrationsComponent } from './features/integrations.component';
import { RunActionComponent } from './features/run-action.component';
import { AdminComponent } from './features/admin.component';
import { SettingsComponent } from './features/settings.component';
import { FinOpsComponent } from './features/finops.component';
import { LoginComponent } from './features/login/login.component';
import { authGuard } from './core/auth.guard';

export const routes: Routes = [
  { path: 'login', component: LoginComponent, title: 'Entrar' },
  {
    path: '',
    component: IntegrationsComponent,
    title: 'Integrações',
    canActivate: [authGuard],
  },
  {
    path: 'run/:contractId/:opId',
    component: RunActionComponent,
    title: 'Executar ação',
    canActivate: [authGuard],
  },
  {
    path: 'admin',
    component: AdminComponent,
    title: 'Admin',
    canActivate: [authGuard],
  },
  {
    path: 'settings',
    component: SettingsComponent,
    title: 'Settings',
    canActivate: [authGuard],
  },
  {
    path: 'finops',
    component: FinOpsComponent,
    title: 'FinOps',
    canActivate: [authGuard],
  },
  { path: '**', redirectTo: '' },
];
