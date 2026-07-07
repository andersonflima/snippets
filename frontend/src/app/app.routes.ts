import { Routes } from '@angular/router';
import { ShellComponent } from './shell/shell.component';
import { DashboardComponent } from './dashboard/dashboard.component';
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
    component: ShellComponent,
    canActivate: [authGuard],
    children: [
      { path: '', component: DashboardComponent, title: 'Dashboard' },
      { path: 'integrations', component: IntegrationsComponent, title: 'Integrações' },
      { path: 'run/:contractId/:opId', component: RunActionComponent, title: 'Executar ação' },
      { path: 'admin', component: AdminComponent, title: 'Admin' },
      { path: 'settings', component: SettingsComponent, title: 'Configurações' },
      { path: 'finops', component: FinOpsComponent, title: 'FinOps' },
    ],
  },
  { path: '**', redirectTo: '' },
];
