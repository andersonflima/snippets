import { Routes } from '@angular/router';
import { IntegrationsComponent } from './features/integrations.component';
import { RunActionComponent } from './features/run-action.component';
import { AdminComponent } from './features/admin.component';
import { SettingsComponent } from './features/settings.component';

export const routes: Routes = [
  { path: '', component: IntegrationsComponent, title: 'Integrações' },
  {
    path: 'run/:contractId/:opId',
    component: RunActionComponent,
    title: 'Executar ação',
  },
  { path: 'admin', component: AdminComponent, title: 'Admin' },
  { path: 'settings', component: SettingsComponent, title: 'Settings' },
  { path: '**', redirectTo: '' },
];
