import { ChangeDetectionStrategy, Component, inject } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { ThemeService } from './core/theme.service';
import { ToastHostComponent } from './shared/toast.component';

/** Root: hosts the router outlet + global toast stack. Layout lives in ShellComponent. */
@Component({
  selector: 'app-root',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterOutlet, ToastHostComponent],
  template: `
    <router-outlet />
    <app-toast-host />
  `,
})
export class AppComponent {
  constructor() {
    // Instantiating the service applies the persisted theme to <html> at startup.
    inject(ThemeService);
  }
}
