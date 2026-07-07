import { ChangeDetectionStrategy, Component, inject } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import { ThemeService } from './core/theme.service';

/** Root: hosts the router outlet. Layout (sidebar/header) lives in ShellComponent. */
@Component({
  selector: 'app-root',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  imports: [RouterOutlet],
  template: `<router-outlet />`,
})
export class AppComponent {
  constructor() {
    // Instantiating the service applies the persisted theme to <html> at startup.
    inject(ThemeService);
  }
}
