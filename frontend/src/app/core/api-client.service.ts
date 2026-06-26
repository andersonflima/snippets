import { Injectable, inject } from '@angular/core';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { firstValueFrom } from 'rxjs';
import { JsonValue } from './json-schema';
import { SettingsService } from './settings.service';

/** Normalized outcome of an action invocation, ready to render. */
export interface ActionResult {
  ok: boolean;
  status: number;
  statusText: string;
  body: unknown;
}

/** Sends action requests to the configured API Gateway base URL. */
@Injectable({ providedIn: 'root' })
export class ApiClientService {
  private readonly http = inject(HttpClient);
  private readonly settings = inject(SettingsService);

  /** POST the payload to `{baseUrl}{path}` and normalize success/error. */
  async runAction(path: string, payload: JsonValue): Promise<ActionResult> {
    const url = `${this.settings.baseUrl()}${path}`;
    try {
      const response = await firstValueFrom(
        this.http.post(url, payload, { observe: 'response' }),
      );
      return {
        ok: true,
        status: response.status,
        statusText: response.statusText,
        body: response.body,
      };
    } catch (err) {
      return this.toErrorResult(err);
    }
  }

  private toErrorResult(err: unknown): ActionResult {
    if (err instanceof HttpErrorResponse) {
      return {
        ok: false,
        status: err.status,
        statusText: err.statusText,
        body: err.error ?? err.message,
      };
    }
    return {
      ok: false,
      status: 0,
      statusText: 'Network/Client error',
      body: err instanceof Error ? err.message : String(err),
    };
  }
}
