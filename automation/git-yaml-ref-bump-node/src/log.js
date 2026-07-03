// Progress logging to stderr, so the Markdown report on stdout stays clean and
// pipeable. Silenced by --quiet.

// progress writes a step line to stderr unless the run is quiet.
export function progress(cfg, message) {
  if (cfg.quiet) {
    return;
  }
  process.stderr.write(`» ${message}\n`);
}
