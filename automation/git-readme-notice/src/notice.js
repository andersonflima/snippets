// Pure notice logic: rendering the banner block, detecting an existing notice
// and prepending it to a file's content. No IO here.

// renderNoticeBlock formats the message as a loud Markdown banner for the top
// of the file, followed by a blank line separating it from the original body.
export function renderNoticeBlock(message) {
  return `> **⚠️ ${message}**\n\n`;
}

// hasNotice reports whether content already carries the notice. Matching on
// the raw message (not the rendered block) keeps reruns idempotent even if the
// banner formatting evolves between versions.
export function hasNotice(content, message) {
  return content.includes(message);
}

// prependNotice returns content with the banner block added at the very top.
export function prependNotice(content, message) {
  return renderNoticeBlock(message) + content;
}
