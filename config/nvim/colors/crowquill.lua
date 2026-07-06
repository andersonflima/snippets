-- =============================================================================
--  Crowquill Ink  —  Neovim colorscheme (dark)
-- -----------------------------------------------------------------------------
--  A strict MONOCHROME (black & white / grayscale) ink theme tuned for the
--  Crowquill Mono font. Signature: language keywords / storage / control-flow
--  are the single BRIGHTEST element — pure white, BOLD and UNDERLINED. Brackets,
--  delimiters and operators are also lit to pure white (fg only, NO bold /
--  underline) so structure "pops"; everything else is a dimmer shade of gray.
--  No chromatic colors anywhere.
--
--  Palette is kept identical to the VS Code "Crowquill Ink Dark" theme.
--
--    bg        #0A0A0A   base editor background (near-black)
--    bg_alt    #0F0F0F   sidebars / floats / panels
--    bg_dark   #000000   deepest surfaces / statusline / ansi black
--    bg_hi     #141414   line highlight
--    sel       #2A2A2A   selection / visual
--    border    #1E1E1E   splits / borders
--    fg        #A8A8A8   default foreground (variables, identifiers)
--    fg_dim    #9A9A9A   secondary foreground
--    gutter    #4A4A4A   line numbers / muted UI
--    comment   #565656   comments (italic)
--    white     #FFFFFF   SIGNATURE keyword (bold + underline) — brightest
--    string    #8C8C8C   strings
--    number    #D2D2D2   numbers / constants / booleans
--    func      #EAEAEA   functions / methods
--    type      #C6C6C6   types / classes / interfaces / properties
--    operator  #7C7C7C   operators (base; treesitter/legacy lit to white)
--    punct     #6E6E6E   punctuation (tag delimiters; brackets lit to white)
--    bracket   #FFFFFF   brackets / delimiters / operators — lit (fg only)
--    param     #A0A0A0   parameters (italic)
--
--  Usage:  vim.o.background = "dark"  →  vim.cmd.colorscheme("crowquill")
-- =============================================================================

local M = {}

-- Reset any previously loaded highlights.
vim.cmd("highlight clear")
if vim.fn.exists("syntax_on") == 1 then
  vim.cmd("syntax reset")
end

vim.o.background = "dark"
vim.g.colors_name = "crowquill"

-- ---------------------------------------------------------------------------
-- Palette (strict grayscale — every value has R == G == B)
-- ---------------------------------------------------------------------------
local c = {
  bg       = "#0A0A0A",
  bg_alt   = "#0F0F0F",
  bg_dark  = "#000000",
  bg_hi    = "#141414",
  sel      = "#2A2A2A",
  border   = "#1E1E1E",
  fg       = "#A8A8A8",
  fg_dim   = "#9A9A9A",
  gutter   = "#4A4A4A",
  comment  = "#565656",
  doc      = "#606060",
  white    = "#FFFFFF",
  string   = "#8C8C8C",
  number   = "#D2D2D2",
  func     = "#EAEAEA",
  type     = "#C6C6C6",
  operator = "#7C7C7C",
  punct    = "#6E6E6E",
  bracket  = "#FFFFFF",
  param    = "#A0A0A0",
  -- diagnostics: grayscale only, differentiated by lightness + undercurl
  err      = "#C6C6C6",
  warn     = "#9A9A9A",
  info     = "#7C7C7C",
  hint     = "#6E6E6E",
  none     = "NONE",
}

-- ---------------------------------------------------------------------------
-- Helper
-- ---------------------------------------------------------------------------
local set = vim.api.nvim_set_hl
local function hl(group, opts)
  set(0, group, opts)
end

-- Signature keyword style: brightest + bold + underline.
local KW = { fg = c.white, bold = true }

-- Bracket / delimiter / operator style: lit to white, fg only (no bold/underline).
local BR = { fg = c.bracket }

-- ---------------------------------------------------------------------------
-- Editor UI
-- ---------------------------------------------------------------------------
hl("Normal",         { fg = c.fg, bg = c.bg })
hl("NormalNC",       { fg = c.fg, bg = c.bg })
hl("NormalFloat",    { fg = c.fg, bg = c.bg_alt })
hl("FloatBorder",    { fg = c.border, bg = c.bg_alt })
hl("FloatTitle",     { fg = c.white, bg = c.bg_alt, bold = true })
hl("ColorColumn",    { bg = c.bg_hi })
hl("Cursor",         { fg = c.bg, bg = c.white })
hl("CursorLine",     { bg = c.bg_hi })
hl("CursorColumn",   { bg = c.bg_hi })
hl("CursorLineNr",   { fg = c.white, bold = true })
hl("LineNr",         { fg = c.gutter })
hl("SignColumn",     { fg = c.gutter, bg = c.bg })
hl("FoldColumn",     { fg = c.gutter, bg = c.bg })
hl("Folded",         { fg = c.comment, bg = c.bg_alt })
hl("VertSplit",      { fg = c.border })
hl("WinSeparator",   { fg = c.border })
hl("Visual",         { bg = c.sel })
hl("VisualNOS",      { bg = c.sel })
hl("Search",         { fg = c.bg_dark, bg = c.number })
hl("IncSearch",      { fg = c.bg_dark, bg = c.white })
hl("CurSearch",      { fg = c.bg_dark, bg = c.white })
hl("MatchParen",     { fg = c.white, bold = true })
hl("NonText",        { fg = "#282828" })
hl("Whitespace",     { fg = "#282828" })
hl("SpecialKey",     { fg = "#282828" })
hl("EndOfBuffer",    { fg = c.bg })
hl("Conceal",        { fg = c.comment })
hl("Directory",      { fg = c.func })
hl("Title",          { fg = c.white, bold = true })
hl("ErrorMsg",       { fg = c.err })
hl("WarningMsg",     { fg = c.warn })
hl("ModeMsg",        { fg = c.fg_dim, bold = true })
hl("MoreMsg",        { fg = c.fg_dim })
hl("Question",       { fg = c.fg_dim })
hl("WildMenu",       { fg = c.bg_dark, bg = c.white })

-- Statusline / tabline
hl("StatusLine",     { fg = c.fg_dim, bg = c.bg_dark })
hl("StatusLineNC",   { fg = c.comment, bg = c.bg_dark })
hl("TabLine",        { fg = c.comment, bg = c.bg_dark })
hl("TabLineFill",    { fg = c.comment, bg = c.bg_dark })
hl("TabLineSel",     { fg = c.white, bg = c.bg, bold = true })
hl("WinBar",         { fg = c.fg_dim, bg = c.none })
hl("WinBarNC",       { fg = c.comment, bg = c.none })

-- Popup menu
hl("Pmenu",          { fg = c.fg, bg = c.bg_alt })
hl("PmenuSel",       { fg = c.white, bg = c.sel, bold = true })
hl("PmenuSbar",      { bg = c.bg_alt })
hl("PmenuThumb",     { bg = c.gutter })
hl("PmenuKind",      { fg = c.type, bg = c.bg_alt })
hl("PmenuExtra",     { fg = c.comment, bg = c.bg_alt })

-- ---------------------------------------------------------------------------
-- Syntax groups (legacy / vim regex)
-- ---------------------------------------------------------------------------
hl("Comment",        { fg = c.comment, italic = true })

hl("Constant",       { fg = c.number })
hl("String",         { fg = c.string })
hl("Character",      { fg = c.string })
hl("Number",         { fg = c.number })
hl("Float",          { fg = c.number })
hl("Boolean",        { fg = c.number, bold = true })

hl("Identifier",     { fg = c.fg })
hl("Function",       { fg = c.func })

-- SIGNATURE: keywords / storage / control-flow are white, bold, underline.
hl("Statement",      KW)
hl("Conditional",    KW)
hl("Repeat",         KW)
hl("Label",          KW)
hl("Operator",       BR)
hl("Keyword",        KW)
hl("Exception",      KW)

hl("PreProc",        { fg = c.type })
hl("Include",        KW)
hl("Define",         { fg = c.type })
hl("Macro",          { fg = c.type })
hl("PreCondit",      { fg = c.type })

hl("Type",           { fg = c.type })
hl("StorageClass",   KW)
hl("Structure",      { fg = c.type })
hl("Typedef",        { fg = c.type })

hl("Special",        { fg = c.number })
hl("SpecialChar",    { fg = c.number })
hl("Tag",            KW)
hl("Delimiter",      BR)
hl("SpecialComment", { fg = c.doc, italic = true })
hl("Debug",          { fg = c.fg_dim })

hl("Underlined",     { fg = c.func, underline = true })
hl("Ignore",         { fg = c.gutter })
hl("Error",          { fg = c.err, bold = true })
hl("Todo",           { fg = c.bg_dark, bg = c.number, bold = true })

-- ---------------------------------------------------------------------------
-- Diagnostics (LSP) — grayscale only; severity conveyed via undercurl.
-- ---------------------------------------------------------------------------
hl("DiagnosticError",          { fg = c.err })
hl("DiagnosticWarn",           { fg = c.warn })
hl("DiagnosticInfo",           { fg = c.info })
hl("DiagnosticHint",           { fg = c.hint })
hl("DiagnosticOk",             { fg = c.fg_dim })
hl("DiagnosticUnderlineError", { undercurl = true, sp = c.err })
hl("DiagnosticUnderlineWarn",  { undercurl = true, sp = c.warn })
hl("DiagnosticUnderlineInfo",  { underline = true, sp = c.info })
hl("DiagnosticUnderlineHint",  { underline = true, sp = c.hint })
hl("DiagnosticVirtualTextError", { fg = c.err, bg = c.bg_hi })
hl("DiagnosticVirtualTextWarn",  { fg = c.warn, bg = c.bg_hi })
hl("DiagnosticVirtualTextInfo",  { fg = c.info, bg = c.bg_hi })
hl("DiagnosticVirtualTextHint",  { fg = c.hint, bg = c.bg_hi })

-- LSP references / semantic base
hl("LspReferenceText",  { bg = c.sel })
hl("LspReferenceRead",  { bg = c.sel })
hl("LspReferenceWrite", { bg = c.sel })
hl("LspInlayHint",      { fg = c.comment, bg = c.bg_hi })
hl("LspCodeLens",       { fg = c.comment, italic = true })
hl("LspSignatureActiveParameter", { fg = c.white, bold = true })

-- ---------------------------------------------------------------------------
-- Diff / Git — grayscale only.
-- ---------------------------------------------------------------------------
hl("DiffAdd",        { bg = "#161616" })
hl("DiffChange",     { bg = "#121212" })
hl("DiffDelete",     { fg = c.comment, bg = "#0D0D0D" })
hl("DiffText",       { bg = "#242424" })
hl("diffAdded",      { fg = c.fg })
hl("diffRemoved",    { fg = c.comment })
hl("diffChanged",    { fg = c.string })
hl("diffFile",       { fg = c.type })
hl("diffLine",       { fg = c.comment })

hl("GitSignsAdd",    { fg = c.fg_dim })
hl("GitSignsChange", { fg = c.string })
hl("GitSignsDelete", { fg = c.comment })

-- ---------------------------------------------------------------------------
-- Spelling — grayscale undercurl.
-- ---------------------------------------------------------------------------
hl("SpellBad",   { undercurl = true, sp = c.err })
hl("SpellCap",   { undercurl = true, sp = c.warn })
hl("SpellRare",  { undercurl = true, sp = c.info })
hl("SpellLocal", { undercurl = true, sp = c.hint })

-- ---------------------------------------------------------------------------
-- Treesitter (@captures)
-- ---------------------------------------------------------------------------
-- Comments
hl("@comment",               { fg = c.comment, italic = true })
hl("@comment.documentation", { fg = c.doc, italic = true })
hl("@comment.error",         { fg = c.bg_dark, bg = c.err, bold = true })
hl("@comment.warning",       { fg = c.bg_dark, bg = c.warn, bold = true })
hl("@comment.todo",          { fg = c.bg_dark, bg = c.number, bold = true })
hl("@comment.note",          { fg = c.bg_dark, bg = c.fg_dim, bold = true })

-- Strings
hl("@string",                { fg = c.string })
hl("@string.documentation",  { fg = c.doc, italic = true })
hl("@string.regexp",         { fg = c.param })
hl("@string.escape",         { fg = c.number })
hl("@string.special",        { fg = c.number })
hl("@string.special.symbol", { fg = c.number })
hl("@string.special.url",    { fg = c.func, underline = true })
hl("@character",             { fg = c.string })
hl("@character.special",     { fg = c.number })

-- Numbers / booleans / constants
hl("@number",                { fg = c.number })
hl("@number.float",          { fg = c.number })
hl("@boolean",               { fg = c.number, bold = true })
hl("@constant",              { fg = c.number })
hl("@constant.builtin",      { fg = c.number, bold = true })
hl("@constant.macro",        { fg = c.type })

-- SIGNATURE: keywords / control-flow / storage → white, bold, underline
hl("@keyword",                     KW)
hl("@keyword.function",            KW)
hl("@keyword.operator",            KW)
hl("@keyword.return",              KW)
hl("@keyword.import",              KW)
hl("@keyword.export",              KW)
hl("@keyword.conditional",         KW)
hl("@keyword.conditional.ternary", { fg = c.operator })
hl("@keyword.repeat",              KW)
hl("@keyword.exception",           KW)
hl("@keyword.coroutine",           KW)
hl("@keyword.debug",               KW)
hl("@keyword.directive",           { fg = c.type })
hl("@keyword.directive.define",    { fg = c.type })
hl("@keyword.storage",             KW)
hl("@keyword.modifier",            KW)
hl("@conditional",                 KW) -- legacy capture name
hl("@repeat",                      KW) -- legacy capture name
hl("@exception",                   KW) -- legacy capture name
hl("@include",                     KW) -- legacy capture name

-- Operators / punctuation / brackets — lit to white (fg only, no bold/underline)
hl("@operator",              BR)
hl("@punctuation.delimiter", BR)
hl("@punctuation.bracket",   BR)
hl("@punctuation.special",   BR)

-- Functions / methods
hl("@function",              { fg = c.func })
hl("@function.builtin",      { fg = c.func, italic = true })
hl("@function.call",         { fg = c.func })
hl("@function.macro",        { fg = c.type })
hl("@function.method",       { fg = c.func })
hl("@function.method.call",  { fg = c.func })
hl("@constructor",           BR) -- table/object braces lit like brackets

-- Variables / parameters / fields
hl("@variable",                   { fg = c.fg })
hl("@variable.builtin",           { fg = c.number, italic = true, bold = true })
hl("@variable.parameter",         { fg = c.param, italic = true })
hl("@variable.parameter.builtin", { fg = c.param, italic = true, bold = true })
hl("@variable.member",            { fg = c.type })
hl("@property",                   { fg = c.type })
hl("@field",                      { fg = c.type })

-- Types / classes / namespaces
hl("@type",             { fg = c.type })
hl("@type.builtin",     { fg = c.type, italic = true })
hl("@type.definition",  { fg = c.type, bold = true })
hl("@type.qualifier",   KW)
hl("@structure",        { fg = c.type })
hl("@storageclass",     KW)
hl("@namespace",        { fg = c.type })
hl("@module",           { fg = c.type })
hl("@module.builtin",   { fg = c.type, italic = true })

-- Labels / attributes / decorators
hl("@label",             { fg = c.string })
hl("@attribute",         { fg = c.type, italic = true })
hl("@attribute.builtin", { fg = c.type, italic = true })
hl("@decorator",         { fg = c.type, italic = true })

-- Markup (markdown, etc.)
hl("@markup.heading",        { fg = c.white, bold = true })
hl("@markup.heading.1",      { fg = c.white, bold = true })
hl("@markup.heading.2",      { fg = c.func, bold = true })
hl("@markup.heading.3",      { fg = c.type, bold = true })
hl("@markup.strong",         { fg = c.func, bold = true })
hl("@markup.italic",         { fg = c.type, italic = true })
hl("@markup.strikethrough",  { fg = c.comment, strikethrough = true })
hl("@markup.underline",      { underline = true })
hl("@markup.link",           { fg = c.func })
hl("@markup.link.label",     { fg = c.string })
hl("@markup.link.url",       { fg = c.func, underline = true })
hl("@markup.raw",            { fg = c.string })
hl("@markup.raw.block",      { fg = c.string })
hl("@markup.quote",          { fg = c.comment, italic = true })
hl("@markup.list",           { fg = c.fg_dim })
hl("@markup.list.checked",   { fg = c.fg })
hl("@markup.list.unchecked", { fg = c.comment })

-- Tags (HTML / JSX / XML)
hl("@tag",           KW)
hl("@tag.builtin",   KW)
hl("@tag.attribute", { fg = c.type, italic = true })
hl("@tag.delimiter", { fg = c.punct })

-- Diff captures
hl("@diff.plus",  { fg = c.fg })
hl("@diff.minus", { fg = c.comment })
hl("@diff.delta", { fg = c.string })

-- ---------------------------------------------------------------------------
-- LSP semantic tokens (@lsp.type.* / @lsp.mod.*)
-- ---------------------------------------------------------------------------
hl("@lsp.type.keyword",       KW)
hl("@lsp.type.modifier",      KW)
hl("@lsp.type.namespace",     { fg = c.type })
hl("@lsp.type.class",         { fg = c.type })
hl("@lsp.type.enum",          { fg = c.type })
hl("@lsp.type.interface",     { fg = c.type })
hl("@lsp.type.struct",        { fg = c.type })
hl("@lsp.type.type",          { fg = c.type })
hl("@lsp.type.typeParameter", { fg = c.type, italic = true })
hl("@lsp.type.function",      { fg = c.func })
hl("@lsp.type.method",        { fg = c.func })
hl("@lsp.type.macro",         { fg = c.type })
hl("@lsp.type.property",      { fg = c.type })
hl("@lsp.type.variable",      { fg = c.fg })
hl("@lsp.type.parameter",     { fg = c.param, italic = true })
hl("@lsp.type.enumMember",    { fg = c.number })
hl("@lsp.type.decorator",     { fg = c.type, italic = true })
hl("@lsp.mod.readonly",       { fg = c.number })
hl("@lsp.mod.deprecated",     { strikethrough = true })
hl("@lsp.typemod.keyword.controlFlow", KW)
hl("@lsp.typemod.function.declaration", { fg = c.func, bold = true })
hl("@lsp.typemod.variable.readonly",    { fg = c.number })

-- ---------------------------------------------------------------------------
-- Common plugin groups (Telescope / NvimTree / Neo-tree / WhichKey / Notify)
-- ---------------------------------------------------------------------------
hl("TelescopeNormal",       { fg = c.fg, bg = c.bg_alt })
hl("TelescopeBorder",       { fg = c.border, bg = c.bg_alt })
hl("TelescopePromptNormal", { fg = c.fg, bg = c.bg_hi })
hl("TelescopePromptBorder", { fg = c.bg_hi, bg = c.bg_hi })
hl("TelescopePromptTitle",  { fg = c.bg_dark, bg = c.white, bold = true })
hl("TelescopeResultsTitle", { fg = c.bg_alt, bg = c.bg_alt })
hl("TelescopePreviewTitle", { fg = c.bg_dark, bg = c.fg_dim, bold = true })
hl("TelescopeSelection",    { fg = c.white, bg = c.sel, bold = true })
hl("TelescopeMatching",     { fg = c.white, bold = true })

hl("NvimTreeNormal",           { fg = c.fg_dim, bg = c.bg_alt })
hl("NvimTreeFolderName",       { fg = c.type })
hl("NvimTreeFolderIcon",       { fg = c.type })
hl("NvimTreeOpenedFolderName", { fg = c.type, bold = true })
hl("NvimTreeRootFolder",       { fg = c.white, bold = true })
hl("NvimTreeGitDirty",         { fg = c.string })
hl("NvimTreeSpecialFile",      { fg = c.func, underline = true })
hl("NvimTreeIndentMarker",     { fg = c.border })

hl("NeoTreeNormal",        { fg = c.fg_dim, bg = c.bg_alt })
hl("NeoTreeNormalNC",      { fg = c.fg_dim, bg = c.bg_alt })
hl("NeoTreeDirectoryName", { fg = c.type })
hl("NeoTreeRootName",      { fg = c.white, bold = true })
hl("NeoTreeGitModified",   { fg = c.string })
hl("NeoTreeGitUntracked",  { fg = c.fg_dim })

hl("WhichKey",          { fg = c.white, bold = true })
hl("WhichKeyGroup",     { fg = c.type })
hl("WhichKeyDesc",      { fg = c.fg })
hl("WhichKeySeparator", { fg = c.comment })
hl("WhichKeyFloat",     { bg = c.bg_alt })

hl("NotifyERRORBorder", { fg = c.err })
hl("NotifyWARNBorder",  { fg = c.warn })
hl("NotifyINFOBorder",  { fg = c.info })
hl("NotifyERRORTitle",  { fg = c.err, bold = true })
hl("NotifyWARNTitle",   { fg = c.warn, bold = true })
hl("NotifyINFOTitle",   { fg = c.info, bold = true })

hl("IndentBlanklineChar", { fg = c.border })
hl("IblIndent",           { fg = c.border })
hl("IblScope",            { fg = c.gutter })

-- ---------------------------------------------------------------------------
-- Terminal ANSI colors — grayscale ramp (black → white, no hues).
-- ---------------------------------------------------------------------------
vim.g.terminal_color_0  = "#000000"
vim.g.terminal_color_1  = "#5A5A5A"
vim.g.terminal_color_2  = "#767676"
vim.g.terminal_color_3  = "#929292"
vim.g.terminal_color_4  = "#A8A8A8"
vim.g.terminal_color_5  = "#BEBEBE"
vim.g.terminal_color_6  = "#D4D4D4"
vim.g.terminal_color_7  = "#E4E4E4"
vim.g.terminal_color_8  = "#6E6E6E"
vim.g.terminal_color_9  = "#7C7C7C"
vim.g.terminal_color_10 = "#8C8C8C"
vim.g.terminal_color_11 = "#A0A0A0"
vim.g.terminal_color_12 = "#B4B4B4"
vim.g.terminal_color_13 = "#C6C6C6"
vim.g.terminal_color_14 = "#E0E0E0"
vim.g.terminal_color_15 = "#FFFFFF"

return M
