-- =============================================================================
--  Crowquill Ink  —  Neovim colorscheme (light)
-- -----------------------------------------------------------------------------
--  A strict MONOCHROME (black & white / grayscale) ink-on-paper theme tuned
--  for the Crowquill Mono font. Signature: language keywords / storage /
--  control-flow are the single DARKEST element — pure black, BOLD and
--  UNDERLINED. Brackets, delimiters and operators are also inked to pure black
--  (fg only, NO bold / underline) so structure "pops"; everything else is a
--  dimmer shade of gray. No chromatic colors anywhere.
--
--  Palette is kept identical to the VS Code "Crowquill Ink Light" theme.
--
--    bg        #FFFFFF   base editor background (paper)
--    bg_alt    #F7F7F7   sidebars / floats / panels
--    bg_light  #F0F0F0   statusline / tabline
--    bg_hi     #F2F2F2   line highlight
--    sel       #DADADA   selection / visual
--    border    #E4E4E4   splits / borders
--    fg        #444444   default foreground (variables, identifiers)
--    fg_dim    #5C5C5C   secondary foreground
--    gutter    #B0B0B0   line numbers / muted UI
--    comment   #9E9E9E   comments (italic)
--    black     #000000   SIGNATURE keyword (bold + underline) — darkest
--    string    #6C6C6C   strings
--    number    #2A2A2A   numbers / constants / booleans
--    func      #141414   functions / methods
--    type      #3A3A3A   types / classes / interfaces / properties
--    operator  #7C7C7C   operators (base; treesitter/legacy inked to black)
--    punct     #8A8A8A   punctuation (tag delimiters; brackets inked to black)
--    bracket   #000000   brackets / delimiters / operators — inked (fg only)
--    param     #5C5C5C   parameters (italic)
--
--  Usage:  vim.o.background = "light"  →  vim.cmd.colorscheme("crowquill-light")
-- =============================================================================

local M = {}

-- Reset any previously loaded highlights.
vim.cmd("highlight clear")
if vim.fn.exists("syntax_on") == 1 then
  vim.cmd("syntax reset")
end

vim.o.background = "light"
vim.g.colors_name = "crowquill-light"

-- ---------------------------------------------------------------------------
-- Palette (strict grayscale — every value has R == G == B)
-- ---------------------------------------------------------------------------
local c = {
  bg       = "#FFFFFF",
  bg_alt   = "#F7F7F7",
  bg_light = "#F0F0F0",
  bg_hi    = "#F2F2F2",
  sel      = "#DADADA",
  border   = "#E4E4E4",
  fg       = "#444444",
  fg_dim   = "#5C5C5C",
  gutter   = "#B0B0B0",
  comment  = "#9E9E9E",
  doc      = "#909090",
  black    = "#000000",
  string   = "#6C6C6C",
  number   = "#2A2A2A",
  func     = "#141414",
  type     = "#3A3A3A",
  operator = "#7C7C7C",
  punct    = "#8A8A8A",
  bracket  = "#000000",
  param    = "#5C5C5C",
  -- diagnostics: grayscale only, differentiated by lightness + undercurl
  err      = "#3A3A3A",
  warn     = "#6C6C6C",
  info     = "#8A8A8A",
  hint     = "#9E9E9E",
  none     = "NONE",
}

-- ---------------------------------------------------------------------------
-- Helper
-- ---------------------------------------------------------------------------
local set = vim.api.nvim_set_hl
local function hl(group, opts)
  set(0, group, opts)
end

-- Signature keyword style: darkest + bold + underline.
local KW = { fg = c.black, bold = true }

-- Bracket / delimiter / operator style: inked to black, fg only (no bold/underline).
local BR = { fg = c.bracket }

-- ---------------------------------------------------------------------------
-- Editor UI
-- ---------------------------------------------------------------------------
hl("Normal",         { fg = c.fg, bg = c.bg })
hl("NormalNC",       { fg = c.fg, bg = c.bg })
hl("NormalFloat",    { fg = c.fg, bg = c.bg_alt })
hl("FloatBorder",    { fg = c.border, bg = c.bg_alt })
hl("FloatTitle",     { fg = c.black, bg = c.bg_alt, bold = true })
hl("ColorColumn",    { bg = c.bg_hi })
hl("Cursor",         { fg = c.bg, bg = c.black })
hl("CursorLine",     { bg = c.bg_hi })
hl("CursorColumn",   { bg = c.bg_hi })
hl("CursorLineNr",   { fg = c.black, bold = true })
hl("LineNr",         { fg = c.gutter })
hl("SignColumn",     { fg = c.gutter, bg = c.bg })
hl("FoldColumn",     { fg = c.gutter, bg = c.bg })
hl("Folded",         { fg = c.comment, bg = c.bg_alt })
hl("VertSplit",      { fg = c.border })
hl("WinSeparator",   { fg = c.border })
hl("Visual",         { bg = c.sel })
hl("VisualNOS",      { bg = c.sel })
hl("Search",         { fg = c.bg, bg = c.number })
hl("IncSearch",      { fg = c.bg, bg = c.black })
hl("CurSearch",      { fg = c.bg, bg = c.black })
hl("MatchParen",     { fg = c.black, bold = true })
hl("NonText",        { fg = "#D8D8D8" })
hl("Whitespace",     { fg = "#D8D8D8" })
hl("SpecialKey",     { fg = "#D8D8D8" })
hl("EndOfBuffer",    { fg = c.bg })
hl("Conceal",        { fg = c.comment })
hl("Directory",      { fg = c.func })
hl("Title",          { fg = c.black, bold = true })
hl("ErrorMsg",       { fg = c.err })
hl("WarningMsg",     { fg = c.warn })
hl("ModeMsg",        { fg = c.fg_dim, bold = true })
hl("MoreMsg",        { fg = c.fg_dim })
hl("Question",       { fg = c.fg_dim })
hl("WildMenu",       { fg = c.bg, bg = c.black })

-- Statusline / tabline
hl("StatusLine",     { fg = c.fg_dim, bg = c.bg_light })
hl("StatusLineNC",   { fg = c.comment, bg = c.bg_light })
hl("TabLine",        { fg = c.comment, bg = c.bg_light })
hl("TabLineFill",    { fg = c.comment, bg = c.bg_light })
hl("TabLineSel",     { fg = c.black, bg = c.bg, bold = true })
hl("WinBar",         { fg = c.fg_dim, bg = c.none })
hl("WinBarNC",       { fg = c.comment, bg = c.none })

-- Popup menu
hl("Pmenu",          { fg = c.fg, bg = c.bg_alt })
hl("PmenuSel",       { fg = c.black, bg = c.sel, bold = true })
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

-- SIGNATURE: keywords / storage / control-flow are black, bold, underline.
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
hl("Todo",           { fg = c.bg, bg = c.number, bold = true })

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
hl("LspSignatureActiveParameter", { fg = c.black, bold = true })

-- ---------------------------------------------------------------------------
-- Diff / Git — grayscale only.
-- ---------------------------------------------------------------------------
hl("DiffAdd",        { bg = "#EEEEEE" })
hl("DiffChange",     { bg = "#F2F2F2" })
hl("DiffDelete",     { fg = c.comment, bg = "#F4F4F4" })
hl("DiffText",       { bg = "#DADADA" })
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
hl("@comment.error",         { fg = c.bg, bg = c.err, bold = true })
hl("@comment.warning",       { fg = c.bg, bg = c.warn, bold = true })
hl("@comment.todo",          { fg = c.bg, bg = c.number, bold = true })
hl("@comment.note",          { fg = c.bg, bg = c.fg_dim, bold = true })

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

-- SIGNATURE: keywords / control-flow / storage → black, bold, underline
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

-- Operators / punctuation / brackets — inked to black (fg only, no bold/underline)
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
hl("@constructor",           BR) -- table/object braces inked like brackets

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
hl("@markup.heading",        { fg = c.black, bold = true })
hl("@markup.heading.1",      { fg = c.black, bold = true })
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
hl("TelescopePromptTitle",  { fg = c.bg, bg = c.black, bold = true })
hl("TelescopeResultsTitle", { fg = c.bg_alt, bg = c.bg_alt })
hl("TelescopePreviewTitle", { fg = c.bg, bg = c.fg_dim, bold = true })
hl("TelescopeSelection",    { fg = c.black, bg = c.sel, bold = true })
hl("TelescopeMatching",     { fg = c.black, bold = true })

hl("NvimTreeNormal",           { fg = c.fg_dim, bg = c.bg_alt })
hl("NvimTreeFolderName",       { fg = c.type })
hl("NvimTreeFolderIcon",       { fg = c.type })
hl("NvimTreeOpenedFolderName", { fg = c.type, bold = true })
hl("NvimTreeRootFolder",       { fg = c.black, bold = true })
hl("NvimTreeGitDirty",         { fg = c.string })
hl("NvimTreeSpecialFile",      { fg = c.func, underline = true })
hl("NvimTreeIndentMarker",     { fg = c.border })

hl("NeoTreeNormal",        { fg = c.fg_dim, bg = c.bg_alt })
hl("NeoTreeNormalNC",      { fg = c.fg_dim, bg = c.bg_alt })
hl("NeoTreeDirectoryName", { fg = c.type })
hl("NeoTreeRootName",      { fg = c.black, bold = true })
hl("NeoTreeGitModified",   { fg = c.string })
hl("NeoTreeGitUntracked",  { fg = c.fg_dim })

hl("WhichKey",          { fg = c.black, bold = true })
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
-- Terminal ANSI colors — grayscale ramp (readable inks on paper, no hues).
-- ---------------------------------------------------------------------------
vim.g.terminal_color_0  = "#000000"
vim.g.terminal_color_1  = "#3C3C3C"
vim.g.terminal_color_2  = "#4E4E4E"
vim.g.terminal_color_3  = "#606060"
vim.g.terminal_color_4  = "#525252"
vim.g.terminal_color_5  = "#444444"
vim.g.terminal_color_6  = "#363636"
vim.g.terminal_color_7  = "#8A8A8A"
vim.g.terminal_color_8  = "#6C6C6C"
vim.g.terminal_color_9  = "#2A2A2A"
vim.g.terminal_color_10 = "#383838"
vim.g.terminal_color_11 = "#4A4A4A"
vim.g.terminal_color_12 = "#3E3E3E"
vim.g.terminal_color_13 = "#303030"
vim.g.terminal_color_14 = "#202020"
vim.g.terminal_color_15 = "#000000"

return M
