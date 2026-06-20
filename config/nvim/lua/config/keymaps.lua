local keymap = vim.keymap
local opts = { noremap = true, silent = true }

local function is_substitute_command(cmdline)
	return cmdline:match("^%%?s$") ~= nil or cmdline:match("^['<>,%d%.$%+%-]*s$") ~= nil
end

keymap.set("n", "x", '"_x')

-- Increment/decrement
keymap.set("n", "+", "<C-a>")
keymap.set("n", "-", "<C-x>")

-- Delete a word backwards
keymap.set("n", "dw", 'vb"_d')

-- Select all
keymap.set("n", "<C-a>", "gg<S-v>G")

-- Save with root permission (not working for now)
--vim.api.nvim_create_user_command('W', 'w !sudo tee > /dev/null %', {})

-- Disable continuations
keymap.set("n", "<Leader>o", "o<Esc>^Da", opts)
keymap.set("n", "<Leader>O", "O<Esc>^Da", opts)

-- Jumplist
keymap.set("n", "<C-m>", "<C-i>", opts)

-- New tab
keymap.set("n", "te", ":tabedit")
keymap.set("n", "<tab>", ":tabnext<Return>", opts)
keymap.set("n", "<s-tab>", ":tabprev<Return>", opts)
-- Split window
keymap.set("n", "ss", ":split<Return>", opts)
keymap.set("n", "sv", ":vsplit<Return>", opts)
-- Move window
keymap.set("n", "sh", "<C-w>h")
keymap.set("n", "sk", "<C-w>k")
keymap.set("n", "sj", "<C-w>j")
keymap.set("n", "sl", "<C-w>l")

-- Resize window
keymap.set("n", "<C-w><left>", "<C-w><")
keymap.set("n", "<C-w><right>", "<C-w>>")
keymap.set("n", "<C-w><up>", "<C-w>+")
keymap.set("n", "<C-w><down>", "<C-w>-")

-- vim.lsp.inlay_hint.enable(not vim.lsp.inlay_hint.is_enabled())
-- -- LSPsaga
-- keymap.set("n", "K", "<Cmd>Lspsaga hover_doc<CR>", opts)
-- keymap.set("n", "gp", "<Cmd>Lspsaga peek_definition<CR>", opts)
-- keymap.set("n", "ga", "<Cmd>Lspsaga code_action<CR>", opts)
--
-- new lspsag keymaps
keymap.set("n", "K", vim.lsp.buf.hover, opts)
keymap.set("n", "gp", vim.lsp.buf.definition, opts)
keymap.set("n", "gr", vim.lsp.buf.references, opts)
keymap.set("n", "ga", vim.lsp.buf.code_action, opts)

keymap.set("n", "<Leader>r", "<Cmd>Rest run<CR>", opts)

keymap.set("n", "<Leader>ls", function()
	return vim.fn.jobstart("live-server", opts)
end, opts)

keymap.set("c", "/", function()
	if vim.fn.getcmdtype() == ":" and is_substitute_command(vim.fn.getcmdline()) then
		return "/\\v"
	end

	return "/"
end, { expr = true, noremap = true, desc = "Always use very magic in :substitute patterns" })

--lspsaga diagnostic
keymap.set("n", "<C-j>", "<Cmd>Lspsaga diagnostic_jump_next<CR>", opts)
