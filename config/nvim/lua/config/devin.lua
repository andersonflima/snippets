-- Integração LazyVim ⇄ Devin CLI (máquina corporativa).
-- Quando o binário `devin` existe:
--   - o pingu passa a usar o provider devin por padrão (PINGU_AI_PROVIDER),
--     a menos que já esteja definido no ambiente;
--   - :Devin abre o CLI num terminal; :Devin <prompt> envia o prompt; em
--     modo visual, :'<,'>Devin manda a seleção como contexto do prompt.
local function devin_available()
	return vim.fn.executable("devin") == 1
end

if devin_available() and (vim.env.PINGU_AI_PROVIDER == nil or vim.env.PINGU_AI_PROVIDER == "") then
	vim.env.PINGU_AI_PROVIDER = "devin"
end

vim.api.nvim_create_user_command("Devin", function(opts)
	if not devin_available() then
		vim.notify("devin CLI não encontrado no PATH", vim.log.levels.ERROR)
		return
	end
	local parts = { "devin" }
	local prompt = opts.args or ""
	if opts.range and opts.range > 0 then
		local lines = vim.api.nvim_buf_get_lines(0, opts.line1 - 1, opts.line2, false)
		local selection = table.concat(lines, "\n")
		prompt = (prompt ~= "" and (prompt .. "\n\n") or "") .. selection
	end
	if prompt ~= "" then
		table.insert(parts, vim.fn.shellescape(prompt))
	end
	vim.cmd("botright split | terminal " .. table.concat(parts, " "))
	vim.cmd("startinsert")
end, {
	desc = "Devin CLI: sem args abre interativo; com args/seleção envia como prompt",
	nargs = "*",
	range = true,
})
