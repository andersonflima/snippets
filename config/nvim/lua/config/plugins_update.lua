-- :PluginsUpdate — update dos plugins na máquina corporativa (toolchain
-- docker presente): roda o nvim-plugins-update (canal npm, sem github) num
-- terminal. Em máquina normal (sem toolchain), aponta para o Lazy update.
local offline = require("config.offline")

vim.api.nvim_create_user_command("PluginsUpdate", function()
	if not offline.is_offline() then
		vim.cmd("Lazy update")
		return
	end
	local script = (vim.env.NVIM_DOCKER_WRAPPER_BIN or (vim.env.HOME .. "/.local/share/nvim-docker-toolchain/bin"))
		.. "/nvim-plugins-update"
	if vim.fn.executable(script) ~= 1 then
		vim.notify("nvim-plugins-update não encontrado; rode o setup do toolchain", vim.log.levels.ERROR)
		return
	end
	vim.cmd("botright split | terminal " .. vim.fn.shellescape(script))
end, { desc = "Atualiza plugins (canal npm no toolchain corporativo; Lazy update fora dele)" })
