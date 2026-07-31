-- Detecção do toolchain docker (máquina corporativa sem git para github):
-- nesse ambiente nenhum git clone/fetch deve partir do nvim — plugins chegam
-- por ZIP via setup_lazyvim_docker_toolchain.sh. A detecção é por filesystem
-- (state dir do toolchain) para valer em qualquer shell, com o env
-- NVIM_DOCKER_STATE_ROOT (env.sh) como confirmação adicional.
local M = {}

function M.is_offline()
	if vim.env.NVIM_DOCKER_STATE_ROOT ~= nil then
		return true
	end
	local state_dir = (vim.env.HOME or "") .. "/.local/share/nvim-docker-toolchain"
	return vim.loop.fs_stat(state_dir) ~= nil
end

return M
