local M = {}

local plugin_name = "pingu_ai_coding_pair_programming"
local plugin_dir = vim.fn.stdpath("data") .. "/lazy/" .. plugin_name
local lockfile = vim.fn.stdpath("config") .. "/lazy-lock.json"
local timeout_ms = 30000

local function path_exists(path)
	return vim.loop.fs_stat(path) ~= nil
end

local function read_lines(path)
	local ok, lines = pcall(vim.fn.readfile, path)
	if not ok then
		return nil
	end
	return lines
end

local function json_is_valid(lines)
	return pcall(vim.json.decode, table.concat(lines, "\n"))
end

local function remove_lock_entry()
	local lines = read_lines(lockfile)
	if not lines then
		return
	end

	local changed = false
	local next_lines = vim.tbl_filter(function(line)
		local remove = line:match('"%s*' .. plugin_name .. '%s*"%s*:')
		changed = changed or remove ~= nil
		return not remove
	end, lines)

	if changed and json_is_valid(next_lines) then
		vim.fn.writefile(next_lines, lockfile)
	end
end

local function close_timer(timer)
	if not timer or timer:is_closing() then
		return
	end
	timer:stop()
	timer:close()
end

local function run_git(args, on_exit)
	local command = vim.list_extend({ "git", "-C", plugin_dir }, args)
	local output = {}
	local job_id
	local timer = vim.loop.new_timer()

	job_id = vim.fn.jobstart(command, {
		stdout_buffered = true,
		stderr_buffered = true,
		on_stdout = function(_, data)
			vim.list_extend(output, data or {})
		end,
		on_exit = function(_, code)
			close_timer(timer)
			if on_exit then
				vim.schedule(function()
					on_exit(code, table.concat(output, "\n"))
				end)
			end
		end,
	})

	if job_id <= 0 then
		return
	end

	timer:start(timeout_ms, 0, function()
		vim.fn.jobstop(job_id)
		close_timer(timer)
	end)
end

local function unload_vimscript_runtime()
	vim.g.loaded_pingu_dev_agent = nil
	vim.g.loaded_pingu_dev_agent_internal = nil
end

local function install_action_menu_fallback_keymap()
	local key = vim.g.pingu_action_menu_key or "<leader>pia"
	pcall(vim.keymap.set, "n", key, "<cmd>PinguIssueActions<cr>", {
		desc = "Pingu: menu de acoes da issue atual",
		silent = true,
	})
end

local function reload_plugin()
	unload_vimscript_runtime()
	pcall(vim.cmd, "Lazy reload " .. plugin_name)
	vim.schedule(function()
		unload_vimscript_runtime()
		pcall(vim.cmd, "runtime plugin/pingu_dev_agent.vim")
		install_action_menu_fallback_keymap()
	end)
end

local function fast_forward_to_main()
	if not path_exists(plugin_dir .. "/.git") then
		return
	end

	run_git({ "status", "--porcelain" }, function(status_code, status_output)
		if status_code ~= 0 or status_output ~= "" then
			return
		end

		run_git({ "fetch", "origin", "main" }, function(fetch_code)
			if fetch_code ~= 0 then
				return
			end

			run_git({ "switch", "-C", "main", "origin/main" }, function(switch_code)
				if switch_code == 0 then
					remove_lock_entry()
					reload_plugin()
				end
			end)
		end)
	end)
end

function M.setup()
	remove_lock_entry()

	vim.api.nvim_create_autocmd("VimEnter", {
		group = vim.api.nvim_create_augroup("PinguAlwaysLatest", { clear = true }),
		once = true,
		callback = function()
			remove_lock_entry()
			fast_forward_to_main()
		end,
	})
end

return M
