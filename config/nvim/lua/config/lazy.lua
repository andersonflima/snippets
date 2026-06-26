-- Documenta lazypath para reduzir ambiguidade neste modulo Lua.
-- Calcula lazypath para suportar o restante do fluxo.
local wrapper_bin = vim.fn.expand("~/.local/share/nvim/wrappers/bin")
if vim.fn.isdirectory(wrapper_bin) == 1 then
	local current_path = vim.env.PATH or ""
	if not string.find(current_path, wrapper_bin, 1, true) then
		vim.env.PATH = wrapper_bin .. ":" .. current_path
	end
end

local lazypath = vim.fn.stdpath("data") .. "/lazy/lazy.nvim"
if not vim.loop.fs_stat(lazypath) then
	vim.notify("lazy.nvim nao encontrado. Execute config/lazyvim/setup_lazyvim_mason_from_zip.sh.", vim.log.levels.ERROR)
	return
end
vim.opt.rtp:prepend(lazypath)

require("lazy").setup({
	spec = {
		------------------------------------------------------------------
		-- LazyVim core
		------------------------------------------------------------------
		{
			"LazyVim/LazyVim",
			import = "lazyvim.plugins",
			opts = {
				colorscheme = "neon",
				news = {
					lazyvim = true,
					neovim = true,
				},
			},
		},

		------------------------------------------------------------------
		-- LINTING
		------------------------------------------------------------------
		{ import = "lazyvim.plugins.extras.linting.eslint" },

		------------------------------------------------------------------
		-- FORMATTING
		------------------------------------------------------------------
		{ import = "lazyvim.plugins.extras.formatting.prettier" },
		{ import = "lazyvim.plugins.extras.formatting.black" },

		------------------------------------------------------------------
		-- LANGUAGES
		------------------------------------------------------------------
		{ import = "lazyvim.plugins.extras.lang.typescript" }, -- JS / TS / React / RN
		{ import = "lazyvim.plugins.extras.lang.python" },
		{ import = "lazyvim.plugins.extras.lang.go" },
		{ import = "lazyvim.plugins.extras.lang.rust" },
		{ import = "lazyvim.plugins.extras.lang.elixir" },
		{ import = "lazyvim.plugins.extras.lang.java" },
		{ import = "lazyvim.plugins.extras.lang.markdown" },
		{ import = "lazyvim.plugins.extras.lang.yaml" },

		------------------------------------------------------------------
		-- UTIL
		------------------------------------------------------------------
		{ import = "lazyvim.plugins.extras.util.mini-hipatterns" },

		------------------------------------------------------------------
		-- Seus plugins
		------------------------------------------------------------------
		{ import = "plugins" },
	},
	defaults = {
		-- By default, only LazyVim plugins will be lazy-loaded. Your custom plugins will load during startup.
		-- If you know what you're doing, you can set this to `true` to have all your custom plugins lazy-loaded by default.
		lazy = false,
		-- It's recommended to leave version=false for now, since a lot the plugin that support versioning,
		-- have outdated releases, which may break your Neovim install.
		version = false, -- always use the latest git commit
		-- version = "*", -- try installing the latest stable version for plugins that support semver
	},
	dev = {
		path = "~/.ghq/github.com",
	},
	install = { missing = false },
	checker = { enabled = false },
	change_detection = { enabled = false, notify = false },
	performance = {
		cache = {
			enabled = true,
			-- disable_events = {},
		},
		rtp = {
			-- disable some rtp plugins
			disabled_plugins = {
				"gzip",
				-- "matchit",
				-- "matchparen",
				"netrwPlugin",
				"rplugin",
				"tarPlugin",
				"tohtml",
				"tutor",
				"zipPlugin",
			},
		},
	},
	ui = {
		custom_keys = {
			["<localleader>d"] = function(plugin)
				dd(plugin)
			end,
		},
	},
	debug = false,
})
