-- Documenta lazypath para reduzir ambiguidade neste modulo Lua.
-- Calcula lazypath para suportar o restante do fluxo.
local lazypath = vim.fn.stdpath("data") .. "/lazy/lazy.nvim"

-- Toolchain docker presente = rede corporativa sem git para github: plugins
-- chegam por ZIP via setup; nenhum git clone/fetch deve acontecer pelo lazy
-- (clone de faltante e checker desligados). Detecção por filesystem — não
-- depende de env.sh sourceado no shell que abriu o nvim.
local offline = require("config.offline").is_offline()

if not vim.loop.fs_stat(lazypath) then
	if offline then
		vim.notify(
			"lazy.nvim ausente em " .. lazypath .. "; rode o setup do toolchain (instala por ZIP)",
			vim.log.levels.ERROR
		)
	else
		vim.fn.system({
			"git",
			"clone",
			"--filter=blob:none",
			"https://github.com/folke/lazy.nvim.git",
			"--branch=stable", -- latest stable release
			lazypath,
		})
	end
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
				colorscheme = "crowquill-light",
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
	-- Sem toolchain docker: comportamento normal (clona faltantes, checa
	-- updates). Com toolchain (rede corporativa): tudo por ZIP, sem git.
	install = { missing = not offline },
	checker = { enabled = not offline }, -- automatically check for plugin updates
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

require("config.plugins_update")
