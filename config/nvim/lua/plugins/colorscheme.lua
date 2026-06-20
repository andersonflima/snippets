return {
	{
		"andersonflima/neon-theme-neovim",
		lazy = false,
		priority = 1000,
		opts = {
			transparent = false,
			terminal_colors = true,
			style = "atom",
		},
		config = function(_, opts)
			require("neon").setup(opts)
			vim.cmd.colorscheme("neon")
		end,
	},
}

