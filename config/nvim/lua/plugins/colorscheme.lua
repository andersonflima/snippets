return {
	{
		"andersonflima/crowquill-theme",
		lazy = false,
		priority = 1000,
		config = function()
			vim.o.background = "light"
			vim.cmd.colorscheme("crowquill-light")
		end,
	},
}
