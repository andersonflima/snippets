return {
	{ "nvim-treesitter/playground", cmd = "TSPlaygroundToggle" },

	{
		"nvim-treesitter/nvim-treesitter",
		opts = function(_, opts)
			local function concat_unique(left, right)
				local seen = {}
				local result = {}

				local function append(values)
					for _, value in ipairs(values or {}) do
						if not seen[value] then
							seen[value] = true
							result[#result + 1] = value
						end
					end
				end

				append(left)
				append(right)

				return result
			end

			opts.ensure_installed = concat_unique(opts.ensure_installed, {
				"astro",
				"css",
				"fish",
				"gitignore",
				"graphql",
				"http",
				"scss",
				"svelte",
				"markdown_inline",
				"markdown",
				"lua",
				"xml",
				"json",
				"vue",
				"elixir",
				"heex",
				"eex",
			})

			opts.matchup = vim.tbl_deep_extend("force", opts.matchup or {}, {
				enable = true,
			})

			opts.query_linter = vim.tbl_deep_extend("force", opts.query_linter or {}, {
				enable = true,
				use_virtual_text = true,
				lint_events = { "BufWrite", "CursorHold" },
			})

			opts.playground = vim.tbl_deep_extend("force", opts.playground or {}, {
				enable = true,
				disable = {},
				updatetime = 25,
				persist_queries = true,
				keybindings = {
					toggle_query_editor = "o",
					toggle_hl_groups = "i",
					toggle_injected_languages = "t",
					toggle_anonymous_nodes = "a",
					toggle_language_display = "I",
					focus_language = "f",
					unfocus_language = "F",
					update = "R",
					goto_node = "<cr>",
					show_help = "?",
				},
			})

			vim.filetype.add({
				extension = {
					mdx = "mdx",
				},
			})

			vim.treesitter.language.register("markdown", "mdx")
		end,
	},
	{
		"nvim-neo-tree/neo-tree.nvim",
		enabled = false,
		branch = "v3.x",
		dependencies = {
			"nvim-lua/plenary.nvim",
			"MunifTanjim/nui.nvim",
			"nvim-tree/nvim-web-devicons", -- optional, but recommended
		},
		lazy = false, -- neo-tree will lazily load itself
	},
}
