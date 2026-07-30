local docker_wrapper_bin = os.getenv("NVIM_DOCKER_WRAPPER_BIN")
	or (vim.env.HOME and (vim.env.HOME .. "/.local/share/nvim-docker-toolchain/bin"))
	or ""
local using_docker_toolchain = docker_wrapper_bin ~= "" and vim.fn.isdirectory(docker_wrapper_bin) == 1

if using_docker_toolchain then
	vim.env.PATH = docker_wrapper_bin .. ":" .. vim.env.PATH
end

return {
	------------------------------------------------------------------
	-- MASON
	------------------------------------------------------------------
	{
		"mason-org/mason.nvim",
		opts = function(_, opts)
			if using_docker_toolchain then
				opts.ensure_installed = {}
				return
			end
			vim.list_extend(opts.ensure_installed, {
				"gopls",
				"pyright",
				"typescript-language-server",
				"eslint-lsp",
				"tailwindcss-language-server",
				"css-lsp",
				"html-lsp",
				"json-lsp",
				"yaml-language-server",
				"lua-language-server",
				"bash-language-server",
				"omnisharp",
				"elixir-ls",
				-- tools
				"stylua",
				"selene",
				"luacheck",
				"shellcheck",
				"shfmt",
				"eslint_d",
			})
		end,
	},

	------------------------------------------------------------------
	-- LSPCONFIG (LazyVim way)
	------------------------------------------------------------------
	{
		"neovim/nvim-lspconfig",
		event = { "BufReadPre", "BufNewFile" },
		opts = {
			inlay_hints = { enabled = false },

			servers = {
				----------------------------------------------------------------
				-- APENAS OVERRIDES
				----------------------------------------------------------------

				tsserver = {
					single_file_support = false,
					settings = {
						typescript = {
							inlayHints = {
								includeInlayParameterNameHints = "literal",
								includeInlayFunctionLikeReturnTypeHints = true,
								includeInlayEnumMemberValueHints = true,
							},
						},
						javascript = {
							inlayHints = {
								includeInlayParameterNameHints = "all",
								includeInlayVariableTypeHints = true,
							},
						},
					},
				},

				lua_ls = {
					single_file_support = true,
					settings = {
						Lua = {
							workspace = { checkThirdParty = false },
							completion = {
								workspaceWord = true,
								callSnippet = "Both",
							},
							diagnostics = {
								disable = { "incomplete-signature-doc", "trailing-space" },
								unusedLocalExclude = { "_*" },
							},
							hint = {
								enable = true,
							},
							format = {
								enable = false,
							},
						},
					},
				},

				yamlls = {
					settings = {
						yaml = {
							keyOrdering = false,
						},
					},
				},

				elixirls = using_docker_toolchain and {
					cmd = { "elixir-ls" },
					filetypes = { "elixir", "eelixir", "heex", "surface" },
				} or {
					cmd = { vim.fn.stdpath("data") .. "/mason/bin/elixir-ls" },
					filetypes = { "elixir", "eelixir", "heex", "surface" },
				},

				tailwindcss = {
					settings = {
						tailwindCSS = {
							validate = true,
							experimental = {
								classRegex = {
									"tw`([^`]*)",
									'tw="([^"]*)',
									"tw={`([^`}]+)`",
								},
							},
						},
					},
				},

				omnisharp = using_docker_toolchain and {
					cmd = {
						"omnisharp",
						"--languageserver",
						"--hostPID",
						tostring(vim.fn.getpid()),
					},
				} or {
					cmd = {
						vim.fn.stdpath("data") .. "/mason/packages/omnisharp/omnisharp",
						"--languageserver",
						"--hostPID",
						tostring(vim.fn.getpid()),
					},
				},
			},
		},
	},

	------------------------------------------------------------------
	-- UI
	------------------------------------------------------------------
	{
		"glepnir/lspsaga.nvim",
		config = function()
			require("lspsaga").setup()
		end,
	},
}
