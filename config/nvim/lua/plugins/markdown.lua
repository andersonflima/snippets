local markdown_preview_options = {
	mkit = {},
	katex = {},
	uml = {},
	maid = {
		securityLevel = "loose",
		startOnLoad = false,
	},
	disable_sync_scroll = 0,
	sync_scroll_type = "middle",
	hide_yaml_meta = 1,
	sequence_diagrams = {},
	flowchart_diagrams = {},
	content_editable = false,
	disable_filename = 0,
	toc = {},
}

local open_mermaid_preview = function()
	local source_file = vim.api.nvim_buf_get_name(0)
	local output_file = vim.fn.fnamemodify(source_file, ":r") .. ".svg"
	local stderr = {}

	vim.fn.jobstart({
		"npm",
		"exec",
		"--yes",
		"--package",
		"@mermaid-js/mermaid-cli",
		"mmdc",
		"--",
		"-i",
		source_file,
		"-o",
		output_file,
	}, {
		stderr_buffered = true,
		on_stderr = function(_, data)
			stderr = vim
				.iter(data)
				:filter(function(line)
					return line ~= nil and line ~= ""
				end)
				:totable()
		end,
		on_exit = function(_, exit_code)
			vim.schedule(function()
				if exit_code ~= 0 then
					local error_message = table.concat(stderr, "\n")
					vim.notify(
						error_message ~= "" and error_message or "Failed to render Mermaid preview",
						vim.log.levels.ERROR
					)
					return
				end

				vim.notify("Mermaid preview rendered: " .. output_file, vim.log.levels.INFO)
				vim.fn.jobstart({ "open", output_file }, { detach = true })
			end)
		end,
	})
end

vim.api.nvim_create_user_command("MermaidPreview", open_mermaid_preview, {})

vim.api.nvim_create_autocmd("FileType", {
	pattern = "mermaid",
	callback = function(event)
		vim.keymap.set("n", "<leader>cp", open_mermaid_preview, {
			buffer = event.buf,
			desc = "Mermaid SVG Preview",
			silent = true,
		})
	end,
})

return {
	{
		"iamcco/markdown-preview.nvim",
		init = function()
			vim.g.mkdp_auto_start = 0
			vim.g.mkdp_auto_close = 0
			vim.g.mkdp_refresh_slow = 0
			vim.g.mkdp_command_for_global = 0
			vim.g.mkdp_echo_preview_url = 1
			vim.g.mkdp_theme = "dark"
			vim.g.mkdp_filetypes = { "markdown", "mdx" }
			vim.g.mkdp_preview_options = markdown_preview_options
		end,
		keys = {
			{
				"<leader>cp",
				"<cmd>MarkdownPreviewToggle<cr>",
				ft = { "markdown", "mdx" },
				desc = "Markdown Mermaid Preview",
			},
		},
	},
}
