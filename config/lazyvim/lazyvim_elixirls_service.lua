-- Copie este arquivo para: ~/.config/nvim/lua/plugins/lsp-elixir.lua
-- Requer que o bootstrap gere: ~/.config/elixir_ls/setup.sh

local function file_exists(path)
  local stat = vim.uv.fs_stat(path)
  return stat ~= nil and stat.type == "file"
end

local function resolve_elixirls_cmd()
  local home = vim.env.HOME or ""
  local setup_sh = home .. "/.config/elixir_ls/setup.sh"
  local mason_ls = home .. "/.local/share/nvim/mason/packages/elixir-ls/language_server.sh"

  if file_exists(setup_sh) and file_exists(mason_ls) then
    local shell_cmd = table.concat({
      "[ -f \"" .. setup_sh .. "\" ] && . \"" .. setup_sh .. "\"",
      "exec \"" .. mason_ls .. "\"",
    }, "; ")

    return { "/bin/sh", "-lc", shell_cmd }
  end

  if file_exists(mason_ls) then
    return { mason_ls }
  end

  return { "/usr/bin/env", "elixir-ls" }
end

return {
  {
    "neovim/nvim-lspconfig",
    opts = function(_, opts)
      local current_servers = opts.servers or {}
      local current_elixirls = current_servers.elixirls or {}

      local merged_elixirls = vim.tbl_deep_extend("force", current_elixirls, {
        cmd = resolve_elixirls_cmd(),
        settings = {
          elixirLS = {
            dialyzerEnabled = false,
            fetchDeps = false,
            enableTestLenses = true,
            suggestSpecs = true,
          },
        },
      })

      opts.servers = vim.tbl_deep_extend("force", current_servers, {
        elixirls = merged_elixirls,
      })
    end,
  },
}
